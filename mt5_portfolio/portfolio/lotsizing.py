import math
import pandas as pd
import MetaTrader5 as mt5
import sqlite3
from decimal import Decimal, ROUND_HALF_UP

# =========================================================
# MT5 INITIALIZATION
# =========================================================
mt5.initialize()


# =========================================================
# ACCOUNT EQUITY
# =========================================================
def get_equity():
    info = mt5.account_info()
    if info is None:
        raise RuntimeError("Unable to fetch MT5 account info.")
    return info.equity


# =========================================================
# PRICE FETCHING
# =========================================================
def get_latest_price(symbol):
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return None
    return tick.bid


def fetch_prices(assets):
    return {a: get_latest_price(a) for a in assets}


def fetch_index_fx_rates(index_fx_map):
    return {idx: get_latest_price(fx) for idx, fx in index_fx_map.items()}


# =========================================================
# NET POSITIONS
# =========================================================
def fetch_net_positions():
    positions = mt5.positions_get()
    if positions is None:
        return {}

    net = {}
    for p in positions:
        symbol = p.symbol
        volume = p.volume if p.type == 0 else -p.volume  # BUY = +, SELL = -
        net[symbol] = net.get(symbol, 0) + volume

    return net


# =========================================================
# SAFE DECIMAL QUANTIZER
# =========================================================
def safe_quantize(x, places="0.0001"):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    return Decimal(str(x)).quantize(Decimal(places), rounding=ROUND_HALF_UP)


# =========================================================
# LOAD BROKER-SPECIFIC METADATA FROM returns.db
# =========================================================
def load_metadata(broker_name):
    table = f"{broker_name.lower()}_metadata"
    conn = sqlite3.connect("returns.db")
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    conn.close()
    return df.set_index("symbol")


# =========================================================
# LOT SIZE CALCULATION (THEORETICAL)
# =========================================================
def compute_lot(row, equity, fx_exempt, index_fx_map, index_fx_rates):
    asset = row["asset"]
    weight = row["scaled_weight"]
    contract_size = row["contract_size"]
    price = row["latest_price"]

    if pd.isna(weight) or pd.isna(contract_size):
        return None
    if contract_size == 0:
        return None

    # FX-exempt assets
    if asset in fx_exempt:
        return (weight * equity) / contract_size

    if price is None or price == 0:
        return None

    # Global index → convert to USD
    if asset in index_fx_map:
        fx_rate = index_fx_rates.get(asset)
        if fx_rate is None or fx_rate == 0:
            return None

        if asset in ("JP225", "JPN225"):
            fx_rate = 1 / fx_rate

        price = price * fx_rate
        if price == 0:
            return None

    denominator = price * contract_size
    if denominator == 0:
        return None

    return (weight * equity) / denominator


# =========================================================
# CURRENT WEIGHT CALCULATION
# =========================================================
def compute_current_weight(row, equity, fx_exempt, index_fx_map, index_fx_rates):
    asset = row["asset"]
    lot = row.get("current_holdings", 0)
    contract_size = row["contract_size"]
    price = row["latest_price"]

    if pd.isna(lot) or pd.isna(contract_size):
        return None
    if contract_size == 0:
        return None

    # FX-exempt assets
    if asset in fx_exempt:
        position_value = lot * contract_size
        return position_value / equity

    if price is None or price == 0:
        return None

    # Global index → convert to USD
    if asset in index_fx_map:
        fx_rate = index_fx_rates.get(asset)
        if fx_rate is None or fx_rate == 0:
            return None

        if asset in ("JP225", "JPN225"):
            fx_rate = 1 / fx_rate

        price = price * fx_rate
        if price == 0:
            return None

    position_value = lot * price * contract_size
    return position_value / equity


# =========================================================
# ADJUST LOT SIZE BASED ON METADATA RULES
# =========================================================
def adjust_to_min_volume(symbol, lot, metadata):
    if lot is None or pd.isna(lot):
        return None

    if symbol not in metadata.index:
        return lot  # fallback if no metadata

    row = metadata.loc[symbol]

    min_vol = row.get("min_volume", 0.01)
    step = row.get("volume_step", min_vol)
    max_vol = row.get("max_volume", 1000)

    # Clamp to allowed range
    lot = max(min(lot, max_vol), -max_vol)

    # Round to nearest step
    steps = round(lot / step)
    adjusted = steps * step

    # Enforce minimum volume (except zero)
    if adjusted != 0 and abs(adjusted) < min_vol:
        adjusted = min_vol if adjusted > 0 else -min_vol

    return float(adjusted)


# =========================================================
# MAIN LOT SIZING PIPELINE
# =========================================================
def run_lot_sizing(df, fx_exempt, index_fx_map, broker_name):
    equity = get_equity()

    # Load broker-specific metadata
    metadata = load_metadata(broker_name)

    # Fetch prices
    all_assets = df["asset"].tolist()
    latest_prices = fetch_prices(all_assets)
    index_fx_rates = fetch_index_fx_rates(index_fx_map)

    df["latest_price"] = df["asset"].map(latest_prices)

    # Fetch net positions
    net_positions = fetch_net_positions()
    df["current_holdings"] = pd.to_numeric(
        df["asset"].map(net_positions).fillna(0),
        errors="coerce"
    )

    # Current weights
    df["current_weight"] = df.apply(
        lambda row: compute_current_weight(row, equity, fx_exempt, index_fx_map, index_fx_rates),
        axis=1
    )
    df["current_weight"] = df["current_weight"].apply(lambda x: safe_quantize(x, "0.0001"))

    # Target lot sizes (theoretical)
    df["target_lot_size"] = df.apply(
        lambda row: compute_lot(row, equity, fx_exempt, index_fx_map, index_fx_rates),
        axis=1
    )
    df["target_lot_size"] = df["target_lot_size"].apply(lambda x: safe_quantize(x, "0.01"))
    df["target_lot_size"] = pd.to_numeric(df["target_lot_size"], errors="coerce")

    # Raw difference
    df["difference"] = (df["target_lot_size"] - df["current_holdings"]).round(2)

    # Adjusted lot sizes (metadata + MT5-valid)
    df["adjusted_lot_size"] = df.apply(
        lambda row: adjust_to_min_volume(row["asset"], row["target_lot_size"], metadata),
        axis=1
    )

    # Final trade instruction
    df["adjusted_difference"] = (df["adjusted_lot_size"] - df["current_holdings"]).round(2)

    # Gross totals
    df["abs_target_lot_size"] = df["target_lot_size"].abs()
    df["abs_current_holdings"] = df["current_holdings"].abs()

    gross_target = df["abs_target_lot_size"].sum()
    gross_current = df["abs_current_holdings"].sum()

    return df, gross_target, gross_current