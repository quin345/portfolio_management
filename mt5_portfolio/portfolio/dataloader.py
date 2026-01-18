import MetaTrader5 as mt5
import pandas as pd
import numpy as np
import sqlite3
import os

DB_NAME = "returns.db"

# -----------------------------------------
# Load symbols from CSV
# -----------------------------------------
def load_symbols_from_csv(csv_path):
    df = pd.read_csv(csv_path, sep="\t")
    return df.iloc[:, 0].dropna().astype(str).tolist()


# -----------------------------------------
# Fetch MT5 price data
# -----------------------------------------
def fetch_mt5_data(symbol, start_date=None):
    utc_to = pd.Timestamp.now()

    if start_date is None:
        utc_from = utc_to - pd.Timedelta(days=2000)
    else:
        utc_from = start_date

    rates = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_D1, utc_from, utc_to)
    if rates is None:
        raise ValueError(f"Failed to fetch data for {symbol}")

    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s")
    df.set_index("time", inplace=True)   # <— time is index here
    return df


# -----------------------------------------
# Compute log returns
# -----------------------------------------
def calculate_log_returns(series):
    return np.log(series / series.shift(1)).dropna()


# -----------------------------------------
# Load existing returns from DB
# -----------------------------------------
def load_from_db(broker_name):
    table = f"{broker_name.lower()}_returns"

    if not os.path.exists(DB_NAME):
        return None

    conn = sqlite3.connect(DB_NAME)
    try:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        df["time"] = pd.to_datetime(df["time"])
        df.set_index("time", inplace=True)   # <— time becomes index
        return df
    except Exception:
        return None
    finally:
        conn.close()


# -----------------------------------------
# Save returns to DB
# -----------------------------------------
def save_to_db(df, broker_name):
    table = f"{broker_name.lower()}_returns"
    conn = sqlite3.connect(DB_NAME)

    df_to_save = df.copy()
    df_to_save.index.name = "time"     # <— enforce name
    df_to_save.reset_index(inplace=True)

    df_to_save.to_sql(table, conn, if_exists="replace", index=False)
    conn.close()


# -----------------------------------------
# Compute USD-adjusted returns
# -----------------------------------------
def compute_returns(symbols, fx_map, start_date=None):
    data = {s: fetch_mt5_data(s, start_date) for s in symbols}

    fx_symbols = set(fx_map[s] for s in symbols if s in fx_map)
    fx_data = {fx: fetch_mt5_data(fx, start_date) for fx in fx_symbols}

    fx_returns = {}
    for fx, df_fx in fx_data.items():
        lr = calculate_log_returns(df_fx["close"])
        if fx.startswith("USD"):
            lr = -lr
        fx_returns[fx] = lr

    asset_returns = {}
    for symbol in symbols:
        idx_lr = calculate_log_returns(data[symbol]["close"])

        if symbol in fx_map:
            fx_symbol = fx_map[symbol]
            fx_lr = fx_returns[fx_symbol]

            combined = pd.concat([idx_lr, fx_lr], axis=1, join="inner")
            combined.columns = ["idx", "fx"]
            asset_returns[symbol] = combined["idx"] + combined["fx"]
        else:
            asset_returns[symbol] = idx_lr

    df = pd.DataFrame(asset_returns).dropna(how="any")
    df.index.name = "time"   # <— enforce index name
    return df


# -----------------------------------------
# Main loader with lookback + incremental update
# -----------------------------------------
def load_log_returns(symbols, fx_map, broker_name, lookback_days=252):
    existing = load_from_db(broker_name)

    if existing is None:
        print("No DB found. Fetching full data from MT5...")
        df = compute_returns(symbols, fx_map)
        save_to_db(df, broker_name)
        return df.tail(lookback_days)

    last_date = existing.index.max()
    today = pd.Timestamp.now().normalize()

    if last_date >= today - pd.Timedelta(days=1):
        print("DB is up to date. Using cached data.")
        return existing.tail(lookback_days)

    print(f"Updating DB from {last_date.date()} to today...")
    start_date = last_date + pd.Timedelta(days=1)

    new_data = compute_returns(symbols, fx_map, start_date=start_date)

    if new_data.empty:
        print("No new MT5 data available.")
        return existing.tail(lookback_days)

    updated = pd.concat([existing, new_data])
    updated = updated[~updated.index.duplicated(keep="last")]

    save_to_db(updated, broker_name)
    return updated.tail(lookback_days)


# -----------------------------------------
# Save metadata
# -----------------------------------------
def save_metadata(symbols, broker_name):
    table = f"{broker_name.lower()}_metadata"

    rows = []
    for symbol in symbols:
        info = mt5.symbol_info(symbol)
        if info is None:
            continue

        rows.append({
            "symbol": symbol,
            "contract_size": info.trade_contract_size,
            "min_volume": info.volume_min,
            "digits": info.digits,
            "description": info.description,
            "updated_at": pd.Timestamp.now()
        })

    df = pd.DataFrame(rows)

    conn = sqlite3.connect(DB_NAME)
    df.to_sql(table, conn, if_exists="replace", index=False)
    conn.close()

    print(f"Saved metadata to table '{table}'")


#load metadata
def load_metadata(broker_name):
    table = f"{broker_name.lower()}_metadata"

    if not os.path.exists(DB_NAME):
        raise FileNotFoundError(f"{DB_NAME} not found")

    conn = sqlite3.connect(DB_NAME)
    try:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        df.columns = df.columns.str.strip().str.lower()
        return df
    finally:
        conn.close()

#load contract size info
def load_contract_dataframe(broker_name, scaled_df):
    """
    Returns a DataFrame containing:
    asset, contract_size, min_volume, digits, description, scaled_weight
    """
    table = f"{broker_name.lower()}_metadata"

    conn = sqlite3.connect(DB_NAME)
    try:
        meta = pd.read_sql(f"SELECT * FROM {table}", conn)
    finally:
        conn.close()

    # Normalize
    meta.columns = meta.columns.str.strip().str.lower()

    # Rename symbol → asset for consistency
    meta.rename(columns={"symbol": "asset"}, inplace=True)

    # Merge with scaled weights
    df = meta.merge(scaled_df, on="asset", how="left")

    return df

# export active symbols
from brokers import get_broker

def export_active_symbols(broker_name=None, csv_path="active_symbols.csv"):
    """Fetch active MT5 Market Watch symbols and export to CSV."""
    import MetaTrader5 as mt5
    import pandas as pd

    # Initialize broker if provided
    broker = None
    if broker_name:
        broker = get_broker(broker_name)
        print(f"Using broker: {broker_name}")

        # If your broker object has login() or initialize() methods, call them
        if hasattr(broker, "initialize"):
            broker.initialize()
        if hasattr(broker, "login"):
            broker.login()

    else:
        # Default MT5 init if no broker object is used
        if not mt5.initialize():
            raise RuntimeError("Failed to initialize MT5")

    # Only visible symbols (Market Watch)
    symbols = [s for s in mt5.symbols_get() if s.visible]

    df = pd.DataFrame([{
        "symbol": s.name
    } for s in symbols])

    df.to_csv(csv_path, index=False, header=False)
    print(f"Exported {len(df)} active symbols to {csv_path}")


# ---------------------------------------------------------
# Command-line entry point
# ---------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export active MT5 symbols to CSV")

    parser.add_argument("--broker", type=str, help="Broker name (e.g., icmarkets)")
    parser.add_argument("--out", type=str, help="Output CSV file")

    args = parser.parse_args()

    # Build output filename AFTER parsing
    if args.out:
        csv_path = args.out
    else:
        if args.broker:
            csv_path = f"{args.broker}_active_symbols.csv"
        else:
            csv_path = "active_symbols.csv"

    export_active_symbols(
        broker_name=args.broker,
        csv_path=csv_path   # <-- use the computed value
    )