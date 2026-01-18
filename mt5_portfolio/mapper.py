import MetaTrader5 as mt5
import pandas as pd

# -----------------------------
# FX mapping logic
# -----------------------------
def infer_fx_pair(symbol):
    info = mt5.symbol_info(symbol)
    if not info:
        return None

    margin = info.currency_margin
    profit = info.currency_profit

    # Skip if USD is involved
    if margin == "USD" or profit == "USD":
        return None

    # Prefer profit currency
    ccy = profit or margin

    # Check if USDXXX exists (USD as base)
    usd_base = f"USD{ccy}"
    if mt5.symbol_info(usd_base) is not None:
        return usd_base

    # Otherwise default to XXXUSD
    return f"{ccy}USD"


# -----------------------------
# Symbol source selection
# -----------------------------
def load_symbols(source="mt5", csv_path=None):
    """
    source: "mt5" or "csv"
    csv_path: path to CSV file if source="csv"
    """
    if source == "mt5":
        symbols = mt5.symbols_get()
        return [
            sym.name
            for sym in symbols
            if sym.visible and sym.trade_mode == mt5.SYMBOL_TRADE_MODE_FULL
        ]

    elif source == "csv":
        if csv_path is None:
            raise ValueError("csv_path must be provided when source='csv'")
        df = pd.read_csv(csv_path)
        return df.iloc[:, 0].tolist()  # assumes first column contains symbols

    else:
        raise ValueError("source must be 'mt5' or 'csv'")


# -----------------------------
# Build FX map from chosen source
# -----------------------------
def build_fx_map(source="mt5", csv_path=None):
    symbols = load_symbols(source=source, csv_path=csv_path)

    fx_map = {}
    fx_exempt = set()

    for symbol in symbols:
        # Collect USDXXX exemptions
        if symbol.startswith("USD"):
            fx_exempt.add(symbol)
            continue

        fx = infer_fx_pair(symbol)
        if fx is None:
            continue

        fx_map[symbol] = fx

    return fx_map, fx_exempt