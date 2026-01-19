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

    # Get all broker symbols once
    all_symbols = mt5.symbols_get()

    # Try to find USD as base: USDXXX + any suffix
    for s in all_symbols:
        if s.name.startswith(f"USD{ccy}"):
            return s.name

    # Try to find XXX as base: XXXUSD + any suffix
    for s in all_symbols:
        if s.name.startswith(f"{ccy}USD"):
            return s.name

    return None




# -----------------------------
# Build FX map from chosen source
# -----------------------------
def build_fx_map(symbols):

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