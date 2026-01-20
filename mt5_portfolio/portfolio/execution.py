import MetaTrader5 as mt5
import time
import sqlite3
import pandas as pd


# =========================================================
# Load broker-specific metadata from returns.db
# =========================================================
def load_metadata(broker_name: str) -> pd.DataFrame:
    table = f"{broker_name.lower()}_metadata"
    conn = sqlite3.connect("returns.db")
    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    conn.close()
    return df.set_index("symbol")


# =========================================================
# Ensure MT5 is initialized
# =========================================================
def ensure_initialized():
    if not mt5.initialize():
        raise RuntimeError("Failed to initialize MT5 terminal")


# =========================================================
# Ensure symbol is tradable
# =========================================================
def ensure_symbol(symbol: str):
    info = mt5.symbol_info(symbol)
    if info is None:
        raise RuntimeError(f"Symbol not found: {symbol}")

    if not info.visible:
        mt5.symbol_select(symbol, True)

    return info


# =========================================================
# Build MT5 order request
# =========================================================
def build_order(symbol: str, volume: float, order_type: int):
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        raise RuntimeError(f"No tick data for symbol: {symbol}")

    price = tick.ask if order_type == mt5.ORDER_TYPE_BUY else tick.bid

    return {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": volume,
        "type": order_type,
        "price": price,
        "deviation": 20,
        "magic": 123456,
        "comment": "auto_rebalance",
        "type_filling": mt5.ORDER_FILLING_IOC,
        "type_time": mt5.ORDER_TIME_GTC,
    }


# =========================================================
# Send a single MT5 order
# =========================================================
def send_order(symbol: str, volume: float, order_type: int) -> dict:
    request = build_order(symbol, volume, order_type)
    result = mt5.order_send(request)

    if result is None:
        return {"symbol": symbol, "status": "FAILED", "reason": "order_send returned None"}

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        return {
            "symbol": symbol,
            "status": "FAILED",
            "retcode": result.retcode,
            "comment": result.comment,
        }

    return {
        "symbol": symbol,
        "status": "SUCCESS",
        "volume": volume,
        "order_type": "BUY" if order_type == mt5.ORDER_TYPE_BUY else "SELL",
        "price": result.price,
    }


# =========================================================
# Execute adjusted_difference with metadata min/step/max
# =========================================================
def execute_trade(symbol: str, diff: float, metadata: pd.DataFrame) -> list[dict]:
    if diff is None or diff == 0:
        return [{"symbol": symbol, "status": "SKIPPED", "reason": "No trade needed"}]

    ensure_symbol(symbol)

    if symbol not in metadata.index:
        return [{"symbol": symbol, "status": "FAILED", "reason": "No metadata for symbol"}]

    row = metadata.loc[symbol]

    min_vol = row.get("min_volume", 0.01)
    step = row.get("volume_step", min_vol)
    max_vol = row.get("max_volume", 1000.0)

    direction = mt5.ORDER_TYPE_BUY if diff > 0 else mt5.ORDER_TYPE_SELL
    remaining = abs(diff)

    results: list[dict] = []

    while remaining > 0:
        # Respect max_volume per trade
        volume = min(remaining, max_vol)

        # If below minimum volume, stop
        if volume < min_vol:
            results.append({
                "symbol": symbol,
                "status": "SKIPPED",
                "reason": f"Remaining volume {volume} < min_volume {min_vol}",
            })
            break

        # Snap to volume_step
        steps = round(volume / step)
        volume = steps * step

        # Safety: if rounding kills the volume, stop
        if volume <= 0:
            results.append({
                "symbol": symbol,
                "status": "SKIPPED",
                "reason": f"Rounded volume <= 0 after step adjustment",
            })
            break

        result = send_order(symbol, volume, direction)
        results.append(result)

        remaining -= volume
        time.sleep(0.2)

    return results


# =========================================================
# Execute all trades from DataFrame
# =========================================================
def execute_rebalance(df: pd.DataFrame, broker_name: str) -> list[dict]:
    """
    Expects df to contain:
      - 'asset'               : symbol name
      - 'adjusted_difference' : final lot delta to trade (can be +/-)
    """
    ensure_initialized()
    metadata = load_metadata(broker_name)

    all_results: list[dict] = []

    for _, row in df.iterrows():
        symbol = row["asset"]
        diff = row["adjusted_difference"]

        trade_results = execute_trade(symbol, diff, metadata)
        all_results.extend(trade_results)

    return all_results