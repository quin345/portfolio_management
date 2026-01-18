import MetaTrader5 as mt5
import pandas as pd
import numpy as np
import sqlite3
import os

# -----------------------------------------
# Load symbols from CSV
def load_symbols_from_csv(csv_path):
    df = pd.read_csv(csv_path)
    # Use the first column as symbols
    symbols = df.iloc[:, 0].dropna().astype(str).tolist()
    return symbols

# -----------------------------------------
# Fetch MT5 price data
# -----------------------------------------
def fetch_mt5_data(symbol, start_date=None):
    if start_date is None:
        utc_to = pd.Timestamp.now()
        utc_from = utc_to - pd.Timedelta(days=2000)
    else:
        utc_from = start_date
        utc_to = pd.Timestamp.now()

    rates = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_D1, utc_from, utc_to)
    if rates is None:
        raise ValueError(f"Failed to fetch data for {symbol}")

    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s")
    df.set_index("time", inplace=True)
    return df


# -----------------------------------------
# Compute log returns
# -----------------------------------------
def calculate_log_returns(series):
    return np.log(series / series.shift(1)).dropna()


# -----------------------------------------
# Load existing returns for a broker
# -----------------------------------------
def load_existing_returns(broker_name):
    db_name = "returns.db"
    table = f"{broker_name.lower()}_returns"

    if not os.path.exists(db_name):
        return None

    conn = sqlite3.connect(db_name)
    try:
        df = pd.read_sql(f"SELECT * FROM {table}", conn, parse_dates=["index"])
        df.set_index("index", inplace=True)
        return df
    except Exception:
        return None
    finally:
        conn.close()


# -----------------------------------------
# Save returns to broker-specific table
# -----------------------------------------
def save_returns_to_db(returns_df, broker_name):
    db_name = "returns.db"
    table = f"{broker_name.lower()}_returns"

    conn = sqlite3.connect(db_name)
    returns_df.to_sql(table, conn, if_exists="replace")
    conn.close()

    print(f"Saved returns to table '{table}' in returns.db")


# -----------------------------------------
# Compute USD-adjusted returns
# -----------------------------------------
def compute_usd_adjusted_returns(symbols, fx_map, start_date=None):
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

    returns = pd.DataFrame(asset_returns)[symbols].dropna(how="any")
    return returns


# -----------------------------------------
# Main loader with incremental update
# -----------------------------------------
def load_returns(symbols, fx_map, broker_name):
    existing = load_existing_returns(broker_name)

    if existing is None:
        print("No existing data. Building full dataset...")
        returns = compute_usd_adjusted_returns(symbols, fx_map)
        save_returns_to_db(returns, broker_name)
        return returns

    last_date = existing.index.max()
    print(f"Existing data found. Last date: {last_date.date()}")

    start_date = last_date + pd.Timedelta(days=1)
    print(f"Fetching new data from {start_date.date()} onward...")

    new_returns = compute_usd_adjusted_returns(symbols, fx_map, start_date=start_date)

    if new_returns.empty:
        print("No new data. Already up to date.")
        return existing

    updated = pd.concat([existing, new_returns])
    updated = updated[~updated.index.duplicated(keep="last")]

    save_returns_to_db(updated, broker_name)
    return updated