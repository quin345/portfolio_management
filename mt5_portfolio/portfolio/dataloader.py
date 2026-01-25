import MetaTrader5 as mt5
import pandas as pd
import numpy as np
import sqlite3
import os

DB_NAME = "returns.db"

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
        df = pd.read_csv(csv_path, sep="\t")
        return df.iloc[:, 0].tolist()  # assumes first column contains symbols
        

    else:
        raise ValueError("source must be 'mt5' or 'csv'")



# -----------------------------------------
# Fetch MT5 price data
# -----------------------------------------
def fetch_mt5_data(symbol, start_date=None, timeframe=mt5.TIMEFRAME_D1):
    utc_to = pd.Timestamp.now()

    if start_date is None:
        utc_from = utc_to - pd.Timedelta(days=2000)
    else:
        utc_from = start_date
    rates = mt5.copy_rates_range(symbol, timeframe=timeframe, utc_from, utc_to)
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

#-----------------------------------
# check database columns
#----------------------------------

def ensure_columns_exist(table, df):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Read existing DB columns
    cursor.execute(f"PRAGMA table_info({table})")
    db_cols = {row[1] for row in cursor.fetchall()}

    # Columns in the DataFrame
    df_cols = set(df.columns)

    # Find columns missing in DB
    missing = df_cols - db_cols

    for col in missing:
        print(f"Adding missing column to DB: {col}")
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} REAL")

    conn.commit()
    conn.close()



# -----------------------------------------
# Main loader with lookback + incremental update
# -----------------------------------------
def load_log_returns(symbols, fx_map, broker_name, lookback_days=252):
    table = f"{broker_name.lower()}_returns"
    existing = load_from_db(broker_name)

    # ---------------------------------------------------------
    # CASE 1 — No DB exists yet → fetch everything
    # ---------------------------------------------------------
    if existing is None:
        print("No DB found. Fetching full data from MT5...")
        df = compute_returns(symbols, fx_map)

        # Save full table ONCE
        save_to_db(df, broker_name)
        return df.tail(lookback_days)

    # ---------------------------------------------------------
    # STEP 1 — Add missing symbols (add new columns)
    # ---------------------------------------------------------
    existing_symbols = set(existing.columns)
    requested_symbols = set(symbols)

    missing = requested_symbols - existing_symbols

    if missing:
        print(f"Missing symbols in DB: {missing}")

        # Fetch full history for missing symbols
        missing_data = compute_returns(list(missing), fx_map)

        # Join into DataFrame
        existing = existing.join(missing_data, how="outer")

        # Build DataFrame containing only new columns
        to_append = existing[list(missing)].reset_index()

        # Ensure DB schema has these columns
        ensure_columns_exist(table, to_append)

        # Append only the new columns
        to_append.to_sql(
            table,
            sqlite3.connect(DB_NAME),
            if_exists="append",
            index=False
        )

    # ---------------------------------------------------------
    # STEP 2 — Check if DB is up to date
    # ---------------------------------------------------------
    last_date = existing.index.max()
    today = pd.Timestamp.now().normalize()

    if last_date >= today - pd.Timedelta(days=1):
        print("DB is up to date. Using cached data.")
        return existing[symbols].tail(lookback_days)

    # ---------------------------------------------------------
    # STEP 3 — Fetch new rows from MT5 and append
    # ---------------------------------------------------------
    print(f"Updating DB from {last_date.date()} to today...")
    start_date = last_date + pd.Timedelta(days=1)

    print(start_date)

    new_data = compute_returns(symbols, fx_map, start_date=start_date)

    if new_data.empty:
        print("No new MT5 data available.")
        return existing[symbols].tail(lookback_days)

    # Prepare new rows for DB
    to_append = new_data.reset_index()

    # Ensure DB has all required columns
    ensure_columns_exist(table, to_append)

    # Append new rows
    to_append.to_sql(
        table,
        sqlite3.connect(DB_NAME),
        if_exists="append",
        index=False
    )

    # Return updated view (without rewriting DB)
    updated = pd.concat([existing, new_data])
    updated = updated[~updated.index.duplicated(keep="last")]

    return updated[symbols].tail(lookback_days)

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
            "max_volume": info.volume_max,
            "volume_step": info.volume_step,
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


