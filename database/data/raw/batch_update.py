import csv
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import pandas as pd
import time
import random

from fetch_tick_data import fetch_tick_data_for_day
from store_tick_data import store_tick_data


def run_fetch(symbol: str, last_date_str: str, end_date: datetime, save_dir: str):
    try:
        start_date = datetime.strptime(last_date_str, "%Y-%m-%d") + timedelta(days=1)
        if start_date >= end_date:
            print(f"⏩ Skipping {symbol}: start date {start_date.date()} is beyond end date.")
            return

        print(f"🚀 Fetching {symbol} from {start_date.date()} to {end_date.date()}")

        while start_date < end_date:
            print(f"📅 Fetching data for {symbol} {start_date.strftime('%Y-%m-%d')}...")
            tick_data = fetch_tick_data_for_day(symbol, start_date)

            if tick_data:
                df = pd.DataFrame(tick_data)
                store_tick_data(df, symbol, save_dir)
                print(f"✅ Saved data for {symbol} {start_date.strftime('%Y-%m-%d')}.")
            else:
                print(f"⚠️ No valid data for {symbol} {start_date.strftime('%Y-%m-%d')}.")

            # small randomized pause to avoid hitting server in bursts
            time.sleep(random.uniform(0.05, 0.25))

            start_date += timedelta(days=1)

        print(f"🏁 Finished fetching {symbol} tick data.")

    except Exception as e:
        print(f"❌ Error fetching {symbol} from {last_date_str}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Fetch tick data for symbols.")
    parser.add_argument("--end-date", required=True, help="Target end date in YYYY-MM-DD format")
    parser.add_argument("--csv", default="last_tick_update.csv", help="CSV file with last update dates")
    parser.add_argument("--save-dir", default="2015_tick_data", help="Directory to save HDF5 files")
    args = parser.parse_args()

    try:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Invalid date format. Please use YYYY-MM-DD.")

    csv_file = args.csv
    save_dir = args.save_dir

    symbols_dates = []
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)  # Skip header
        for row in reader:
            if len(row) < 2:
                continue
            symbol = row[0].strip()
            last_date = row[1].strip()
            if symbol and last_date:
                symbols_dates.append((symbol, last_date))

    max_workers = int(os.environ.get('BATCH_MAX_WORKERS', '8'))
    max_workers = min(max_workers, max(1, len(symbols_dates)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for symbol, last_date in symbols_dates:
            executor.submit(run_fetch, symbol, last_date, end_date, save_dir)


if __name__ == '__main__':
    main()