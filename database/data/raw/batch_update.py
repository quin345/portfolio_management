import csv
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import pandas as pd
import time
import random
import logging
import os

from fetch_tick_data import fetch_tick_data_for_day
from store_tick_data import store_tick_data


LOG_PATH = os.path.join(os.path.dirname(__file__), 'batch_update.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[logging.FileHandler(LOG_PATH, encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def run_fetch(symbol: str, last_date_str: str, end_date: datetime, save_dir: str, delay_between_days: float = 0.05):
    try:
        start_date = datetime.strptime(last_date_str, "%Y-%m-%d") + timedelta(days=1)
        if start_date >= end_date:
            logger.info("Skipping %s: start date %s is beyond end date.", symbol, start_date.date())
            return

        logger.info("Fetching %s from %s to %s", symbol, start_date.date(), end_date.date())

        while start_date < end_date:
            logger.info("Fetching data for %s %s", symbol, start_date.strftime('%Y-%m-%d'))
            tick_data = fetch_tick_data_for_day(symbol, start_date)

            if tick_data:
                df = pd.DataFrame(tick_data)
                try:
                    store_tick_data(df, symbol, save_dir)
                    logger.info("Saved data for %s %s", symbol, start_date.strftime('%Y-%m-%d'))
                except Exception as e:
                    logger.exception("Error saving data for %s %s: %s", symbol, start_date.strftime('%Y-%m-%d'), e)
            else:
                logger.warning("No valid data for %s %s", symbol, start_date.strftime('%Y-%m-%d'))

            # small randomized pause to avoid hitting server in bursts
            time.sleep(delay_between_days + random.uniform(0, 0.05))

            start_date += timedelta(days=1)

        logger.info("Finished fetching %s tick data.", symbol)

    except Exception as e:
        logger.exception("Error fetching %s from %s: %s", symbol, last_date_str, e)


def main():
    global logger
    parser = argparse.ArgumentParser(description="Fetch tick data for symbols.")
    parser.add_argument("--end-date", required=True, help="Target end date in YYYY-MM-DD format")
    parser.add_argument("--csv", default="last_tick_update.csv", help="CSV file with last update dates")
    parser.add_argument("--save-dir", default="2015_tick_data", help="Directory to save HDF5 files")
    parser.add_argument("--workers", type=int, help="Override BATCH_MAX_WORKERS env var")
    parser.add_argument("--log-file", required=False, help="Optional log file path (overrides default)")
    args = parser.parse_args()

    # allow overriding log file path
    if args.log_file:
        for h in list(logger.handlers):
            logger.removeHandler(h)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s: %(message)s',
            handlers=[logging.FileHandler(args.log_file, encoding='utf-8'), logging.StreamHandler()]
        )
        # re-fetch module logger so it uses the new handlers/root configuration
        logger = logging.getLogger(__name__)

    try:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError:
        logger.error("Invalid date format. Please use YYYY-MM-DD.")
        return

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

    env_workers = int(os.environ.get('BATCH_MAX_WORKERS', '8'))
    if args.workers:
        max_workers = max(1, args.workers)
    else:
        max_workers = min(env_workers, max(1, len(symbols_dates)))

    logger.info("Starting batch update with workers=%d", max_workers)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for symbol, last_date in symbols_dates:
            futures.append(executor.submit(run_fetch, symbol, last_date, end_date, save_dir))
        for fut in futures:
            try:
                fut.result()
            except Exception as e:
                logger.exception("Batch task failed: %s", e)


if __name__ == '__main__':
    main()