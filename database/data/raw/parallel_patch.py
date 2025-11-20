import csv
import ast
import os
import shutil
from collections import defaultdict
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import List, Sequence
import time
import random
import argparse
import logging

import pandas as pd

from fetch_tick_data import fetch_tick_data_for_day


# === Step 1: Parse CSV ===
CSV_FILE = "missing_day_group.csv"

# logging
LOG_PATH = os.path.join(os.path.dirname(__file__), 'parallel_patch.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[logging.FileHandler(LOG_PATH, encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def parse_missing_groups(csv_file: str = CSV_FILE) -> defaultdict:
    instrument_dates = defaultdict(list)
    if not os.path.exists(csv_file):
        logger.warning("CSV not found: %s", csv_file)
        return instrument_dates

    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)  # Skip header if present
        for row in reader:
            if len(row) < 2:
                continue
            instrument = row[0].strip()
            try:
                date_list = ast.literal_eval(row[1])
                for date_str in date_list:
                    try:
                        date = datetime.strptime(date_str, "%Y-%m-%d")
                        instrument_dates[instrument].append(date)
                    except ValueError:
                        logger.warning("Invalid date format: %s", date_str)
            except Exception as e:
                logger.warning("Failed to parse date list for %s: %s", instrument, e)

    return instrument_dates


def chunkify(lst: Sequence, n: int) -> List[List]:
    """Split `lst` into `n` chunks as evenly as possible (some chunks may be empty)."""
    if n <= 0:
        return [list(lst)]
    length = len(lst)
    q, r = divmod(length, n)
    chunks = []
    idx = 0
    for i in range(n):
        size = q + (1 if i < r else 0)
        chunks.append(list(lst[idx:idx + size]))
        idx += size
    return chunks


def worker(worker_id: int, tasks: List[tuple]):
    temp_file = f"temp_worker_{worker_id}.h5"
    for instrument, date in tasks:
        try:
            logger.info("Worker %d: %s %s", worker_id, instrument, date.date())
            data = fetch_tick_data_for_day(instrument, date)
            # small randomized delay between requests to avoid bursts
            time.sleep(random.uniform(0.05, 0.2))

            if not data:
                logger.warning("Worker %d no data for %s %s", worker_id, instrument, date.date())
                continue

            df = pd.DataFrame(data)
            if df.empty or 'timestamp' not in df.columns:
                logger.warning("Worker %d fetched invalid data for %s %s", worker_id, instrument, date.date())
                continue

            ts = pd.to_datetime(df['timestamp'], unit='ms')
            df['year'] = ts.dt.year
            df['month'] = ts.dt.month
            df['day'] = ts.dt.day

            os.makedirs('.', exist_ok=True)

            with pd.HDFStore(temp_file, mode='a') as store:
                for (y, m, d), group in df.groupby(['year', 'month', 'day']):
                    key = f"/{instrument}/y{y}/m{m:02}/d{d:02}"
                    store.put(key, group.drop(columns=['year', 'month', 'day']), format='table', data_columns=True)

            logger.info("Worker %d saved %s %s", worker_id, instrument, date.date())

        except Exception as e:
            logger.exception("Worker %d error on %s %s: %s", worker_id, instrument, date.date(), e)


def merge_hdf5_files(temp_files: List[str], final_file: str):
    with pd.HDFStore(final_file, mode='a') as final_store:
        for temp in temp_files:
            if not os.path.exists(temp):
                logger.warning("Skipping missing file: %s", temp)
                continue
            try:
                with pd.HDFStore(temp, mode='r') as temp_store:
                    keys = temp_store.keys()
                    if not keys:
                        logger.warning("Skipping empty file: %s", temp)
                        continue
                    for key in keys:
                        df = temp_store[key]
                        final_store.put(key, df, format='table', data_columns=True)
            except Exception as e:
                logger.exception("Error reading %s: %s", temp, e)
            finally:
                try:
                    os.remove(temp)
                    logger.info("Deleted temp file: %s", temp)
                except Exception as e:
                    logger.warning("Could not delete %s: %s", temp, e)


def decompose_by_instrument(final_file: str, output_dir: str = "split_by_instrument"):
    os.makedirs(output_dir, exist_ok=True)
    with pd.HDFStore(final_file, mode='r') as store:
        keys = store.keys()
        instrument_groups = defaultdict(list)
        for key in keys:
            instrument = key.strip("/").split("/")[0]
            instrument_groups[instrument].append(key)

        for instrument, group_keys in instrument_groups.items():
            out_path = os.path.join(output_dir, f"{instrument}_tick_data.h5")
            with pd.HDFStore(out_path, mode='w') as out_store:
                for key in group_keys:
                    df = store[key]
                    out_store.put(key, df, format='table', data_columns=True)


def merge_instrument_file(instrument: str, fetched_dir: str = "split_by_instrument", raw_dir: str = "./2015_tick_data"):
    fetched_path = os.path.join(fetched_dir, f"{instrument}_tick_data.h5")
    raw_path = os.path.join(raw_dir, f"{instrument}_tick_data.h5")

    if not os.path.exists(fetched_path):
        logger.warning("Fetched file missing: %s", instrument)
        return
    if not os.path.exists(raw_path):
        logger.warning("Raw file missing: %s", instrument)
        return

    with pd.HDFStore(raw_path, mode='a') as raw_store, pd.HDFStore(fetched_path, mode='r') as fetched_store:
        for key in fetched_store.keys():
            if key in raw_store:
                logger.info("Skipping duplicate key: %s in %s", key, instrument)
                continue
            df = fetched_store[key]
            raw_store.put(key, df, format='table', data_columns=True)

    logger.info("Merged fetched → raw: %s", instrument)


def main():
    parser = argparse.ArgumentParser(description="Parallel fetch missing tick-day groups.")
    parser.add_argument("--workers", type=int, help="Override PARALLEL_MAX_WORKERS env var")
    parser.add_argument("--no-cleanup", action='store_true', help="Do not delete final_tick_data.h5 and split_by_instrument folder")
    parser.add_argument("--csv", default=CSV_FILE, help="CSV file with missing day groups")
    args = parser.parse_args()

    instrument_dates = parse_missing_groups(args.csv)
    all_tasks = [(instr, date) for instr, dates in instrument_dates.items() for date in dates]

    env_workers = int(os.environ.get('PARALLEL_MAX_WORKERS', '6'))
    if args.workers:
        desired_workers = max(1, args.workers)
    else:
        desired_workers = min(env_workers, max(1, len(all_tasks)))

    task_chunks = chunkify(all_tasks, desired_workers)

    logger.info("Starting workers=%d tasks=%d", desired_workers, len(all_tasks))
    futures = []
    with ThreadPoolExecutor(max_workers=desired_workers) as executor:
        for i, chunk in enumerate(task_chunks):
            if chunk:
                futures.append(executor.submit(worker, i, chunk))
        for fut in futures:
            try:
                fut.result()
            except Exception as e:
                logger.exception("Worker raised: %s", e)

    temp_files = [f"temp_worker_{i}.h5" for i in range(len(task_chunks))]
    final_file = "final_tick_data.h5"
    merge_hdf5_files(temp_files, final_file)
    decompose_by_instrument(final_file)
    logger.info("All tasks complete. Data split by instrument.")

    # MERGE TO RAW FILES
    fetched_dir = "split_by_instrument"
    raw_dir = "./2015_tick_data"

    if not os.path.isdir(fetched_dir):
        logger.warning("Fetched directory not found: %s. Skipping merge to raw files.", fetched_dir)
    else:
        instruments = [
            filename.replace("_tick_data.h5", "")
            for filename in os.listdir(fetched_dir)
            if filename.endswith("_tick_data.h5")
        ]

        max_merge_workers = min(28, max(1, len(instruments)))
        logger.info("Merging %d instruments into raw storage (workers=%d)", len(instruments), max_merge_workers)
        merge_futures = []
        with ThreadPoolExecutor(max_workers=max_merge_workers) as executor:
            for instrument in instruments:
                merge_futures.append(executor.submit(merge_instrument_file, instrument, fetched_dir, raw_dir))
            for fut in merge_futures:
                try:
                    fut.result()
                except Exception as e:
                    logger.exception("Merge job failed: %s", e)

    if args.no_cleanup:
        logger.info("Skipping cleanup (--no-cleanup set).")
        return

    # After merging fetched files into raw storage, remove the fetched directory and final file to save space.
    try:
        if os.path.exists(final_file):
            os.remove(final_file)
            logger.info("Deleted final file: %s", final_file)
    except Exception as e:
        logger.warning("Could not delete %s: %s", final_file, e)

    # Remove fetched per-instrument files directory
    try:
        if os.path.isdir(fetched_dir):
            shutil.rmtree(fetched_dir)
            logger.info("Deleted fetched directory and its files: %s", fetched_dir)
    except Exception as e:
        logger.warning("Could not delete fetched directory %s: %s", fetched_dir, e)

if __name__ == '__main__':
    main()
