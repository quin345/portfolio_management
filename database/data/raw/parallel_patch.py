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

import pandas as pd

from fetch_tick_data import fetch_tick_data_for_day


# === Step 1: Parse CSV ===
CSV_FILE = "missing_day_group.csv"


def parse_missing_groups(csv_file: str = CSV_FILE) -> defaultdict:
    instrument_dates = defaultdict(list)
    if not os.path.exists(csv_file):
        print(f"⚠️ CSV not found: {csv_file}")
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
                        print(f"⚠️ Invalid date format: {date_str}")
            except Exception as e:
                print(f"⚠️ Failed to parse date list for {instrument}: {e}")

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
            print(f"🧵 Worker {worker_id}: {instrument} {date.date()}")
            data = fetch_tick_data_for_day(instrument, date)
            # small randomized delay between requests to avoid bursts
            time.sleep(random.uniform(0.05, 0.2))

            if not data:
                print(f"⚠️ Worker {worker_id} no data for {instrument} {date.date()}")
                continue

            df = pd.DataFrame(data)
            if df.empty or 'timestamp' not in df.columns:
                print(f"⚠️ Worker {worker_id} fetched invalid data for {instrument} {date.date()}")
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

            print(f"✅ Worker {worker_id} saved {instrument} {date.date()}")

        except Exception as e:
            print(f"❌ Worker {worker_id} error on {instrument} {date.date()}: {e}")


def merge_hdf5_files(temp_files: List[str], final_file: str):
    with pd.HDFStore(final_file, mode='a') as final_store:
        for temp in temp_files:
            if not os.path.exists(temp):
                print(f"⚠️ Skipping missing file: {temp}")
                continue
            try:
                with pd.HDFStore(temp, mode='r') as temp_store:
                    keys = temp_store.keys()
                    if not keys:
                        print(f"⚠️ Skipping empty file: {temp}")
                        continue
                    for key in keys:
                        df = temp_store[key]
                        final_store.put(key, df, format='table', data_columns=True)
            except Exception as e:
                print(f"❌ Error reading {temp}: {e}")
            finally:
                try:
                    os.remove(temp)
                    print(f"🗑️ Deleted temp file: {temp}")
                except Exception as e:
                    print(f"⚠️ Could not delete {temp}: {e}")


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
        print(f"⚠️ Fetched file missing: {instrument}")
        return
    if not os.path.exists(raw_path):
        print(f"⚠️ Raw file missing: {instrument}")
        return

    with pd.HDFStore(raw_path, mode='a') as raw_store, pd.HDFStore(fetched_path, mode='r') as fetched_store:
        for key in fetched_store.keys():
            if key in raw_store:
                print(f"🔁 Skipping duplicate key: {key} in {instrument}")
                continue
            df = fetched_store[key]
            raw_store.put(key, df, format='table', data_columns=True)

    print(f"✅ Merged fetched → raw: {instrument}")


def main():
    instrument_dates = parse_missing_groups(CSV_FILE)
    all_tasks = [(instr, date) for instr, dates in instrument_dates.items() for date in dates]

    # Decide number of workers; make conservative default to avoid overloading server
    env_workers = int(os.environ.get('PARALLEL_MAX_WORKERS', '6'))
    desired_workers = min(env_workers, max(1, len(all_tasks)))
    task_chunks = chunkify(all_tasks, desired_workers)

    with ThreadPoolExecutor(max_workers=desired_workers) as executor:
        for i, chunk in enumerate(task_chunks):
            if chunk:
                executor.submit(worker, i, chunk)

    temp_files = [f"temp_worker_{i}.h5" for i in range(len(task_chunks))]
    merge_hdf5_files(temp_files, "final_tick_data.h5")
    decompose_by_instrument("final_tick_data.h5")
    print("✅ All tasks complete. Data split by instrument.")

    # MERGE TO RAW FILES
    fetched_dir = "split_by_instrument"
    raw_dir = "./2015_tick_data"

    instruments = [
        filename.replace("_tick_data.h5", "")
        for filename in os.listdir(fetched_dir)
        if filename.endswith("_tick_data.h5")
    ]

    with ThreadPoolExecutor(max_workers=min(28, max(1, len(instruments)))) as executor:
        for instrument in instruments:
            executor.submit(merge_instrument_file, instrument, fetched_dir, raw_dir)

    # After merging fetched files into raw storage, remove the fetched directory and final file to save space.
    try:
        if os.path.exists("final_tick_data.h5"):
            os.remove("final_tick_data.h5")
            print("🗑️ Deleted final file: final_tick_data.h5")
    except Exception as e:
        print(f"⚠️ Could not delete final_tick_data.h5: {e}")

    # Remove fetched per-instrument files directory
    try:
        if os.path.isdir(fetched_dir):
            shutil.rmtree(fetched_dir)
            print(f"🗑️ Deleted fetched directory and its files: {fetched_dir}")
    except Exception as e:
        print(f"⚠️ Could not delete fetched directory {fetched_dir}: {e}")

if __name__ == '__main__':
    main()
