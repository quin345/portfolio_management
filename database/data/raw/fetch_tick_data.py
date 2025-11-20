import subprocess
import json
from datetime import datetime, timedelta
import os
from typing import List, Optional
import threading
import time
import random

# --- Configuration ---
NODE_MEMORY_LIMIT = int(os.environ.get('NODE_MEMORY_LIMIT', '8192'))  # In MB
PROCESS_TIMEOUT = int(os.environ.get('PROCESS_TIMEOUT', '90'))  # In seconds
FETCHER_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), 'fetcher.js')

# Rate limiting / retry configuration (tunable via environment variables)
MAX_CONCURRENT_FETCHES = int(os.environ.get('MAX_CONCURRENT_FETCHES', '6'))
FETCH_MAX_RETRIES = int(os.environ.get('FETCH_MAX_RETRIES', '3'))
FETCH_BACKOFF_BASE = float(os.environ.get('FETCH_BACKOFF_BASE', '0.5'))
FETCH_BACKOFF_MAX = float(os.environ.get('FETCH_BACKOFF_MAX', '10'))
FETCH_REQUEST_DELAY = float(os.environ.get('FETCH_REQUEST_DELAY', '0.2'))

# Semaphore to limit concurrent Node.js fetch processes
FETCH_SEMAPHORE = threading.BoundedSemaphore(MAX_CONCURRENT_FETCHES)


def _build_command(asset: str, date: datetime) -> List[str]:
    next_date = date + timedelta(days=1)
    from_date_iso = date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    to_date_iso = next_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')

    return [
        'node',
        f'--max-old-space-size={NODE_MEMORY_LIMIT}',
        FETCHER_SCRIPT_PATH,
        asset,
        from_date_iso,
        to_date_iso,
    ]


def fetch_tick_data_for_day(asset: str, date: datetime) -> List[dict]:
    """Fetch tick data for a single UTC day for `asset` by invoking the Node.js fetcher.

    Returns a list of dictionaries (tick points) or an empty list on error.
    """
    cmd = _build_command(asset, date)

    attempt = 0
    while attempt < FETCH_MAX_RETRIES:
        attempt += 1
        acquired = False
        try:
            FETCH_SEMAPHORE.acquire()
            acquired = True

            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=PROCESS_TIMEOUT, check=False)
            except FileNotFoundError:
                print(f"❌ Node or fetcher script not found: {cmd[0]} or {FETCHER_SCRIPT_PATH}")
                return []
            except subprocess.TimeoutExpired:
                print(f"⏱️ Timeout (attempt {attempt}) fetching data for {asset} on {date.strftime('%Y-%m-%d')}")
                result = None

            if result is None:
                # Will backoff below
                pass
            elif result.returncode != 0:
                stderr = result.stderr.strip() if result.stderr else '<no stderr>'
                print(f"❌ Node.js error (attempt {attempt}) for {asset} on {date.strftime('%Y-%m-%d')}: {stderr}")
            else:
                try:
                    payload = json.loads(result.stdout or '[]')
                    # gentle throttle after a successful call
                    time.sleep(FETCH_REQUEST_DELAY)
                    return payload
                except json.JSONDecodeError as e:
                    raw = (result.stdout or '')[:400]
                    print(f"❌ JSON decode error for {asset} on {date.strftime('%Y-%m-%d')}: {e}\nRaw output: '{raw}'")

        finally:
            if acquired:
                try:
                    FETCH_SEMAPHORE.release()
                except Exception:
                    pass

        # If we get here, we will retry with exponential backoff + jitter
        if attempt < FETCH_MAX_RETRIES:
            backoff = min(FETCH_BACKOFF_MAX, FETCH_BACKOFF_BASE * (2 ** (attempt - 1)))
            # add jitter
            sleep_time = backoff * (0.5 + random.random() / 2.0)
            print(f"⏳ Backing off for {sleep_time:.2f}s before retry {attempt + 1} for {asset} {date.strftime('%Y-%m-%d')}")
            time.sleep(sleep_time)

    print(f"❌ Failed to fetch data for {asset} on {date.strftime('%Y-%m-%d')} after {FETCH_MAX_RETRIES} attempts")
    return []


def fetch_and_store_tick_data(start_date: datetime, end_date: datetime, asset: str, save_dir: Optional[str] = None, store_func=None) -> int:
    """Fetch tick data for asset in [start_date, end_date) and store using `store_func`.

    - `store_func` should accept (df: pandas.DataFrame, asset: str, save_dir: str).
    - Returns number of days successfully fetched and stored.
    """
    if save_dir is None:
        save_dir = os.path.join(os.path.dirname(__file__), '..', '2015_tick_data')

    # Lazy import to avoid heavy dependency if not used
    import pandas as pd

    if store_func is None:
        try:
            from .store_tick_data import store_tick_data as default_store
        except Exception:
            try:
                # fallback relative import when running as script
                from store_tick_data import store_tick_data as default_store
            except Exception:
                default_store = None
        store_func = default_store

    current = start_date
    saved_days = 0
    while current < end_date:
        data = fetch_tick_data_for_day(asset, current)
        if data:
            df = pd.DataFrame(data)
            if 'timestamp' in df.columns and not df.empty and store_func is not None:
                try:
                    store_func(df, asset, save_dir)
                    saved_days += 1
                    print(f"✅ Stored {asset} {current.strftime('%Y-%m-%d')}")
                except Exception as e:
                    print(f"❌ Error storing {asset} {current.strftime('%Y-%m-%d')}: {e}")
            else:
                print(f"⚠️ Fetched data invalid or no `timestamp` for {asset} {current.strftime('%Y-%m-%d')}")
        else:
            print(f"⚠️ No data for {asset} {current.strftime('%Y-%m-%d')}")

        current = current + timedelta(days=1)

    return saved_days


if __name__ == '__main__':
    # Minimal CLI for ad-hoc use
    import argparse

    parser = argparse.ArgumentParser(description='Fetch tick data for an asset and date range.')
    parser.add_argument('--start', required=True, help='Start date YYYY-MM-DD')
    parser.add_argument('--end', required=True, help='End date YYYY-MM-DD (exclusive)')
    parser.add_argument('--asset', required=True, help='Asset symbol (e.g., eurusd)')
    parser.add_argument('--save-dir', required=False, help='Directory to save HDF5 files')
    args = parser.parse_args()

    s = datetime.strptime(args.start, '%Y-%m-%d')
    e = datetime.strptime(args.end, '%Y-%m-%d')
    count = fetch_and_store_tick_data(s, e, args.asset, save_dir=args.save_dir)
    print(f"Finished. Days saved: {count}")