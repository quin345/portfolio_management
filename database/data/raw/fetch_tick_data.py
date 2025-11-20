import subprocess
import json
from datetime import datetime, timedelta
import os
from typing import List, Optional
import threading
import time
import random
import logging

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

# logging
LOG_PATH = os.path.join(os.path.dirname(__file__), 'fetch_tick_data.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[logging.FileHandler(LOG_PATH, encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


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
                logger.error("Node or fetcher script not found: %s or %s", cmd[0], FETCHER_SCRIPT_PATH)
                return []
            except subprocess.TimeoutExpired:
                logger.warning("Timeout (attempt %d) fetching data for %s on %s", attempt, asset, date.strftime('%Y-%m-%d'))
                result = None

            if result is None:
                # Will backoff below
                pass
            elif result.returncode != 0:
                stderr = result.stderr.strip() if result.stderr else '<no stderr>'
                logger.warning("Node.js error (attempt %d) for %s on %s: %s", attempt, asset, date.strftime('%Y-%m-%d'), stderr)
            else:
                try:
                    payload = json.loads(result.stdout or '[]')
                    # gentle throttle after a successful call
                    time.sleep(FETCH_REQUEST_DELAY)
                    return payload
                except json.JSONDecodeError as e:
                    raw = (result.stdout or '')[:400]
                    logger.error("JSON decode error for %s on %s: %s -- Raw: %s", asset, date.strftime('%Y-%m-%d'), e, raw)

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
            logger.info("Backing off for %.2fs before retry %d for %s %s", sleep_time, attempt + 1, asset, date.strftime('%Y-%m-%d'))
            time.sleep(sleep_time)

    logger.error("Failed to fetch data for %s on %s after %d attempts", asset, date.strftime('%Y-%m-%d'), FETCH_MAX_RETRIES)
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
                    logger.info("Stored %s %s", asset, current.strftime('%Y-%m-%d'))
                except Exception as e:
                    logger.exception("Error storing %s %s: %s", asset, current.strftime('%Y-%m-%d'), e)
            else:
                logger.warning("Fetched data invalid or no `timestamp` for %s %s", asset, current.strftime('%Y-%m-%d'))
        else:
            logger.warning("No data for %s %s", asset, current.strftime('%Y-%m-%d'))

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
    parser.add_argument('--log-file', required=False, help='Optional log file path (overrides default)')
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
        logger = logging.getLogger(__name__)

    s = datetime.strptime(args.start, '%Y-%m-%d')
    e = datetime.strptime(args.end, '%Y-%m-%d')
    count = fetch_and_store_tick_data(s, e, args.asset, save_dir=args.save_dir)
    logger.info("Finished. Days saved: %d", count)