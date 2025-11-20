Database raw data tools
======================

This folder contains small utilities used to scan, fetch, patch and store raw tick HDF5 files.

Overview
 - `fetch_tick_data.py`: Python wrapper that invokes the Node.js `fetcher.js` to retrieve tick data for a single day. Provides `fetch_tick_data_for_day()` and `fetch_and_store_tick_data()` plus a small CLI for ad-hoc fetches.
 - `store_tick_data.py`: Helpers to persist fetched data into per-instrument HDF5 files under `2015_tick_data` partitioned by year/month/day. It deduplicates and sorts rows by `timestamp`.
 - `parallel_patch.py`: Orchestrates parallel fetching for many missing days (drives `fetch_tick_data_for_day` in workers), merges temporary HDF5 files into `final_tick_data.h5`, decomposes into `split_by_instrument/`, then merges those into `2015_tick_data`.
 - `batch_update.py`: Reads `last_tick_update.csv` and fetches until a provided `--end-date` in parallel.
 - `manual_update.py`: Small CLI wrapper to fetch a date range for one `--asset`.
 - `scanner.py`: Scans existing HDF5 files for missing groups/tables and writes CSV outputs used by the other scripts.

Dependencies
 - Python packages: `pandas`, `tables` (PyTables), `h5py` (as required). Install via:

```powershell
pip install pandas tables h5py
```

 - Node.js and the `fetcher.js` script must be available in this folder. The Python fetcher calls `node` and passes `fetcher.js` as the worker script.

Key environment variables (tuning and safety)
 - `MAX_CONCURRENT_FETCHES` (default `6`): max concurrent Node.js fetch processes (controlled by a semaphore).
 - `FETCH_MAX_RETRIES` (default `3`): number of retry attempts per day on error.
 - `FETCH_BACKOFF_BASE` (default `0.5`): base seconds for exponential backoff.
 - `FETCH_BACKOFF_MAX` (default `10`): ceiling for backoff seconds.
 - `FETCH_REQUEST_DELAY` (default `0.2`): small pause after a successful fetch.
 - `PARALLEL_MAX_WORKERS` (default `6`): worker count for `parallel_patch.py` (conservative default to avoid server bursts).
 - `BATCH_MAX_WORKERS` (default `8`): worker count for `batch_update.py`.
 - `NODE_MEMORY_LIMIT` and `PROCESS_TIMEOUT` are also honored by the fetcher wrapper.

Running the scripts (examples)
 - Quick ad-hoc fetch for `eurusd` (PowerShell):

```powershell
python database\data\raw\fetch_tick_data.py --start 2015-01-01 --end 2015-01-03 --asset eurusd
```

 - Run parallel patch with conservative worker count (PowerShell):

```powershell
$env:PARALLEL_MAX_WORKERS='6'; python database\data\raw\parallel_patch.py
```

 - Run batch update using `last_tick_update.csv`:

```powershell
python database\data\raw\batch_update.py --end-date 2015-01-10 --csv last_tick_update.csv
```

Cleanup and safety notes
 - `parallel_patch.py` will delete temporary `temp_worker_*.h5` files during merge, then remove the aggregated `final_tick_data.h5` and delete the `split_by_instrument/` directory after merging those per-instrument files into the raw `2015_tick_data` files.
 - These deletions are unconditional in the current implementation. Back up data you care about before running the full pipeline.

Tuning advice
 - If you see remote server 429s or 5xx errors, reduce `PARALLEL_MAX_WORKERS` and `MAX_CONCURRENT_FETCHES`, or increase `FETCH_REQUEST_DELAY` and `FETCH_BACKOFF_BASE`.
 - Start small (e.g., 2–6 workers), verify stability, then gradually increase.

Further improvements you may want
 - Add a `--no-cleanup` or `--cleanup` flag to `parallel_patch.py` so deletion is explicit/opt-in.
 - Combine multiple days into a single Node.js call (requires changes to `fetcher.js`) to reduce process churn.

Contact
 - If you want, I can add CLI flags for explicit cleanup control, structured logging, or adaptive throttling based on observed errors. Tell me which and I'll implement it.

at the current directory

for manual update, use `manual_update.py`

`python manual_update.py --start 2015-01-01 --end 2016-01-01 --asset usdjpy`

scanner
no date default to all data to date from 2015
`python scanner.py --start-date 2015-01-01 --end-date 2025-11-08`

batch update updates all instruments at once 
`batch_update.py`