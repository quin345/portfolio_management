Database raw data tools
======================

This folder contains utilities used to scan, fetch, patch and store raw tick HDF5 files. The scripts are intentionally small and procedural so you can run them step-by-step, inspect intermediate files, and tune concurrency to avoid overloading remote servers.

Overview
- `fetch_tick_data.py`: Python wrapper that invokes the Node.js `fetcher.js` to retrieve tick data for a single day. Provides `fetch_tick_data_for_day()` and `fetch_and_store_tick_data()` plus a CLI for ad-hoc fetches.
- `store_tick_data.py`: Helpers to persist fetched data into per-instrument HDF5 files under `2015_tick_data` partitioned by year/month/day. It deduplicates and sorts rows by `timestamp`.
- `parallel_patch.py`: Drives parallel fetching for many missing days, merges temporary HDF5 files into `final_tick_data.h5`, decomposes into `split_by_instrument/`, then merges those into `2015_tick_data` and cleans up temporary files.
- `batch_update.py`: Reads `last_tick_update.csv` and fetches until a provided `--end-date` in parallel.
- `manual_update.py`: CLI wrapper to fetch a date range for a single `--asset`.
- `scanner.py`: Scans existing HDF5 files for missing groups/tables and writes these CSV outputs used by the other scripts.

Quick start — dependencies
- Python packages: `pandas`, `tables` (PyTables), `h5py` (as required). Install via:

```powershell
pip install pandas tables h5py
```

- Node.js is required and `fetcher.js` must be present in this folder. The Python wrapper executes `node fetcher.js ...`.

Important environment variables (tuning and safety)
- `MAX_CONCURRENT_FETCHES` (default `6`): max concurrent Node.js fetch processes (controlled by a semaphore in the Python wrapper).
- `FETCH_MAX_RETRIES` (default `3`): number of retry attempts per day on error.
- `FETCH_BACKOFF_BASE` (default `0.5`): base seconds for exponential backoff.
- `FETCH_BACKOFF_MAX` (default `10`): maximum backoff seconds.
- `FETCH_REQUEST_DELAY` (default `0.2`): small pause after a successful fetch to avoid bursts.
- `PARALLEL_MAX_WORKERS` (default `6`): worker count for `parallel_patch.py` (conservative default).
- `BATCH_MAX_WORKERS` (default `8`): worker count for `batch_update.py`.
- `NODE_MEMORY_LIMIT` and `PROCESS_TIMEOUT` are also honored by the fetcher wrapper.

CLI examples

PowerShell (Windows):

```powershell
# Ad-hoc fetch for asset eurusd over two days
python database\data\raw\fetch_tick_data.py --start 2015-01-01 --end 2015-01-03 --asset eurusd

# Run parallel patch with conservative worker count
$env:PARALLEL_MAX_WORKERS='6'; python database\data\raw\parallel_patch.py

# Batch update using last_tick_update.csv
python database\data\raw\batch_update.py --end-date 2015-01-10 --csv last_tick_update.csv
```

Flags and logging
- `--workers N`: set number of worker threads (overrides `PARALLEL_MAX_WORKERS` / `BATCH_MAX_WORKERS`).
- `--no-cleanup`: (parallel patch) skip deleting `final_tick_data.h5` and the `split_by_instrument/` folder.
- `--csv PATH`: choose an alternate CSV input file for `parallel_patch.py` or `batch_update.py`.
- `--log-file PATH`: optional flag for `fetch_tick_data.py` and `batch_update.py` to write logs to a specific file (overrides default log path).
 - `--log-file PATH`: optional flag for `fetch_tick_data.py`, `batch_update.py` and `manual_update.py` to write logs to a specific file (overrides default log path).

Examples with flags:

```powershell
# Parallel patch with explicit workers and keep intermediate files
python database\data\raw\parallel_patch.py --workers 4 --no-cleanup

# Batch update with 6 workers and custom log file
python database\data\raw\batch_update.py --end-date 2015-01-10 --csv last_tick_update.csv --workers 6 --log-file c:\tmp\batch.log

# Fetcher with custom log file
python database\data\raw\fetch_tick_data.py --start 2015-01-01 --end 2015-01-03 --asset eurusd --log-file c:\tmp\fetch.log
```

Command Prompt (cmd.exe):

```cmd
set PARALLEL_MAX_WORKERS=6 && python database\data\raw\parallel_patch.py
```

Bash (Linux/macOS):

```bash
PARALLEL_MAX_WORKERS=6 python database/data/raw/parallel_patch.py
```

CSV formats and scanner outputs
- `last_tick_update.csv` (used by `batch_update.py`): CSV with header and rows like:

```
instrument,last_good_date
eurusd,2015-01-10
xauusd,2015-01-08
```

- `missing_day_group.csv` (produced by `scanner.py`): CSV with rows mapped to instruments and a Python-style list of missing date strings. Example row:

```
eurusd,["2015-01-05","2015-01-07"]
```

- `missing_day_summary.csv` contains a per-instrument missing day count; `missing_table.csv` lists missing table datasets.

Cleanup behavior (important)
- `parallel_patch.py` will attempt to delete temporary `temp_worker_*.h5` files during merging.
- After decomposing `final_tick_data.h5` into `split_by_instrument/` and merging those into `2015_tick_data/`, `parallel_patch.py` will by default remove `final_tick_data.h5` and delete the `split_by_instrument` directory and its contents.
- These deletions are unconditional in the current implementation. If you want to keep intermediate files for inspection or debugging, back them up or request a `--no-cleanup`/`--cleanup` flag (I can add this quickly).

Troubleshooting
- Node or `fetcher.js` not found: the fetcher wrapper logs an error. Ensure `node` is on `PATH` and `fetcher.js` is in this folder.
- JSON decode errors: the wrapper logs part of the raw output. Inspect the log file (or console) and run `node fetcher.js ...` manually to see stdout/stderr.
- 429s / server errors: lower `PARALLEL_MAX_WORKERS`, `MAX_CONCURRENT_FETCHES` or raise `FETCH_REQUEST_DELAY` and `FETCH_BACKOFF_BASE`.
- Memory: increase `NODE_MEMORY_LIMIT` or lower concurrency if your machine runs out of memory.
- Disk space: the pipeline writes HDF5 files; ensure sufficient free space before running large patches.

Recommended workflow
1. Run `scanner.py` to produce `last_tick_update.csv` and `missing_day_group.csv`.
2. Inspect CSVs to see scale of missing data.
3. Start with conservative env settings (e.g., `PARALLEL_MAX_WORKERS=4`, `MAX_CONCURRENT_FETCHES=4`) and run `parallel_patch.py` on a small subset.
4. Increase concurrency gradually if stable.
5. For production-scale fetches consider batching multiple days per Node.js call (requires updating `fetcher.js`) to reduce process churn.

Next improvements I can make for you
- Add a `--no-cleanup` flag to `parallel_patch.py` so deletions are opt-in.
- Add a `--workers` CLI flag to override env vars directly.
- Replace prints with structured `logging` and add a `--log-file` option.
- Implement adaptive throttling (reduce concurrency automatically on repeated server errors).

If you'd like any of those, tell me which and I'll implement it.

at the current directory

for manual update, use `manual_update.py`

`python manual_update.py --start 2015-01-01 --end 2016-01-01 --asset usdjpy`

scanner
no date default to all data to date from 2015
`python scanner.py --start-date 2015-01-01 --end-date 2025-11-08`

batch update updates all instruments at once 
`batch_update.py`