import os
import csv
import h5py
import calendar
import argparse
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed

# === Argument Parsing ===
def parse_args():
    parser = argparse.ArgumentParser(description="Scan HDF5 tick data for integrity.")
    parser.add_argument("--start-date", type=str, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", type=str, help="End date in YYYY-MM-DD format")
    parser.add_argument("--instrument", type=str, help="Instrument name or comma-separated list to scan (optional)")
    args = parser.parse_args()
    start = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else None
    end = datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else None
    instruments = None
    if args.instrument:
        instruments = [s.strip() for s in args.instrument.split(",") if s.strip()]
        if not instruments:
            instruments = None
    return start, end, instruments

# === Date Utilities ===
def valid_dates(year, month):
    for day in range(1, calendar.monthrange(year, month)[1] + 1):
        date_obj = datetime(year, month, day)
        if date_obj.weekday() != 5:  # Skip Saturdays
            yield date_obj

# === Dataset Validation ===
def is_dataset_good(dset):
    try:
        _ = dset[...]
        return True
    except Exception:
        return False

# === HDF5 File Scanner ===
def scan_hdf5(file_path, start_date=None, end_date=None, instruments=None):
    last_updates, missing_groups, missing_tables = [], [], []

    with h5py.File(file_path, "r") as f:
        available = list(f.keys())
        if instruments:
            # only scan requested instruments that exist in file
            to_scan = [inst for inst in instruments if inst in available]
        else:
            to_scan = available

        for instrument in to_scan:
            last_good_date = None
            for year_key in sorted(f[instrument].keys(), key=lambda x: int(x[1:])):
                year_group = f[instrument][year_key]
                year = int(year_key[1:])

                for month_key in sorted(year_group.keys(), key=lambda x: int(x[1:])):
                    month_group = year_group[month_key]
                    month = int(month_key[1:])

                    for date_obj in valid_dates(year, month):
                        date_str = date_obj.strftime("%Y-%m-%d")
                        day_key = f'd{str(date_obj.day).zfill(2)}'

                        if start_date and date_obj < start_date:
                            continue
                        if end_date and date_obj > end_date:
                            continue

                        try:
                            day_group = month_group[day_key]
                            if "table" in day_group and is_dataset_good(day_group["table"]):
                                last_good_date = date_str
                            else:
                                missing_tables.append([instrument, date_str])
                        except KeyError:
                            missing_groups.append([instrument, date_str])

            if last_good_date:
                last_updates.append([instrument, last_good_date])

    return last_updates, missing_groups, missing_tables

# === CSV Writers ===
def write_csv(filename, header, rows):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

def write_grouped_csv(filename, grouped_data):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Instrument", "Missing Day Groups"])
        for instrument, dates in grouped_data.items():
            writer.writerow([instrument, dates])

def write_missing_day_summary(filename, grouped_data):
    total_missing = sum(len(dates) for dates in grouped_data.values())
    rows = [[instrument, len(dates)] for instrument, dates in grouped_data.items()]
    rows.append(["TOTAL", total_missing])
    write_csv(filename, ["Instrument", "Missing Day Count"], rows)

# === Grouping Utility ===
def group_missing_days(missing_groups):
    grouped = defaultdict(list)
    for instrument, date_str in missing_groups:
        grouped[instrument].append(date_str)
    return grouped

# === Worker Wrapper ===
def process_file(args):
    filename, folder_path, start_date, end_date, instruments = args
    file_path = os.path.join(folder_path, filename)
    try:
        print(f"🔍 Scanning {filename}...")
        last_rows, group_rows, table_rows = scan_hdf5(file_path, start_date, end_date, instruments)
        print(f"✅ {filename}: {len(last_rows)} updates, {len(group_rows)} missing groups, {len(table_rows)} missing tables")
        return last_rows, group_rows, table_rows
    except Exception as e:
        print(f"❌ Error scanning {filename}: {e}")
        return [], [], []

# === Main Execution ===
def main():
    start_date, end_date, instruments = parse_args()
    folder_path = "2015_tick_data"

    output_files = {
        "last_update": "last_tick_update.csv",
        "missing_groups": "missing_day_group.csv",
        "missing_tables": "missing_table.csv",
        "summary": "missing_day_summary.csv"
    }

    h5_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".h5")]
    if instruments:
        print(f"📁 Found {len(h5_files)} HDF5 files in {folder_path} — scanning instruments: {', '.join(instruments)}")
    else:
        print(f"📁 Found {len(h5_files)} HDF5 files in {folder_path}")

    all_last_updates, all_missing_groups, all_missing_tables = [], [], []

    with ProcessPoolExecutor(max_workers=28) as executor:
        tasks = [(f, folder_path, start_date, end_date, instruments) for f in h5_files]
        futures = [executor.submit(process_file, task) for task in tasks]
        for future in as_completed(futures):
            last_rows, group_rows, table_rows = future.result()
            all_last_updates.extend(last_rows)
            all_missing_groups.extend(group_rows)
            all_missing_tables.extend(table_rows)

    grouped_missing = group_missing_days(all_missing_groups)

    write_csv(output_files["last_update"], ["Instrument", "Last Good Date"], all_last_updates)
    write_grouped_csv(output_files["missing_groups"], grouped_missing)
    write_missing_day_summary(output_files["summary"], grouped_missing)
    write_csv(output_files["missing_tables"], ["Instrument", "Missing Table Dataset"], all_missing_tables)

    print("🏁 Scan completed.")
    for label, path in output_files.items():
        print(f"→ {label.replace('_', ' ').title()} saved to: {path}")

if __name__ == "__main__":
    main()