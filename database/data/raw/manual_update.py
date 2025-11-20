"""Small CLI wrapper to fetch and store tick data for a single asset and date range."""

import argparse
from datetime import datetime
from fetch_tick_data import fetch_and_store_tick_data


def main():
	parser = argparse.ArgumentParser(description="Fetch tick data from Dukascopy")
	parser.add_argument("--start", required=True, help="Start date in YYYY-MM-DD format")
	parser.add_argument("--end", required=True, help="End date in YYYY-MM-DD format (exclusive)")
	parser.add_argument("--asset", required=True, help="Asset symbol (e.g., eurusd, xauusd)")
	parser.add_argument("--save-dir", required=False, help="Directory to save HDF5 files")

	args = parser.parse_args()

	start_date = datetime.strptime(args.start, "%Y-%m-%d")
	end_date = datetime.strptime(args.end, "%Y-%m-%d")
	asset = args.asset

	fetch_and_store_tick_data(start_date, end_date, asset, save_dir=args.save_dir)


if __name__ == '__main__':
	main()