"""Small CLI wrapper to fetch and store tick data for a single asset and date range."""

import argparse
from datetime import datetime
import logging
import os

from fetch_tick_data import fetch_and_store_tick_data


LOG_PATH = os.path.join(os.path.dirname(__file__), 'manual_update.log')
logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s %(levelname)s: %(message)s',
	handlers=[logging.FileHandler(LOG_PATH, encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def main():
	parser = argparse.ArgumentParser(description="Fetch tick data from Dukascopy")
	parser.add_argument("--start", required=True, help="Start date in YYYY-MM-DD format")
	parser.add_argument("--end", required=True, help="End date in YYYY-MM-DD format (exclusive)")
	parser.add_argument("--asset", required=True, help="Asset symbol (e.g., eurusd, xauusd)")
	parser.add_argument("--save-dir", required=False, help="Directory to save HDF5 files")
	parser.add_argument("--log-file", required=False, help="Optional log file path (overrides default)")

	args = parser.parse_args()

	if args.log_file:
		for h in list(logger.handlers):
			logger.removeHandler(h)
		logging.basicConfig(
			level=logging.INFO,
			format='%(asctime)s %(levelname)s: %(message)s',
			handlers=[logging.FileHandler(args.log_file, encoding='utf-8'), logging.StreamHandler()]
		)
		logger = logging.getLogger(__name__)

	start_date = datetime.strptime(args.start, "%Y-%m-%d")
	end_date = datetime.strptime(args.end, "%Y-%m-%d")
	asset = args.asset

	logger.info("Starting manual fetch for %s from %s to %s", asset, start_date.date(), end_date.date())
	count = fetch_and_store_tick_data(start_date, end_date, asset, save_dir=args.save_dir)
	logger.info("Finished. Days saved: %d", count)


if __name__ == '__main__':
	main()