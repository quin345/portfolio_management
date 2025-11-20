const { getHistoricalRates } = require('dukascopy-node');

/**
 * This script fetches historical tick data from Dukascopy.
 * It accepts three command-line arguments:
 * 1. instrument: The financial instrument (e.g., 'eurusd').
 * 2. fromDate: The start date in ISO format (e.g., '2023-01-01T00:00:00.000Z').
 * 3. toDate: The end date in ISO format (e.g., '2023-01-02T00:00:00.000Z').
 */
(async () => {
  const [,, instrument, fromDateStr, toDateStr] = process.argv;

  if (!instrument || !fromDateStr || !toDateStr) {
    console.error('Error: Missing arguments. Usage: node fetcher.js <instrument> <fromDate> <toDate>');
    process.exit(1);
  }

  try {
    const data = await getHistoricalRates({
      instrument,
      dates: {
        from: new Date(fromDateStr),
        to: new Date(toDateStr)
      },
      timeframe: 'tick',
      format: 'json',
      batchSize: 10,
      pauseBetweenBatchesMs: 500
    });
    console.log(JSON.stringify(data));
  } catch (error) {
    console.error(`Error fetching data for ${instrument}:`, error.message);
    process.exit(1);
  }
})();