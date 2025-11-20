import os
from typing import Optional

import pandas as pd
import warnings
from tables import NaturalNameWarning

warnings.filterwarnings("ignore", category=NaturalNameWarning)


def store_tick_data(df: pd.DataFrame, asset: str, save_dir: str = "2015_tick_data") -> None:
    """Store tick dataframe into HDF5 partitioned by year/month/day.

    Ensures no duplicate timestamps are stored and data is sorted by timestamp.
    """
    if df is None or df.empty:
        print(f"⚠️ No data to store for {asset}")
        return

    if 'timestamp' not in df.columns:
        print(f"⚠️ Dataframe missing 'timestamp' column for {asset}")
        return

    ts = pd.to_datetime(df['timestamp'], unit='ms')
    df = df.copy()
    df['year'] = ts.dt.year
    df['month'] = ts.dt.month
    df['day'] = ts.dt.day

    hdf5_path = os.path.join(save_dir, f"{asset}_tick_data.h5")
    os.makedirs(save_dir, exist_ok=True)

    with pd.HDFStore(hdf5_path, mode='a') as store:
        for (y, m, d), group in df.groupby(['year', 'month', 'day']):
            key = f"/{asset}/y{y}/m{m:02}/d{d:02}"
            payload = group.drop(columns=['year', 'month', 'day']).copy()

            if key in store:
                existing = store[key]
                combined = pd.concat([existing, payload], ignore_index=True)
            else:
                combined = payload

            # Deduplicate by timestamp and sort
            if 'timestamp' in combined.columns:
                combined = combined.drop_duplicates(subset=['timestamp']).sort_values('timestamp').reset_index(drop=True)

            store.put(key, combined, format='table', data_columns=True)