# expectedreturns.py
import pandas as pd
import numpy as np


# -----------------------------------------
# Z-score helper
# -----------------------------------------
def zscore(series: pd.Series):
    mean = series.mean()
    std = series.std()

    if std == 0 or np.isnan(std):
        return pd.Series(0, index=series.index)

    return (series - mean) / std


# -----------------------------------------
# Compute realized daily volatility
# -----------------------------------------
def compute_daily_volatility(returns: pd.DataFrame, window=20):
    """
    Computes rolling daily volatility per asset.
    Default window = 20 days.
    """
    vol = returns.rolling(window).std().iloc[-1]

    # Replace missing vol with median
    if vol.isna().any():
        vol = vol.fillna(vol.median())

    return vol


# -----------------------------------------
# Load signals from a tab-separated CSV file
# -----------------------------------------
def load_signals_from_tsv(path: str):
    """
    Expects a TSV file with two columns:
    asset<TAB>signal
    """
    df = pd.read_csv(path, sep="\t", header=None, names=["asset", "signal"])
    df["asset"] = df["asset"].astype(str)
    return df.set_index("asset")["signal"].to_dict()


# -----------------------------------------
# Main expected returns function
# -----------------------------------------
def compute_expected_returns(signals,
                             returns: pd.DataFrame,
                             ic=0.05,
                             vol_target=0.10,
                             vol_window=20,
                             from_file=True):
    """
    Convert raw signals into expected returns using:
    1. Z-scoring
    2. Information Coefficient (IC)
    3. Volatility scaling

    Parameters
    ----------
    signals : dict or str
        If dict → {asset: signal}
        If str → path to TSV file (asset<TAB>signal)

    returns : pd.DataFrame
        Historical returns matrix (time x assets)

    ic : float or dict
        Information coefficient

    vol_target : float
        Target volatility scaling

    vol_window : int
        Rolling window for realized volatility

    from_file : bool
        If True, interpret `signals` as a TSV file path

    Returns
    -------
    pd.Series
        Expected returns per asset
    """

    # Load signals from TSV file if needed
    if from_file:
        signals = load_signals_from_tsv(signals)

    # Convert to Series and align with returns
    sig = pd.Series(signals, dtype=float)
    sig = sig.reindex(returns.columns).fillna(0)

    # Step 1 — Z-score signals
    z = zscore(sig)
    

    # Step 2 — Realized volatility
    realized_vol = compute_daily_volatility(returns, window=vol_window)

    # Step 3 — IC scaling
    if isinstance(ic, dict):
        ic_series = pd.Series(ic).reindex(z.index).fillna(np.mean(list(ic.values())))
    else:
        ic_series = pd.Series(ic, index=z.index)

    ic_scaled = z * ic_series

    # Step 4 — Volatility scaling
    expected = ic_scaled * (vol_target / realized_vol)

    return expected