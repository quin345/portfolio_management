import pandas as pd
import matplotlib.pyplot as plt
from talib import abstract




# ---------------------------------------------------------
# 2. SIGNAL GENERATORS
# ---------------------------------------------------------
import pandas as pd
from talib import abstract

# ---------------------------------------------------------
# Helper: convert df → TA‑Lib OHLCV dict
# ---------------------------------------------------------
def df_to_ohlcv(df):
    return {
        "open": df["open"].astype(float),
        "high": df["high"].astype(float),
        "low": df["low"].astype(float),
        "close": df["close"].astype(float),
        "volume": df["volume"].astype(float),
    }

# ---------------------------------------------------------
# 1. MA CROSSOVER
# ---------------------------------------------------------
def factor_ma_cross(df, fast=10, slow=30):
    ohlcv = df_to_ohlcv(df)

    # TA‑Lib returns numpy arrays → convert to pandas Series
    fast_ma = pd.Series(abstract.SMA(ohlcv, timeperiod=fast), index=df.index)
    slow_ma = pd.Series(abstract.SMA(ohlcv, timeperiod=slow), index=df.index)

    # Initialize signal
    signal = pd.Series(0, index=df.index)

    # Bullish crossover
    signal[(fast_ma > slow_ma) & (fast_ma.shift(1) <= slow_ma.shift(1))] = 1

    # Bearish crossover
    signal[(fast_ma < slow_ma) & (fast_ma.shift(1) >= slow_ma.shift(1))] = -1

    return signal

# ---------------------------------------------------------
# 2. BOLLINGER BANDS (MEAN REVERSION)
# ---------------------------------------------------------
def factor_bbands(
    df,
    period=20,
    num_std=1,
    vol_filter=False,
    vol_window=50,
    vol_quantile=0.5
):
    """
    Bollinger Bands mean reversion with optional volatility filter.
    If vol_filter=True, trades only when volatility is below a chosen quantile.
    """

    ohlcv = df_to_ohlcv(df)

    # --- Bollinger Bands ---
    upper, middle, lower = abstract.BBANDS(
        ohlcv,
        timeperiod=period,
        nbdevup=float(num_std),
        nbdevdn=float(num_std),
        matype=0
    )

    upper = pd.Series(upper, index=df.index)
    lower = pd.Series(lower, index=df.index)

    # --- Raw signal (mean reversion) ---
    signal = pd.Series(0, index=df.index)
    signal[df["close"] < lower] = 1
    signal[df["close"] > upper] = -1

    # --- Optional Volatility Filter ---
    if vol_filter:
        vol = df["close"].pct_change().rolling(vol_window).std()
        vol_threshold = vol.quantile(vol_quantile)
        signal[vol > vol_threshold] = 0

    return signal

# ---------------------------------------------------------
# 3. MACD CROSSOVER
# ---------------------------------------------------------
def factor_macd(df, fast=12, slow=26, signal_period=9):
    ohlcv = df_to_ohlcv(df)

    macd, macd_signal, macd_hist = abstract.MACD(
        ohlcv,
        fastperiod=fast,
        slowperiod=slow,
        signalperiod=signal_period
    )

    macd = pd.Series(macd, index=df.index)
    macd_signal = pd.Series(macd_signal, index=df.index)

    signal = pd.Series(0, index=df.index)
    signal[(macd < macd_signal) & (macd.shift(1) >= macd_signal.shift(1))] = 1
    signal[(macd > macd_signal) & (macd.shift(1) <= macd_signal.shift(1))] = -1
    return signal


# ---------------------------------------------------------
# 4. COMBINED BBANDS + MACD FACTOR  
# ---------------------------------------------------------

def factor_bbands_macd(
    df,
    bb_period=20,
    bb_std=1,
    macd_fast=12,
    macd_slow=26,
    macd_signal=9,
    vol_filter=True,
    vol_window=50,
    vol_quantile=0.5,
    **kwargs
):
    """
    Combined Bollinger Bands + MACD factor.
    BBANDS gives mean-reversion signals.
    MACD confirms trend direction.
    Optional volatility filter.
    """

    ohlcv = df_to_ohlcv(df)

    # --- Bollinger Bands ---
    upper, middle, lower = abstract.BBANDS(
        ohlcv,
        timeperiod=bb_period,
        nbdevup=float(bb_std),
        nbdevdn=float(bb_std),
        matype=0
    )

    upper = pd.Series(upper, index=df.index)
    lower = pd.Series(lower, index=df.index)

    # --- MACD ---
    macd, macd_signal_line, macd_hist = abstract.MACD(
        ohlcv,
        fastperiod=macd_fast,
        slowperiod=macd_slow,
        signalperiod=macd_signal
    )

    macd_hist = pd.Series(macd_hist, index=df.index)

    # --- Raw BBANDS signal ---
    signal = pd.Series(0, index=df.index)
    signal[df["close"] < lower] = 1
    signal[df["close"] > upper] = -1

    # --- MACD confirmation ---
    # Only keep signals aligned with MACD direction
    signal[(signal == -1) & (macd_hist < 0)] = 0
    signal[(signal == 1) & (macd_hist > 0)] = 0

    # --- Optional Volatility Filter ---
    if vol_filter:
        vol = df["close"].pct_change().rolling(vol_window).std()
        vol_threshold = vol.quantile(vol_quantile)
        signal[vol > vol_threshold] = 0

    return signal


# ---------------------------------------------------------
# MAIN INTERFACE
# ---------------------------------------------------------
def generate_signal(df, factor="ma", **kwargs):
    """
    factor options:
        "ma"     → Moving Average Crossover
        "bbands" → Bollinger Bands Mean Reversion
        "macd"   → MACD Crossover
    kwargs are passed to the factor function
    """
    factor = factor.strip().lower() 
    if factor == "ma":
        return factor_ma_cross(df, **kwargs)

    elif factor == "bbands":
        return factor_bbands(df, **kwargs)

    elif factor == "macd":
        return factor_macd(df, **kwargs)
    
    elif factor == "bbands_macd":
        
        return factor_bbands_macd(df, **kwargs) 

    else:
        print(repr(factor))
        raise ValueError(f"Unknown factor: {factor}")
    
