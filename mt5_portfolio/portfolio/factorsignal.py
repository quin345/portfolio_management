import pandas as pd
import matplotlib.pyplot as plt
from talib import abstract
import numpy as np




# ---------------------------------------------------------
# 2. SIGNAL GENERATORS
# ---------------------------------------------------------


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
    vol_quantile=0.50,
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
# 5. BUY/SELL PRESSURE IMBALANCE (MFI + AD)
# ---------------------------------------------------------
def factor_pressure_imbalance(
    df,
    mfi_period=14,
    ad_slope_window=1,
    mfi_upper=60,
    mfi_lower=40
):
    """
    Models buying/selling pressure imbalance using:
        - MFI (Money Flow Index)
        - AD (Accumulation/Distribution)
        - AD slope (momentum of accumulation/distribution)

    Output:
        +1 → buying pressure dominates
        -1 → selling pressure dominates
         0 → neutral
    """

    ohlcv = df_to_ohlcv(df)

    # --- Money Flow Index (0–100) ---
    mfi = pd.Series(
        abstract.MFI(ohlcv, timeperiod=mfi_period),
        index=df.index
    )

    # --- Accumulation/Distribution Line ---
    ad = pd.Series(
        abstract.AD(ohlcv),
        index=df.index
    )

    # --- AD slope (change in accumulation/distribution) ---
    ad_slope = ad.diff(ad_slope_window)

    # --- Initialize signal ---
    signal = pd.Series(0, index=df.index)

    # --- Buying pressure ---
    signal[(mfi > mfi_upper) & (ad_slope > 0)] = -1

    # --- Selling pressure ---
    signal[(mfi < mfi_lower) & (ad_slope < 0)] = 1

    return signal

# ---------------------------------------------------------
# 6. RSI PRESSURE REVERSAL
# ---------------------------------------------------------
def factor_rsi_pressure(df, period=14, upper=70, lower=30):
    """
    Models pressure imbalance using RSI extremes.
    +1 → oversold reversal (buying pressure expected)
    -1 → overbought reversal (selling pressure expected)
     0 → neutral
    """
    ohlcv = df_to_ohlcv(df)

    rsi = pd.Series(
        abstract.RSI(ohlcv, timeperiod=period),
        index=df.index
    )

    signal = pd.Series(0, index=df.index)
    signal[rsi < lower] = 1
    signal[rsi > upper] = -1

    return signal

# ---------------------------------------------------------
# 7. STOCHASTIC MOMENTUM IMBALANCE
# ---------------------------------------------------------
def factor_stoch_pressure(df, k_period=14, d_period=3, upper=80, lower=20):
    """
    Uses Stochastic Oscillator to detect momentum imbalance.
    +1 → bullish momentum
    -1 → bearish momentum
     0 → neutral
    """
    ohlcv = df_to_ohlcv(df)

    slowk, slowd = abstract.STOCH(
        ohlcv,
        fastk_period=k_period,
        slowk_period=d_period,
        slowd_period=d_period
    )

    slowk = pd.Series(slowk, index=df.index)
    slowd = pd.Series(slowd, index=df.index)

    signal = pd.Series(0, index=df.index)
    signal[(slowk > slowd) & (slowk < lower)] = 1
    signal[(slowk < slowd) & (slowk > upper)] = -1

    return signal

# ---------------------------------------------------------
# 8. OBV PRESSURE TREND
# ---------------------------------------------------------
def factor_obv_pressure(df, slope_window=5):
    """
    OBV slope models volume-driven pressure.
    +1 → OBV rising (buying pressure)
    -1 → OBV falling (selling pressure)
     0 → flat
    """
    ohlcv = df_to_ohlcv(df)

    obv = pd.Series(
        abstract.OBV(ohlcv),
        index=df.index
    )

    slope = obv.diff(slope_window)

    signal = pd.Series(0, index=df.index)
    signal[slope > 0] = -1
    signal[slope < 0] = 1

    return signal

# ---------------------------------------------------------
# 9. CANDLE BODY PRESSURE (BODY vs WICK)
# ---------------------------------------------------------
def factor_candle_pressure(df, body_ratio=0.55):
    """
    Measures candle body dominance.
    +1 → strong bullish candle (buyers dominate)
    -1 → strong bearish candle (sellers dominate)
     0 → weak / indecisive candle
    """
    body = (df["close"] - df["open"]).abs()
    range_ = df["high"] - df["low"]

    strength = body / range_.replace(0, np.nan)

    signal = pd.Series(0, index=df.index)
    signal[(df["close"] > df["open"]) & (strength > body_ratio)] = -1
    signal[(df["close"] < df["open"]) & (strength > body_ratio)] = 1

    return signal



# ---------------------------------------------------------
# KELTNER CHANNEL TREND-PULLBACK
# ---------------------------------------------------------
def factor_keltner(df, period=10, atr_mult=1.5):
    ohlcv = df_to_ohlcv(df)

    ema = pd.Series(abstract.EMA(ohlcv, timeperiod=period), index=df.index)
    atr = pd.Series(abstract.ATR(ohlcv, timeperiod=period), index=df.index)

    upper = ema + atr_mult * atr
    lower = ema - atr_mult * atr

    signal = pd.Series(0, index=df.index)

    # Uptrend pullback: price near EMA, above lower band
    signal[(df["close"] > ema) & (df["close"] < upper)] = -1

    # Downtrend pullback: price near EMA, below upper band
    signal[(df["close"] < ema) & (df["close"] > lower)] = 1

    return signal

# ---------------------------------------------------------
# ADX TREND STRENGTH FILTER
# ---------------------------------------------------------
def factor_adx_trend(df, adx_period=14, adx_thresh=25):
    ohlcv = df_to_ohlcv(df)

    adx = pd.Series(abstract.ADX(ohlcv, timeperiod=adx_period), index=df.index)
    plus_di = pd.Series(abstract.PLUS_DI(ohlcv, timeperiod=adx_period), index=df.index)
    minus_di = pd.Series(abstract.MINUS_DI(ohlcv, timeperiod=adx_period), index=df.index)

    signal = pd.Series(0, index=df.index)
    signal[(adx > adx_thresh) & (plus_di > minus_di)] = 1
    signal[(adx > adx_thresh) & (minus_di > plus_di)] = -1

    return signal

# ---------------------------------------------------------
# CCI DIVERGENCE REVERSAL
# ---------------------------------------------------------
def factor_cci_reversal(df, period=20, upper=100, lower=-100):
    ohlcv = df_to_ohlcv(df)

    cci = pd.Series(abstract.CCI(ohlcv, timeperiod=period), index=df.index)

    signal = pd.Series(0, index=df.index)
    signal[(cci < lower) & (cci.shift(1) < cci)] = 1
    signal[(cci > upper) & (cci.shift(1) > cci)] = -1

    return signal

# ---------------------------------------------------------
# ATR VOLATILITY BREAKOUT
# ---------------------------------------------------------
def factor_atr_breakout(df, period=14, k=1.5):
    ohlcv = df_to_ohlcv(df)

    atr = pd.Series(abstract.ATR(ohlcv, timeperiod=period), index=df.index)

    upper = df["close"].shift(1) + k * atr
    lower = df["close"].shift(1) - k * atr

    signal = pd.Series(0, index=df.index)
    signal[df["close"] > upper] = 1
    signal[df["close"] < lower] = -1

    return signal

# ---------------------------------------------------------
# DONCHIAN CHANNEL BREAKOUT
# ---------------------------------------------------------
def factor_donchian(df, period=20):
    high_roll = df["high"].rolling(period).max()
    low_roll = df["low"].rolling(period).min()

    signal = pd.Series(0, index=df.index)
    signal[df["close"] > high_roll.shift(1)] = 1
    signal[df["close"] < low_roll.shift(1)] = -1

    return signal

# ---------------------------------------------------------
# CHAIKIN MONEY FLOW (CMF) PRESSURE
# ---------------------------------------------------------
def factor_cmf_pressure(df, period=20, upper=0.1, lower=-0.1):
    ohlcv = df_to_ohlcv(df)

    cmf = pd.Series(abstract.ADOSC(ohlcv, fastperiod=3, slowperiod=10), index=df.index)
    cmf = cmf.rolling(period).mean()

    signal = pd.Series(0, index=df.index)
    signal[cmf > upper] = 1
    signal[cmf < lower] = -1

    return signal

# ---------------------------------------------------------
# VOLATILITY-ADJUSTED RSI
# ---------------------------------------------------------
def factor_rsi_vol(df, rsi_period=14, vol_window=50, upper=70, lower=30, vol_quantile=0.7):
    ohlcv = df_to_ohlcv(df)

    rsi = pd.Series(abstract.RSI(ohlcv, timeperiod=rsi_period), index=df.index)
    vol = df["close"].pct_change().rolling(vol_window).std()
    vol_thresh = vol.quantile(vol_quantile)

    signal = pd.Series(0, index=df.index)
    # Only trade when volatility is below threshold
    cond_low_vol = vol < vol_thresh

    signal[(rsi < lower) & cond_low_vol] = 1
    signal[(rsi > upper) & cond_low_vol] = -1

    return signal

# ---------------------------------------------------------
# VOLUME SPIKE EXHAUSTION
# ---------------------------------------------------------
def factor_volume_spike(df, window=50, quantile=0.95):
    vol = df["volume"]
    vol_thresh = vol.rolling(window).quantile(quantile)

    signal = pd.Series(0, index=df.index)
    signal[(vol > vol_thresh) & (df["close"] < df["open"])] = 1
    signal[(vol > vol_thresh) & (df["close"] > df["open"])] = -1

    return signal

# ---------------------------------------------------------
# VWAP REVERSION (INTRADAY)
# ---------------------------------------------------------
def factor_vwap_reversion(df, dist_mult=1.5):
    # assumes df index is intraday and continuous per day
    pv = df["close"] * df["volume"]
    cum_pv = pv.cumsum()
    cum_vol = df["volume"].cumsum()
    vwap = cum_pv / cum_vol.replace(0, np.nan)

    dist = (df["close"] - vwap) / vwap

    signal = pd.Series(0, index=df.index)
    signal[dist < -dist_mult * dist.rolling(100).std()] = 1
    signal[dist > dist_mult * dist.rolling(100).std()] = -1

    return signal

# ---------------------------------------------------------
# RANGE COMPRESSION (SQUEEZE) BREAKOUT
# ---------------------------------------------------------
def factor_squeeze_breakout(df, bb_period=20, bb_std=2.0, width_window=100):
    ohlcv = df_to_ohlcv(df)

    upper, middle, lower = abstract.BBANDS(
        ohlcv,
        timeperiod=bb_period,
        nbdevup=float(bb_std),
        nbdevdn=float(bb_std),
        matype=0
    )

    upper = pd.Series(upper, index=df.index)
    lower = pd.Series(lower, index=df.index)

    width = (upper - lower) / middle
    min_width = width.rolling(width_window).min()

    squeeze = width <= min_width

    signal = pd.Series(0, index=df.index)
    signal[(squeeze) & (df["close"] > upper)] = 1
    signal[(squeeze) & (df["close"] < lower)] = -1

    return signal

# ---------------------------------------------------------
# HEIKIN-ASHI TREND FILTER
# ---------------------------------------------------------
def factor_heikin_ashi(df):
    ha_close = (df["open"] + df["high"] + df["low"] + df["close"]) / 4

    ha_open = ha_close.copy()
    ha_open.iloc[0] = (df["open"].iloc[0] + df["close"].iloc[0]) / 2
    for i in range(1, len(df)):
        ha_open.iloc[i] = (ha_open.iloc[i-1] + ha_close.iloc[i-1]) / 2

    signal = pd.Series(0, index=df.index)
    signal[ha_close > ha_open] = 1
    signal[ha_close < ha_open] = -1

    return signal

# ---------------------------------------------------------
# Z-SCORE OF RETURNS
# ---------------------------------------------------------
def factor_zscore_returns(df, window=50, z_entry=2.0):
    ret = df["close"].pct_change()
    mean = ret.rolling(window).mean()
    std = ret.rolling(window).std()
    z = (ret - mean) / std.replace(0, np.nan)

    signal = pd.Series(0, index=df.index)
    signal[z < -z_entry] = 1
    signal[z > z_entry] = -1

    return signal

# ---------------------------------------------------------
# ROLLING SKEWNESS REVERSAL
# ---------------------------------------------------------
def factor_skew_reversal(df, window=50, z_entry=1.5):
    ret = df["close"].pct_change()
    skew = ret.rolling(window).skew()

    signal = pd.Series(0, index=df.index)
    signal[(skew < -z_entry) & (ret < 0)] = 1
    signal[(skew > z_entry) & (ret > 0)] = -1

    return signal

# ---------------------------------------------------------
# RSI + MACD HYBRID
# ---------------------------------------------------------
def factor_rsi_macd_hybrid(df, rsi_period=14, upper=70, lower=30,
                           macd_fast=12, macd_slow=26, macd_signal=9):
    ohlcv = df_to_ohlcv(df)

    rsi = pd.Series(abstract.RSI(ohlcv, timeperiod=rsi_period), index=df.index)

    macd, macd_sig, macd_hist = abstract.MACD(
        ohlcv,
        fastperiod=macd_fast,
        slowperiod=macd_slow,
        signalperiod=macd_signal
    )
    macd_hist = pd.Series(macd_hist, index=df.index)

    signal = pd.Series(0, index=df.index)
    signal[(rsi < lower) & (macd_hist > macd_hist.shift(1))] = 1
    signal[(rsi > upper) & (macd_hist < macd_hist.shift(1))] = -1

    return signal

# ---------------------------------------------------------
# PRICE-VOLUME TREND (PVT) MOMENTUM
# ---------------------------------------------------------
def factor_pvt_momentum(df, slope_window=10):
    ret = df["close"].pct_change().fillna(0)
    pvt = (ret * df["volume"]).cumsum()

    slope = pvt.diff(slope_window)

    signal = pd.Series(0, index=df.index)
    signal[slope > 0] = 1
    signal[slope < 0] = -1

    return signal



# ---------------------------------------------------------
# MAIN INTERFACE
# ---------------------------------------------------------
def generate_signal(df, factor="ma", **kwargs):

    factor = factor.strip().lower() 

    if factor == "ma":
        return factor_ma_cross(df, **kwargs)
    elif factor == "bbands":
        return factor_bbands(df, **kwargs)
    elif factor == "macd":
        return factor_macd(df, **kwargs)
    elif factor == "bbands_macd":
        return factor_bbands_macd(df, **kwargs)    
    elif factor == "pressure":
        return factor_pressure_imbalance(df, **kwargs)    
    elif factor == "rsi_pressure":
        return factor_rsi_pressure(df, **kwargs)
    elif factor == "stoch_pressure":
        return factor_stoch_pressure(df, **kwargs)
    elif factor == "obv_pressure":
        return factor_obv_pressure(df, **kwargs)
    elif factor == "candle_pressure":
        return factor_candle_pressure(df, **kwargs)    
    elif factor == "keltner":
        return factor_keltner(df, **kwargs)
    elif factor == "adx_trend":
        return factor_adx_trend(df, **kwargs)
    elif factor == "cci_reversal":
        return factor_cci_reversal(df, **kwargs)
    elif factor == "atr_breakout":
        return factor_atr_breakout(df, **kwargs)
    elif factor == "donchian":
        return factor_donchian(df, **kwargs)
    elif factor == "cmf_pressure":
        return factor_cmf_pressure(df, **kwargs)
    elif factor == "rsi_vol":
        return factor_rsi_vol(df, **kwargs)
    elif factor == "volume_spike":
        return factor_volume_spike(df, **kwargs)
    elif factor == "vwap_reversion":
        return factor_vwap_reversion(df, **kwargs)
    elif factor == "squeeze_breakout":
        return factor_squeeze_breakout(df, **kwargs)
    elif factor == "heikin_ashi":
        return factor_heikin_ashi(df, **kwargs)
    elif factor == "zscore_returns":
        return factor_zscore_returns(df, **kwargs)
    elif factor == "skew_reversal":
        return factor_skew_reversal(df, **kwargs)
    elif factor == "rsi_macd_hybrid":
        return factor_rsi_macd_hybrid(df, **kwargs)
    elif factor == "pvt_momentum":
        return factor_pvt_momentum(df, **kwargs)
    else:
        raise ValueError(f"Unknown factor: {factor}")
    


    
