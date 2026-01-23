import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import MetaTrader5 as mt5
from factorsignal import generate_signal

def fetch_mt5_data(symbol="NZDUSD.pro", num_candles=99999, timeframe=mt5.TIMEFRAME_M5):
    """
    Fetch OHLCV data from MetaTrader 5 and return a clean DataFrame
    with basic stats (max high, average spread).
    """

    # Initialize MT5
    if not mt5.initialize():
        raise RuntimeError("MT5 initialization failed")

    # Ensure symbol is available
    mt5.symbol_select(symbol, True)

    # Fetch candles
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, num_candles)

    # Shutdown MT5
    mt5.shutdown()

    # Convert to DataFrame
    df = pd.DataFrame(rates)
    df = df.rename(columns={"tick_volume": "volume"})
    df["time"] = pd.to_datetime(df["time"], unit="s")

    # Compute stats
    max_high = df["high"].max()
    avg_spread = df["spread"].mean()

    
    return df, max_high, avg_spread

def backtest_symbols(symbols, factor="ma", num_candles=99999, timeframe=mt5.TIMEFRAME_M5):
    """
    Returns:
        - performance_df: summary table
        - returns_df: factor premia return series for all symbols
        - ohlcv_dict: {symbol: OHLCV DataFrame}
    """

    plt.figure(figsize=(14,6))
    results = []
    all_returns = {}
    ohlcv_dict = {}   # <-- store OHLCV DataFrame for each symbol

    for symbol in symbols:

        # 1. FETCH DATA
        df = fetch_mt5_data(
            symbol=symbol,
            num_candles=num_candles,
            timeframe=timeframe
        )[0]

        # store raw OHLCV before modifying
        ohlcv_dict[symbol] = df.copy()

        # 2. CLEAN & PREPARE
        df = df.rename(columns={"tick_volume": "volume"})
        df["time"] = pd.to_datetime(df["time"], unit="s")
        df = df.set_index("time")
        df["ret"] = df["close"].pct_change()

        # 3. GENERATE SIGNAL
        signal = generate_signal(df, factor=factor)

        # 4. FACTOR PREMIA
        factor_premia = df["ret"] * signal.shift(1)

        # store return series
        all_returns[symbol] = factor_premia

        # 5. CUMULATIVE RETURNS
        cum_factor = (1 + factor_premia.fillna(0)).cumprod()

        # 6. PERFORMANCE METRICS
        sr = factor_premia.mean() / factor_premia.std()
        annualized_sr = sr * np.sqrt(252)
        annualized_return = factor_premia.mean() * 252

        # 7. COLLECT RESULTS
        results.append({
            "Symbol": symbol,
            "Sharpe": annualized_sr,
            "Annualized Return": annualized_return
        })

        # 8. PLOT
        plt.plot(
            cum_factor,
            label=f"{symbol} | SR={annualized_sr:0.2f} | Ret={annualized_return:0.2%}"
        )

    # FINAL GRAPH SETTINGS
    plt.title(f"Multi-Symbol Factor Backtest – {factor.upper()} Strategy")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

    # Summary tables
    performance_df = pd.DataFrame(results)
    returns_df = pd.DataFrame(all_returns)

    return performance_df, returns_df, ohlcv_dict