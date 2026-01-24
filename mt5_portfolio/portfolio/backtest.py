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

    plt.figure(figsize=(14,6))
    results = []
    all_returns = {}
    ohlcv_dict = {}

    for symbol in symbols:

        # 1. FETCH DATA
        df = fetch_mt5_data(
            symbol=symbol,
            num_candles=num_candles,
            timeframe=timeframe
        )[0]

        ohlcv_dict[symbol] = df.copy()

        # 2. CLEAN
        df = df.rename(columns={"tick_volume": "volume"})
        df["time"] = pd.to_datetime(df["time"], unit="s")
        df = df.set_index("time")
        df["ret"] = df["close"].pct_change()

        # 3. SIGNAL
        signal = generate_signal(df, factor=factor)

        # 4. FACTOR PREMIA
        factor_premia = df["ret"] * signal.shift(1)
        all_returns[symbol] = factor_premia

        # 5. CUMULATIVE RETURNS
        cum_factor = (1 + factor_premia.fillna(0)).cumprod()

        # 6. METRICS
        sr = factor_premia.mean() / factor_premia.std()
        annualized_sr = sr * np.sqrt(252)
        annualized_return = factor_premia.mean() * 252

        results.append({
            "Symbol": symbol,
            "Sharpe": annualized_sr,
            "Annualized Return": annualized_return
        })

        # 7. PLOT INDIVIDUAL
        plt.plot(
            cum_factor,
            label=f"{symbol} | SR={annualized_sr:0.2f} | Ret={annualized_return:0.2%}"
        )

    # -----------------------------
    # 📌 PORTFOLIO CALCULATIONS
    # -----------------------------
    returns_df = pd.DataFrame(all_returns).fillna(0)

    # Equal-weight portfolio
    portfolio_ret = returns_df.mean(axis=1)

    portfolio_cum = (1 + portfolio_ret).cumprod()

    # Portfolio metrics
    port_sr = portfolio_ret.mean() / portfolio_ret.std()
    port_annualized_sr = port_sr * np.sqrt(252)
    port_annualized_return = portfolio_ret.mean() * 252

    # Plot portfolio line
    plt.plot(
        portfolio_cum,
        linewidth=3,
        color="black",
        label=f"PORTFOLIO | SR={port_annualized_sr:0.2f} | Ret={port_annualized_return:0.2%}"
    )

    # -----------------------------
    # 📌 ADD TEXT BOX WITH PORTFOLIO METRICS
    # -----------------------------
    textstr = (
        f"Portfolio Sharpe: {port_annualized_sr:0.2f}\n"
        f"Portfolio Annualized Return: {port_annualized_return:0.2%}"
    )

    plt.gcf().text(
        0.85, 0.75, textstr,
        fontsize=11,
        bbox=dict(facecolor='white', alpha=0.7)
    )

    # FINAL GRAPH SETTINGS
    plt.title(f"Multi-Symbol Factor Backtest – {factor.upper()} Strategy")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.show()

    performance_df = pd.DataFrame(results)

    return performance_df, returns_df