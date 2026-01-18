# strategy.py

import pandas as pd
import MetaTrader5 as mt5

from brokers import get_broker
from mapper import build_fx_map
from dataloader import (
    load_symbols_from_csv,
    load_log_returns,
    save_metadata,
    load_contract_dataframe
)
from covariance import get_covariance
from expectedreturns import compute_expected_returns
from optimizer import optimize_portfolio
from risk import risk_analysis
from lotsizing import run_lot_sizing


# ============================================================
# MAIN STRATEGY PIPELINE
# ============================================================
def run_strategy(
    broker_name: str,
    macro_signal_csv: str,
    vol_target: float,
    method: str,
    ewma_lambda: float,
    ic: float,
    lookback_days: int = 60
):
    """
    Full portfolio construction + lot sizing pipeline.

    Parameters
    ----------
    broker_name : str
        Name of broker (e.g., "icmarkets")
    active_symbols_csv : str
        CSV file containing list of tradable symbols
    factor_signal_csv : str
        CSV file containing factor signals
    vol_target : float
        Annual volatility target (e.g., 0.10)
    ewma_lambda : float
        EWMA decay factor for covariance
    ic : float
        Information coefficient for expected returns
    lookback_days : int
        Lookback window for returns

    Returns
    -------
    df : DataFrame
        Final lot sizing table
    gross_target : float
        Total absolute target lots
    gross_current : float
        Total absolute current lots
    """

    # --------------------------------------------------------
    # 1. Initialize broker + MT5
    # --------------------------------------------------------
    broker = get_broker(broker_name)
    broker.initialize()

    # --------------------------------------------------------
    # 2. Load symbols
    # --------------------------------------------------------
    symbols = load_symbols_from_csv(macro_signal_csv)

    # --------------------------------------------------------
    # 3. FX mapping
    # --------------------------------------------------------
    fx_map, fx_exempt = build_fx_map(
        source="csv",
        csv_path=macro_signal_csv
    )

    # --------------------------------------------------------
    # 4. Load returns (with DB caching)
    # --------------------------------------------------------
    returns = load_log_returns(
        symbols=symbols,
        fx_map=fx_map,
        broker_name=broker.name,
        lookback_days=lookback_days
    )

    # --------------------------------------------------------
    # 5. Save metadata (contract size, min volume)
    # --------------------------------------------------------
    save_metadata(symbols=symbols, broker_name=broker.name)

    # --------------------------------------------------------
    # 6. Covariance matrix
    # --------------------------------------------------------
    cov = get_covariance(returns, method=method, lambda_=ewma_lambda)

    # --------------------------------------------------------
    # 7. Expected returns
    # --------------------------------------------------------
    expected_returns = compute_expected_returns(
        signals=macro_signal_csv,
        returns=returns,
        ic=ic,
        vol_target=vol_target,
        vol_window=lookback_days
    )

    # --------------------------------------------------------
    # 8. Optimize portfolio (Max Sharpe)
    # --------------------------------------------------------
    weights, daily_ret, daily_vol = optimize_portfolio(
        expected_returns,
        cov,
        dollar_neutral=True
    )

    # --------------------------------------------------------
    # 9. Volatility targeting
    # --------------------------------------------------------
    result = risk_analysis(weights, returns)
    scaled_weights = result["scaled_weights"]

    scaled_df = pd.DataFrame({
        "asset": returns.columns,
        "scaled_weight": scaled_weights
    })

    # --------------------------------------------------------
    # 10. Load contract metadata + merge with weights
    # --------------------------------------------------------
    df = load_contract_dataframe(
        broker_name=broker.name,
        scaled_df=scaled_df
    )

    # --------------------------------------------------------
    # 11. Lot sizing
    # --------------------------------------------------------
    df, gross_target, gross_current = run_lot_sizing(
        df=df,
        fx_exempt=fx_exempt,
        index_fx_map=fx_map
    )

    # --------------------------------------------------------
    # 12. Shutdown MT5
    # --------------------------------------------------------
    mt5.shutdown()

    return df, gross_target, gross_current