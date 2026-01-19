# risk.py
import numpy as np
import pandas as pd


# ---------------------------------------------------------
# EWMA covariance (clean version of your logic)
# ---------------------------------------------------------
def ewma_covariance(returns, decay=0.94):
    """
    returns: DataFrame of log returns
    decay: EWMA lambda
    """
    r = returns.values
    cov = returns.cov().values.copy()

    for t in range(1, len(r)):
        x = r[t].reshape(-1, 1)
        cov = decay * cov + (1 - decay) * (x @ x.T)

    return cov


# ---------------------------------------------------------
# Convert log returns → simple returns
# ---------------------------------------------------------
def log_to_simple(log_returns):
    return np.exp(log_returns) - 1.0


# ---------------------------------------------------------
# Ex-post performance using simple returns
# ---------------------------------------------------------
def ex_post_stats(weights, log_returns):
    """
    weights: ndarray
    log_returns: DataFrame of log returns
    """
    simple = log_to_simple(log_returns)
    port_series = simple.values @ weights

    daily_ret = port_series.mean()
    daily_vol = port_series.std()

    annual_ret = daily_ret * 252
    annual_vol = daily_vol * np.sqrt(252)

    sharpe = annual_ret / annual_vol if annual_vol > 0 else np.nan

    return {
        "daily_return": daily_ret,
        "annual_return": annual_ret,
        "daily_vol": daily_vol,
        "annual_vol": annual_vol,
        "sharpe": sharpe,
    }


# ---------------------------------------------------------
# Scale weights to target annual volatility
# ---------------------------------------------------------
def scale_to_target_vol(weights, cov, target_annual_vol):
    target_daily = target_annual_vol / np.sqrt(252)

    current_daily_vol = np.sqrt(weights.T @ cov @ weights)
    if current_daily_vol <= 0:
        raise ValueError("Portfolio volatility is zero or invalid.")

    scale = target_daily / current_daily_vol
    scaled_weights = weights * scale

    scaled_daily_vol = np.sqrt(scaled_weights.T @ cov @ scaled_weights)
    scaled_annual_vol = scaled_daily_vol * np.sqrt(252)

    return scaled_weights, scale, scaled_daily_vol, scaled_annual_vol


# ---------------------------------------------------------
# Main risk function (clean wrapper)
# ---------------------------------------------------------
def risk_analysis(weights, returns, target_annual_vol=0.05, decay=0.94):
    """
    weights: ndarray (already aligned to returns.columns)
    returns: DataFrame of log returns
    """

    # 1. EWMA covariance
    cov = ewma_covariance(returns, decay=decay)

    # 2. Ex-post stats BEFORE scaling
    base = ex_post_stats(weights, returns)
 
        
    # 3. Scale to target vol
    scaled_weights, scale_factor, scaled_daily_vol, scaled_annual_vol = \
        scale_to_target_vol(weights, cov, target_annual_vol)

    # 4. Ex-post stats AFTER scaling
    scaled = ex_post_stats(scaled_weights, returns)

    return {
        "cov": cov,
        "base": base,
        "scaled_weights": scaled_weights,
        "scale_factor": scale_factor,
        "scaled": scaled,
    }