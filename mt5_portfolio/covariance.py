# covariance.py
import numpy as np
import pandas as pd


# -----------------------------------------
# Simple sample covariance
# -----------------------------------------
def simple_cov(returns: pd.DataFrame):
    """
    Standard sample covariance matrix.
    """
    return returns.cov()


# -----------------------------------------
# EWMA covariance (RiskMetrics)
# -----------------------------------------
def ewma_cov(returns: pd.DataFrame, lambda_=0.94):
    """
    Exponentially Weighted Moving Average covariance.
    lambda_ = decay factor (0.94 for daily, 0.97 for weekly)
    """
    # Ensure returns are aligned
    returns = returns.dropna()

    # Convert to numpy
    X = returns.values
    n, k = X.shape

    # Initialize covariance with sample cov
    cov = np.cov(X, rowvar=False)

    # Apply EWMA
    for t in range(1, n):
        x = X[t].reshape(-1, 1)
        cov = lambda_ * cov + (1 - lambda_) * (x @ x.T)

    return pd.DataFrame(cov, index=returns.columns, columns=returns.columns)


# -----------------------------------------
# Dispatcher (optional)
# -----------------------------------------
def get_covariance(returns: pd.DataFrame, method="simple", **kwargs):
    """
    method: "simple" or "ewma"
    kwargs: passed to ewma_cov (e.g., lambda_=0.94)
    """
    method = method.lower()

    if method == "simple":
        return simple_cov(returns)

    if method == "ewma":
        return ewma_cov(returns, **kwargs)

    raise ValueError(f"Unknown covariance method: {method}")