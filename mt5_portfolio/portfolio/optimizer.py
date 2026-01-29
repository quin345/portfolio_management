import numpy as np
from scipy.optimize import minimize

def optimize_portfolio(expected_returns,
                       cov_matrix,
                       risk_free_rate=0.0,
                       max_weight=0.10):
    """
    Long-only, diversified Sharpe ratio optimizer.
    Ensures:
    - weights >= 0
    - weights <= max_weight
    - sum(weights) = 1
    """

    num_assets = len(expected_returns)

    # Objective: maximize Sharpe (minimize negative Sharpe)
    def neg_sharpe(weights):
        port_ret = np.dot(weights, expected_returns)
        port_vol = np.sqrt(weights.T @ cov_matrix @ weights)
        if port_vol <= 0:
            return 1e6
        return -(port_ret - risk_free_rate) / port_vol

    # Bounds: long-only + max weight
    bounds_list = [(0, max_weight)] * num_assets

    # Constraint: fully invested (sum of weights = 1)
    constraints = [
        {"type": "eq", "fun": lambda w: np.sum(w) - 1}
    ]

    # Initial guess: equal weight
    init_guess = np.ones(num_assets) / num_assets

    # Optimize
    result = minimize(
        neg_sharpe,
        init_guess,
        bounds=bounds_list,
        constraints=constraints,
        method="SLSQP",
    )

    if not result.success:
        print("Warning:", result.message)

    weights = result.x
    daily_ret = np.dot(weights, expected_returns)
    daily_vol = np.sqrt(weights.T @ cov_matrix @ weights)

    return weights, daily_ret, daily_vol


