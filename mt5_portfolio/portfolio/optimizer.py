import numpy as np
from scipy.optimize import minimize

def optimize_portfolio(expected_returns,
                       cov_matrix,
                       risk_free_rate=0.0,
                       dollar_neutral=False,
                       long_only=False,
                       bounds=None):

    num_assets = len(expected_returns)

    # Objective: negative Sharpe
    def neg_sharpe(weights):
        port_ret = np.dot(weights, expected_returns)
        port_vol = np.sqrt(weights.T @ cov_matrix @ weights)
        if port_vol <= 0:
            return 1e6
        return -(port_ret - risk_free_rate) / port_vol

    # Bounds
    if bounds is None:
        if long_only:
            bounds_list = [(0, 1)] * num_assets
        else:
            bounds_list = [(-1, 1)] * num_assets
    else:
        bounds_list = [bounds] * num_assets

    # Constraints
    constraints = []

    # Dollar neutrality
    if dollar_neutral:
            constraints = [
                {"type": "eq", "fun": lambda w: np.sum(w)},  # dollar neutral
                {"type": "ineq", "fun": lambda w: 1.0 - np.sum(w**2)}  # leverage
            ]
    else:
        constraints = [
            {"type": "ineq", "fun": lambda w: 1.0 - np.sum(w**2)}  # L2 leverage only
        ]

    # Replace L2 constraint with proper gross leverage constraint
    # Example: sum(|w|) <= 1
    #constraints.append({"type": "ineq", "fun": lambda w: 1.0 - np.sum(np.abs(w))})

    # Neutral initial guess
    init_guess = np.zeros(num_assets)
    init_guess[0] = 0.5
    init_guess[1] = -0.5

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