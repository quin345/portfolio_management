import numpy as np
from scipy.optimize import minimize

# -----------------------------
# Max Sharpe optimization (YOUR version + options)
# -----------------------------
def optimize_portfolio(expected_returns,
                     cov_matrix,
                     risk_free_rate=0.0,
                     dollar_neutral=False,
                     long_only=False,
                     bounds=None):
    num_assets = len(expected_returns)

    # -----------------------------
    # Objective: negative Sharpe
    # -----------------------------
    def neg_sharpe(weights):
        port_ret = np.dot(weights, expected_returns)
        port_vol = np.sqrt(weights.T @ cov_matrix @ weights)
        if port_vol <= 0:
            return 1e6
        return -(port_ret - risk_free_rate) / port_vol

    # -----------------------------
    # Bounds
    # -----------------------------
    if bounds is None:
        if long_only:
            bounds_list = [(0, 1)] * num_assets
        else:
            bounds_list = [(-1, 1)] * num_assets
    elif isinstance(bounds, tuple):
        bounds_list = [bounds] * num_assets
    elif isinstance(bounds, dict):
        bounds_list = [bounds[a] for a in expected_returns.index]
    else:
        raise ValueError("Invalid bounds format")

    # -----------------------------
    # Constraints
    # -----------------------------
    constraints = []

    # Dollar-neutral or fully invested
    if dollar_neutral:
        constraints.append({"type": "eq", "fun": lambda w: np.sum(w)})
    else:
        constraints.append({"type": "eq", "fun": lambda w: np.sum(w) - 1})

    # Your original L2 leverage constraint
    constraints.append({"type": "ineq", "fun": lambda w: 1.0 - np.sum(w**2)})

    # -----------------------------
    # Initial guess (your original)
    # -----------------------------
    init_guess = np.ones(num_assets) / num_assets

    # -----------------------------
    # Optimization
    # -----------------------------
    result = minimize(
        neg_sharpe,
        init_guess,
        bounds=bounds_list,
        constraints=constraints,
        method="SLSQP",
    )

    if not result.success:
        print("Warning: optimization did not fully converge:", result.message)

    weights = result.x
    daily_ret = np.dot(weights, expected_returns)
    daily_vol = np.sqrt(weights.T @ cov_matrix @ weights)

    return weights, daily_ret, daily_vol