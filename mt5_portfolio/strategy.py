import MetaTrader5 as mt5
import os
from brokers import get_broker
from dotenv import load_dotenv
from mapper import build_fx_map
from dataloader import load_log_returns, load_symbols_from_csv, save_metadata
from covariance import get_covariance
from expectedreturns import compute_expected_returns
from optimizer import optimize_portfolio
import pandas as pd


load_dotenv()

# Choose your broker
broker = get_broker("icmarkets")
broker.initialize()

csv_path = "active_symbols.csv"

# define tradable symbols

symbols = load_symbols_from_csv(csv_path)
# -----------------------------
# Map non USD to their corresponding FX pairs
# -----------------------------

fx_map = build_fx_map(source="csv", csv_path=csv_path) 

returns = load_log_returns(
    symbols=symbols, 
    fx_map=fx_map, 
    broker_name=broker.name, 
    lookback_days=60
)

save_metadata(symbols=symbols, broker_name=broker.name)


cov = get_covariance(returns, method="simple")



# signals = dict of raw factor values
# returns = your returns matrix from dataloader
# ic = your assumed IC (or dict per asset)

expected_returns = compute_expected_returns(
    signals="ai_factor_signal_icm.csv",
    returns=returns,
    ic=0.05,
    vol_target=0.10,
    vol_window=60
)



weights, daily_ret, daily_vol = optimize_portfolio(
    expected_returns,
    cov,
    dollar_neutral=True
)

print(weights)
print("Expected daily return:", daily_ret)
print("Expected daily volatility:", daily_vol)

from risk import risk_analysis

# weights = ndarray from optimizer
# returns = DataFrame of log returns

results = risk_analysis(
    weights=weights,
    returns=returns,
    target_annual_vol=0.05
)

print(f"\nScale factor: {results['scale_factor']:.4f}")
print(f"Scaled annual vol: {results['scaled']['annual_vol']:.2%}")
print(f"Scaled annual return: {results['scaled']['annual_return']:.2%}")
print(f"Scaled Sharpe: {results['scaled']['sharpe']:.2f}")

scaled_weights = results["scaled_weights"]

pd.Series(
    scaled_weights,
    index=returns.columns
).to_csv("icm_scaled_weights.csv", header=False)




mt5.shutdown()