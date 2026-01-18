import MetaTrader5 as mt5
import os
from brokers import get_broker
from dotenv import load_dotenv
load_dotenv()

# Choose your broker
broker = get_broker("icmarkets")
broker.initialize()

csv_path = "active_symbols.csv"

# -----------------------------
# Map non USD to their corresponding FX pairs
# -----------------------------
from mapper import build_fx_map
fx_map, fx_exempt = build_fx_map(source="csv", csv_path=csv_path) #choose between mt5 and csv file


from dataloader import load_log_returns, load_symbols_from_csv, save_metadata, load_contract_dataframe
symbols = load_symbols_from_csv(csv_path)
returns = load_log_returns(
    symbols=symbols, 
    fx_map=fx_map, 
    broker_name=broker.name, 
    lookback_days=60
)
save_metadata(symbols=symbols, broker_name=broker.name)

from covariance import get_covariance
cov = get_covariance(returns, method="ewma")

# signals = dict of raw factor values
# returns = your returns matrix from dataloader
# ic = your assumed IC
from expectedreturns import compute_expected_returns
expected_returns = compute_expected_returns(
    signals="ai_macro_signal_icm.csv",
    returns=returns,
    ic=0.05,
    vol_target=0.10,
    vol_window=60
)

# Optimize Max Sharpe
from optimizer import optimize_portfolio
weights, daily_ret, daily_vol = optimize_portfolio(
    expected_returns,
    cov,
    dollar_neutral=True
)
#volatility target
from risk import risk_analysis
import pandas as pd
# After running risk_analysis()
result = risk_analysis(weights, returns)
scaled_weights = result["scaled_weights"]

# Convert numpy array → DataFrame
scaled_df = pd.DataFrame({
    "asset": returns.columns,
    "scaled_weight": scaled_weights
})

# Load contract metadata + merge with scaled weights
df = load_contract_dataframe(
    broker_name=broker.name,
    scaled_df=scaled_df
)

from lotsizing import run_lot_sizing
# Run lot sizing
df, gross_target, gross_current = run_lot_sizing(
    df=df,
    fx_exempt=fx_exempt,
    index_fx_map=fx_map
)

print(df[["asset", "contract_size", "min_volume", "current_holdings", "target_lot_size"]])

mt5.shutdown()