import MetaTrader5 as mt5
import os
from brokers import get_broker
from dotenv import load_dotenv
from mapper import build_fx_map
from dataloader import load_returns, load_symbols_from_csv
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

returns = load_returns(symbols=symbols, fx_map=fx_map, broker_name="icmarkets")

mt5.shutdown()