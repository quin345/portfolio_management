from MLmodel import generate_features
import xgboost as xgb
import MetaTrader5 as mt5
from data import MT5DatabaseSaver
import numpy as np

acg_data = MT5DatabaseSaver("acg")
symbol = "USOIL.pro"
df = acg_data.load_ohlcv(
        symbol=str(symbol),
        timeframe="15",
        limit=100000
)
mid = len(df) // 2

# Split into two halves
df_first_half = df.iloc[:mid]
df_second_half = df.iloc[mid:]

acg_data.update_db(symbol=str(symbol), timeframe=mt5.TIMEFRAME_M15, num_bars=300)
#df_live = acg_data.load_ohlcv(symbol="EURUSD", timeframe="30", limit=100000)

model = xgb.XGBClassifier()
model.load_model("xgb_multiclass.json")


features = generate_features(df_second_half)

features = features.dropna()

X_live = features.values

prediction = model.predict(X_live)

# MAP BACK TO TRADING LABELS
inv_map = {0: -1, 1: 0, 2: 1}
y_pred = np.vectorize(inv_map.get)(prediction)

print(y_pred)
print(df_second_half.head(1))
print(df_second_half.tail(1))
# --- IGNORE ---

import numpy as np

values, counts = np.unique(y_pred, return_counts=True)
print(values)
print(counts)

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# --- 1. Compute raw returns based on your signal ---
signal = pd.Series(y_pred, index=df_second_half.index)

# price returns
price_ret = df_second_half['close'].pct_change()

# strategy returns BEFORE cost
ret = price_ret * -signal.shift(1)   # shift to avoid lookahead bias
ret = ret.fillna(0)

# --- 2. Compute transaction costs ---
# cost occurs when signal changes (position flip)
transaction_cost = 0.0002   # example: 0.1%

# position change indicator
pos_change = signal.diff().abs()
print(pos_change)

# cost per trade
cost = pos_change * transaction_cost

# --- 3. Apply cost to returns ---
ret_after_cost = ret - cost

# --- 4. Compute cumulative returns ---
cum_ret = (1 + ret_after_cost).cumprod()

# --- 5. Plot ---
plt.figure(figsize=(12,6))
plt.plot(cum_ret, label='Strategy (after cost)', color='blue')
plt.axhline(1, color='gray', linestyle='--', linewidth=1)
plt.title('Cumulative Backtest With Transaction Costs')
plt.xlabel('Date')
plt.ylabel('Cumulative Return')
plt.legend()
plt.grid(True)
plt.show()