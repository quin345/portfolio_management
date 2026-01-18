import pandas as pd
import MetaTrader5 as mt5
from dotenv import load_dotenv
import os

# -----------------------------------
# Load credentials & initialize MT5
# -----------------------------------
load_dotenv()
login = int(os.getenv("ICM_MT5_LOGIN"))
password = os.getenv("ICM_MT5_PASSWORD")
server = os.getenv("ICM_MT5_SERVER")

if not mt5.initialize(login=login, password=password, server=server):
    print("MT5 initialization failed:", mt5.last_error())
    quit()

# -----------------------------------
# Load CSV
# -----------------------------------
df = pd.read_csv("icm_lot_sizes_output.csv")

# -----------------------------------
# Order sender (robust + unconstrained)
# -----------------------------------
def send_order(symbol, lot):

    # Direction
    order_type = mt5.ORDER_TYPE_BUY if lot > 0 else mt5.ORDER_TYPE_SELL
    lot = abs(lot)

    # Ensure symbol is enabled
    mt5.symbol_select(symbol, True)

    # Get fresh tick
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        print(f"{symbol} → No tick data")
        return

    # Determine price
    price = tick.ask if order_type == mt5.ORDER_TYPE_BUY else tick.bid
    if price is None or price <= 0:
        print(f"{symbol} → Invalid price ({price})")
        return

    # Build order request
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": lot,
        "type": order_type,
        "price": price,
        "deviation": 1000,
        "magic": 1,
        "comment": "ICM auto-exec",
        "type_filling": mt5.ORDER_FILLING_IOC,  # most permissive
        "type_time": mt5.ORDER_TIME_GTC
    }

    # Send order
    result = mt5.order_send(request)

    # Print full result for debugging
    print(f"{symbol} {lot} → retcode: {result.retcode}")
    print("\n=== ORDER RESULT ===")
    print("Symbol:", symbol)
    print("Lot:", lot)
    print("Retcode:", result.retcode)
    print("Comment:", result.comment)
    print("Request ID:", result.request_id)
    print("Order:", result.order)
    print("Volume:", result.volume)
    print("Price:", result.price)
    print("Bid:", result.bid)
    print("Ask:", result.ask)
    print("Retcode External:", result.retcode_external)
print("====================\n")

# -----------------------------------
# Loop through CSV and execute
# -----------------------------------
for _, row in df.iterrows():
    symbol = row["asset"]
    lot = row["target_lot_size"]

    if lot != 0:
        send_order(symbol, lot)

# -----------------------------------
# Shutdown
# -----------------------------------
mt5.shutdown()