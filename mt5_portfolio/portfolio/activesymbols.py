# export active symbols
from brokers import get_broker

def export_active_symbols(broker_name=None, csv_path="active_symbols.csv"):
    """Fetch active MT5 Market Watch symbols and export to CSV."""
    import MetaTrader5 as mt5
    import pandas as pd

    # Initialize broker if provided
    broker = None
    if broker_name:
        broker = get_broker(broker_name)
        print(f"Using broker: {broker_name}")

        # If your broker object has login() or initialize() methods, call them
        if hasattr(broker, "initialize"):
            broker.initialize()
        if hasattr(broker, "login"):
            broker.login()

    else:
        # Default MT5 init if no broker object is used
        if not mt5.initialize():
            raise RuntimeError("Failed to initialize MT5")

    # Only visible symbols (Market Watch)
    symbols = [s for s in mt5.symbols_get() if s.visible]

    df = pd.DataFrame([{
        "symbol": s.name
    } for s in symbols])

    df.to_csv(csv_path, index=False, header=False)
    print(f"Exported {len(df)} active symbols to {csv_path}")