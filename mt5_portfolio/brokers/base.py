# brokers/base.py
import MetaTrader5 as mt5

class BrokerBase:
    def initialize(self, login, password, server):
        ok = mt5.initialize(login=login, password=password, server=server)
        if not ok:
            raise RuntimeError(f"MT5 initialization failed: {mt5.last_error()}")
        return True