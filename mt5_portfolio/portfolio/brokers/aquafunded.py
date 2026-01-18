import os
from dotenv import load_dotenv
from .base import BrokerBase

class AquaFunded(BrokerBase):
    def __init__(self):
        self.name = "aquafunded"
        load_dotenv()

        # Store credentials
        self.account = int(os.getenv("AQUA_MT5_LOGIN"))
        self.password = os.getenv("AQUA_MT5_PASSWORD")
        self.server = os.getenv("AQUA_MT5_SERVER")

    def initialize(self):
        # Call BrokerBase.initialize() with credentials
        return super().initialize(
            login=self.account,
            password=self.password,
            server=self.server
        )

    def login(self):
        # Explicit login method
        import MetaTrader5 as mt5
        return mt5.login(
            login=self.account,
            password=self.password,
            server=self.server
        )