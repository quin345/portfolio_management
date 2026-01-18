# brokers/icmarkets.py
import os
from dotenv import load_dotenv
from .base import BrokerBase

class ICMarkets(BrokerBase):
    def __init__(self):
        load_dotenv()
        self.login = int(os.getenv("ICM_MT5_LOGIN"))
        self.password = os.getenv("ICM_MT5_PASSWORD")
        self.server = os.getenv("ICM_MT5_SERVER")

    def initialize(self):
        return super().initialize(
            login=self.login,
            password=self.password,
            server=self.server
        )