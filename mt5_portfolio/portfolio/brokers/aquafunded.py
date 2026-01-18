# brokers/aquafunded.py
import os
from dotenv import load_dotenv
from .base import BrokerBase

class AquaFunded(BrokerBase):
    def __init__(self):
        self.name = "aquafunded"
        load_dotenv()
        self.login = int(os.getenv("AQUA_MT5_LOGIN"))
        self.password = os.getenv("AQUA_MT5_PASSWORD")
        self.server = os.getenv("AQUA_MT5_SERVER")

    def initialize(self):
        return super().initialize(
            login=self.login,
            password=self.password,
            server=self.server
        )