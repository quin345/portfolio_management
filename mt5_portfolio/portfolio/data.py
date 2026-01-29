from brokers import get_broker
import MetaTrader5 as mt5
import sqlite3
from datetime import datetime
import pandas as pd

class MT5DatabaseSaver:
    def __init__(self, broker_name, db_path="market_data.db"):
        self.broker_name = broker_name.lower()
        self.db_path = db_path

        # Initialize broker
        self.broker = get_broker(broker_name)
        self.broker.initialize()

        # SQLite connection
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

        # Create broker-specific tables
        self._create_tables()

    def _table(self, base_name):
        return f"{self.broker_name}_{base_name}"

    def _create_tables(self):
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self._table("market_data")} (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                tick_volume INTEGER,
                PRIMARY KEY (symbol, timeframe, timestamp)
            )
        """)

        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self._table("symbol_metadata")} (
                symbol TEXT PRIMARY KEY,
                minimal_volume REAL,
                volume_step REAL,
                margin_currency TEXT,
                profit_currency TEXT
            )
        """)
        self.conn.commit()

    def save_data(self, symbol, timeframe, rates):
        for rate in rates:
            
            time = datetime.utcfromtimestamp(int(rate['time'])).strftime("%Y-%m-%d %H:%M:%S")

            self.cursor.execute(f"""
                INSERT OR IGNORE INTO {self._table("market_data")}
                (symbol, timeframe, timestamp, open, high, low, close, tick_volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(symbol),
                str(timeframe),
                time,
                float(rate['open']),
                float(rate['high']),
                float(rate['low']),
                float(rate['close']),
                int(rate['tick_volume'])
            ))
        self.conn.commit()

    def update_db(self, symbol="EURUSD", timeframe=mt5.TIMEFRAME_M1, num_bars=10):
        if not mt5.initialize():
            raise RuntimeError("MT5 initialization failed")

        rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, num_bars)
        if rates is None or len(rates) == 0:
            mt5.shutdown()
            raise ValueError(f"No data returned for {symbol} on timeframe {timeframe}")

        closed_rates = rates[:-1]  # drop last forming candle

        self.save_data(symbol, timeframe, closed_rates)
        self.save_symbol_info(symbol)

        mt5.shutdown()

    def save_symbol_info(self, symbol="EURUSD"):
        if not mt5.initialize():
            raise RuntimeError("MT5 initialization failed")

        info = mt5.symbol_info(symbol)
        if info is None:
            mt5.shutdown()
            raise ValueError(f"Symbol {symbol} not found")

        self.cursor.execute(f"""
            INSERT OR REPLACE INTO {self._table("symbol_metadata")}
            (symbol, minimal_volume, volume_step, margin_currency, profit_currency)
            VALUES (?, ?, ?, ?, ?)
        """, (
            info.name,
            float(info.volume_min),
            float(info.volume_step),
            getattr(info, "currency_margin", None),
            getattr(info, "currency_profit", None)
        ))
        self.conn.commit()

        mt5.shutdown()

    def load_ohlcv(self, symbol="EURUSD", timeframe="1", limit=500):
        """Load OHLCV data for this broker instance, guaranteed ascending."""
        table_name = self._table("market_data")

        # First get the latest N rows (descending), then reorder ascending
        query = f"""
            SELECT *
            FROM (
                SELECT timestamp, open, high, low, close, tick_volume
                FROM {table_name}
                WHERE symbol = ? AND timeframe = ?
                ORDER BY timestamp DESC
                LIMIT ?
            )
            ORDER BY timestamp ASC
        """

        df = pd.read_sql_query(query, self.conn, params=(symbol, timeframe, limit))

        df.rename(columns={"tick_volume": "volume"}, inplace=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df.set_index("timestamp", inplace=True)

        return df
        
    def load_symbol_info(self, symbol="EURUSD"):
        """Load symbol metadata for this broker instance."""
        table_name = self._table("symbol_metadata")
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE symbol = ?
        """

        df = pd.read_sql_query(query, self.conn, params=(symbol,))
        if df.empty:
            return None
        return df.iloc[0].to_dict()

    def close(self):
        self.conn.close()