import sys
import os
import time
import logging
from ib_async import *

# Add root to path so we can import config
sys.path.append(os.getcwd())
import config

from src.data import DataManager
from src.execution import ExecutionHandler
from src.risk import RiskManager
from strategies.template import GenericStrategy

"""
# --- DYNAMIC STRATEGY LOADER ---
# This allows us to plug in different strategy files without rewriting main.py
from strategies.momentum import MomentumStrategy # Example import
"""

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()
logging.getLogger("ib_async").setLevel(logging.ERROR)

class SystemMonitor:
    """
    Writes the 'Heartbeat' CSV for the Streamlit Dashboard.
    """
    def __init__(self, filepath='data/live_monitor.csv'):
        self.filepath = filepath
        # Ensure 'data' folder exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        # Create file with headers if missing (or overwrite to start fresh)
        # Note: 'a' appends, 'w' overwrites. Using 'a' is safer for restarts.
        if not os.path.exists(filepath):
            with open(filepath, 'w') as f:
                f.write("Timestamp,ASSET_A,ASSET_B,Spread,Z-Score\n")

    def log_tick(self, prices, meta):
        """
        prices: Series of current prices (from DataManager)
        meta: Dictionary from Strategy (must contain z_score and spread)
        """
        import datetime
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Safely get values (default to 0 if missing to prevent crashes)
        # Note: We access prices by the Config Keys ('ASSET_A', 'ASSET_B')
        price_a = prices.get('ASSET_A', 0)
        price_b = prices.get('ASSET_B', 0)
        z = meta.get('z_score', 0)
        spread = meta.get('spread', 0)
        
        # Append to CSV
        try:
            with open(self.filepath, 'a') as f:
                f.write(f"{now},{price_a},{price_b},{spread},{z}\n")
        except Exception as e:
            logger.error(f"⚠️ Dashboard Log Failed: {e}")

class TradingEngine:
    def __init__(self):
        self.ib = IB()
        
        # 1. SETUP INFRASTRUCTURE
        self.data_manager = DataManager(self.ib, config.INSTRUMENTS)
        self.executor = ExecutionHandler(self.ib)
        self.risk = RiskManager()
        self.monitor = SystemMonitor() #The Telemetry Recorder

        
        # 2. SETUP STRATEGY
        # We pass the Instrument Dictionary, not just a list of strings
        self.strategy = GenericStrategy(config.INSTRUMENTS, config.STRATEGY_PARAMS)
        
        logger.info(f"🤖 SYSTEM INITIALIZED. Universe: {list(config.INSTRUMENTS.keys())}")

    def start(self):
        try:
            self.ib.connect('127.0.0.1', config.IB_PORT, clientId=config.IB_CLIENT_ID)
            self.ib.reqMarketDataType(3) # Delayed Data for Paper
            logger.info("✅ Connected to IBKR")
        except Exception as e:
            logger.error(f"❌ Connection to TWS Failed: {e}")
            return

        # Start Data Stream
        self.data_manager.subscribe()

        # Attach the Event Loop
        self.ib.pendingTickersEvent += self.on_tick
        
        logger.info("🟢 ENGINE RUNNING. Waiting for market data...")
        self.ib.run()

    def on_tick(self, tickers):
        # 1. Ingest Data
        has_new_data = self.data_manager.on_tick(tickers)
        if not has_new_data or not self.data_manager.is_ready():
            return

        # 2. Get Snapshot
        prices = self.data_manager.get_latest_prices()

        # 3. Strategy Logic
        signal = self.strategy.on_tick(prices)
        
        if signal:
            # 4. Telemetry (The Eyes) -> Log to CSV for Dashboard
            # We log every single calculated tick, so the chart moves smoothly
            self.monitor.log_tick(prices, signal.meta)

            # 5. Risk & Execution (The Trader)
            # Only execute if we have actual Orders
            if signal.orders:
                if self.risk.check(signal):
                    self.executor.execute_signal(signal)
                    time.sleep(60) # Rate limit to prevent spam

if __name__ == "__main__":
    eng = TradingEngine()
    eng.start()