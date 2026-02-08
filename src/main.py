import sys
import os
import time
import logging
from ib_async import *
from datetime import datetime

# Local Imports
import config
from src.data import DataManager
from src.execution import ExecutionHandler
from src.risk import RiskManager

# Dynamic Strategy Import (Change this line to swap strategies)
from strategies.kalman_strategy import KalmanPairStrategy 

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger("MainEngine")

class TradingEngine:
    def __init__(self):
        self.ib = IB()
        
        # 1. INFRASTRUCTURE
        self.data_manager = DataManager(self.ib, config.INSTRUMENTS)
        self.executor = ExecutionHandler(self.ib)
        self.risk = RiskManager()
        
        # 2. STRATEGY (The Brain)
        # We initialize it with the Instruments and Config Parameters
        self.strategy = KalmanPairStrategy(config.INSTRUMENTS, config.STRATEGY_PARAMS)
        
        logger.info(f"🤖 BLUEGREY ENGINE INITIALIZED.")
        logger.info(f"   Strategy: {self.strategy.__class__.__name__}")
        logger.info(f"   Universe: {list(config.INSTRUMENTS.keys())}")

    def start(self):
        """
        Boot Sequence.
        """
        try:
            logger.info("🔌 Connecting to IBKR...")
            self.ib.connect('127.0.0.1', config.IB_PORT, clientId=config.IB_CLIENT_ID)
            self.ib.reqMarketDataType(3) # Delayed Data (switch to 1 for Live)
            logger.info("✅ Connected.")
        except Exception as e:
            logger.error(f"❌ Connection Failed: {e}")
            return

        # Start Data Feed
        self.data_manager.subscribe()

        # Attach the Event Loop
        self.ib.pendingTickersEvent += self.on_tick_event
        
        logger.info("🟢 ENGINE RUNNING. Listening for Market Data...")
        self.ib.run()

    def on_tick_event(self, tickers):
        """
        THE MAIN LOOP.
        Triggered by IBKR roughly every 250ms if there are updates.
        """
        # 1. INGEST & CHECK HEALTH
        event = self.data_manager.on_tick(tickers)
        
        if not self.data_manager.is_ready():
            return

        # 2. HANDLE HEARTBEAT (GAPS)
        if event.gap_detected:
            logger.warning(f"⚠️ Gap Detected ({event.gap_duration}). Resetting Strategy State.")
            self.strategy.reset()
            return # Don't trade immediately after a reset

        # 3. RUN STRATEGY (Two Modes)
        signal = None
        
        # Mode A: New Candle (For Math/Logic Updates)
        if event.has_new_bar:
            latest_bars = self.data_manager.get_latest_bars()
            signal = self.strategy.on_bar(latest_bars)

        # Mode B: New Tick (For Risk/Stop-Loss Checks - Optional)
        # if event.has_new_tick:
        #     latest_prices = self.data_manager.get_latest_prices()
        #     signal = self.strategy.on_tick(latest_prices)

        # 4. EXECUTE & TELEMETRY
        if signal:
            self._handle_signal(signal)

    def _handle_signal(self, signal):
        """
        Routes the signal to Risk and Execution.
        """
        # A. Telemetry (Log the Brain's state)
        if signal.meta:
            self._log_telemetry(signal.meta)
            
        # B. Risk Check
        if not signal.orders:
            return
            
        if self.risk.check(signal):
            self.executor.execute_signal(signal)
        else:
            logger.warning("⛔ Signal rejected by Risk Manager.")

    def _log_telemetry(self, meta):
        """
        Writes to the dashboard CSV.
        """
        # Implementation of simple CSV logging for Streamlit
        # (Same as your previous monitor.py logic, just standardized)
        pass

if __name__ == "__main__":
    eng = TradingEngine()
    eng.start()