import sys
import os
import time
import logging
from ib_async import *
from datetime import datetime

# Local Imports
from src.config import config
from src.engine.data import DataManager
from src.engine.execution import ExecutionHandler
from src.engine.risk import RiskManager

# --- STRATEGY LOADER ---
# TODO: Import your specific strategy class here when ready.
# For now, we assume a standard interface.
# from strategies.my_strategy import MyStrategy 
from strategies.base import StrategySignal # Assuming base exists, or similar

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
        # Placeholder: Initialize your strategy here
        # self.strategy = MyStrategy(config.INSTRUMENTS, config.STRATEGY_PARAMS)
        self.strategy = None 
        logger.warning("⚠️ No Strategy Loaded. Engine is in 'Data-Only' mode.")
        
        logger.info(f"🤖 BLUEGREY ENGINE INITIALIZED.")
        logger.info(f"   Universe: {list(config.INSTRUMENTS.keys())}")

    def start(self):
        """
        Boot Sequence.
        """
        try:
            logger.info("🔌 Connecting to IBKR...")
            self.ib.connect(config.IB_HOST, config.IB_PORT, clientId=config.IB_CLIENT_ID)
            self.ib.reqMarketDataType(config.IB_MARKET_DATA_TYPE) # Delayed Data (switch to 1 for Live)
            logger.info("✅ Connected.")
        except Exception as e:
            logger.error(f"❌ Connection Failed: {e}")
            return

        # --- BIND EVENTS (CRITICAL FOR EXECUTION) ---
        # Connect the ExecutionHandler to IBKR's feedback loops
        self.ib.orderStatusEvent += self.executor.on_order_status
        self.ib.execDetailsEvent += self.executor.on_exec_details

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
        
        # --- RISK HEARTBEAT ---
        # Pings the Risk Manager with the latest equity state.
        # (Future: Link this to self.ib.accountSummary() for live IBKR data)
        self.risk.update_state(
            current_equity=100000.0, 
            start_of_day_equity=100000.0
        )
        
        if not self.data_manager.is_ready():
            return

        # 2. HANDLE HEARTBEAT (GAPS)
        if event.gap_detected:
            logger.warning(f"⚠️ Gap Detected ({event.gap_duration}). Resetting Strategy State.")
            if self.strategy:
                self.strategy.reset()
            return 

        # 3. RUN STRATEGY (If Loaded)
        if not self.strategy:
            return

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
        pass

if __name__ == "__main__":
    eng = TradingEngine()
    eng.start()
