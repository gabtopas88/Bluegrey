import sys
import os
import time
import logging
import importlib
import pandas as pd
from ib_async import *
from datetime import datetime

# Local Imports
import config
from src.data import DataManager
from src.execution import ExecutionHandler
from src.risk import RiskManager

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
        
        # 2. STRATEGY (Dynamically Loaded)
        self.strategy = self._load_strategy()
        
        logger.info(f"🤖 BLUEGREY ENGINE INITIALIZED.")
        logger.info(f"   Loaded Strategy: {self.strategy.__class__.__name__}")
        logger.info(f"   Universe: {list(config.INSTRUMENTS.keys())}")

    def _load_strategy(self):
        """
        Dynamically loads the strategy class defined in config.py.
        This keeps the Engine completely blind to the strategy logic.
        """
        strategy_name = config.STRATEGY_CLASS
        
        try:
            # Fallback direct import for the specific Kalman strategy
            if strategy_name == "KalmanPairStrategy":
                from strategies.kalman_pairs import KalmanPairsStrategy
                return KalmanPairsStrategy(config.INSTRUMENTS, config.STRATEGY_PARAMS)
            
            # Dynamic module loading for future strategies
            module = importlib.import_module("strategies")
            strat_class = getattr(module, strategy_name)
            return strat_class(config.INSTRUMENTS, config.STRATEGY_PARAMS)
        
        except Exception as e:
            logger.error(f"❌ Failed to load strategy '{strategy_name}': {e}")
            sys.exit(1)

    def start(self):
        """Boot Sequence."""
        try:
            logger.info("🔌 Connecting to IBKR...")
            self.ib.connect('127.0.0.1', config.IB_PORT, clientId=config.IB_CLIENT_ID)
            self.ib.reqMarketDataType(3) # Delayed Data (switch to 1 for Live)
            logger.info("✅ Connected.")
        except Exception as e:
            logger.error(f"❌ Connection Failed: {e}")
            return

        # --- BOOTSTRAP SEQUENCE ---
        
        # 1. Prime the Strategy (Historical Warmup)
        self._prime_strategy()
        
        # 2. Sync Live Portfolio (The upcoming safety mechanism)
        self._sync_portfolio()

        # --- BIND EVENTS (CRITICAL FOR EXECUTION)---
        self.ib.orderStatusEvent += self.executor.on_order_status
        self.ib.execDetailsEvent += self.executor.on_exec_details
        self.ib.pendingTickersEvent += self.on_tick_event

        # Start Data Feed
        self.data_manager.subscribe()
        
        logger.info("🟢 ENGINE RUNNING. Listening for Market Data...")
        self.ib.run()

    def _prime_strategy(self):
        """Fetches historical bars and builds the matrix dictionary for warmup."""
        # 1. Ask the strategy what it needs
        lookback = self.strategy.get_warmup_lookback()
        
        # 2. If it returns 0, skip priming (e.g., for simple models)
        if lookback <= 0:
            logger.info("⏩ Strategy requires no warmup. Skipping historical fetch.")
            return
            
        logger.info(f"⏳ Strategy requested {lookback} bars for warmup. Fetching from IBKR...")
        
        close_data = {}
        for key, contract in config.INSTRUMENTS.items():
            self.ib.qualifyContracts(contract)
            
            # Determine data type based on asset class (Midpoint for FX, Trades for Equities)
            what_to_show = 'MIDPOINT' if getattr(contract, 'secType', '') == 'CASH' else 'TRADES'
            
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr='2 D',  
                barSizeSetting='1 min',
                whatToShow=what_to_show,
                useRTH=False,
                formatDate=1
            )
            
            df = util.df(bars)
            if df is not None and not df.empty:
                df.set_index('date', inplace=True)
                close_data[key] = df['close']
        
        if not close_data:
            logger.error("❌ Failed to fetch historical data. Strategy will cold-start.")
            return
            
        # Build the exact dictionary structure expected by store.py / prime_state
        hist_matrix = pd.DataFrame(close_data).dropna().tail(lookback)
        structured_data = {'close': hist_matrix}
        
        if len(hist_matrix) < lookback:
            logger.warning(f"⚠️ Only fetched {len(hist_matrix)} aligned bars. Lookback requested is {lookback}.")
        
        self.strategy.prime_state(structured_data)

    def _sync_portfolio(self):
        """Queries IBKR for current open positions and injects them into the Strategy.
        (Pending implementation)"""
        logger.info("🔄 Syncing Live Portfolio... (Module Pending)")
        pass

    def on_tick_event(self, tickers):
        """
        THE MAIN LOOP
        Triggered by IBKR roughly every 250ms if there are updates.
        """
        # 1. INGEST & CHECK HEALTH

        event = self.data_manager.on_tick(tickers)
        
        # --- RISK HEARTBEAT ---
        self.risk.update_state(current_equity=100000.0, start_of_day_equity=100000.0)
        
        if not self.data_manager.is_ready():
            return

        # 2. HANDLE HEARTBEAT (GAPS)
        if event.gap_detected:
            logger.warning(f"⚠️ Gap Detected ({event.gap_duration}). Resetting Strategy State.")
            if self.strategy:
                self.strategy.reset()
            return 
        
        # 3. RUN STRATEGY
        if not self.strategy:
            return

        signal = None
        
        # Pass the cross-sectional DataFrame to the strategy
        if event.has_new_bar:
            latest_bars = self.data_manager.get_latest_bars()
            signal = self.strategy.on_bar(latest_bars)
        
        # 4. EXECUTE & TELEMETRY
        if signal:
            self._handle_signal(signal)

    def _handle_signal(self, signal):
        """Routes the signal to Risk and Execution."""
        
        # A. Telemetry (Log the Brain's state)
        if signal.meta:
            self._log_telemetry(signal.meta)

        # B. Risk Check (Ignore empty signals generated during WARMUP or FLAT states)    
        if not signal.orders:
            return
            
        if self.risk.check(signal):
            self.executor.execute_signal(signal)
        else:
            logger.warning("⛔ Signal rejected by Risk Manager.")

    def _log_telemetry(self, meta):
        """Writes to the dashboard CSV."""
        pass

if __name__ == "__main__":
    eng = TradingEngine()
    eng.start()