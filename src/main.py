import sys
import os
import time
import logging
import importlib
import pandas as pd
from datetime import datetime, timezone
from ib_async import *

# Local Imports
import config
from src.data import DataManager
from src.execution import ExecutionHandler
from src.risk import RiskManager
from src.portfolio import PortfolioManager
from src.telemetry import TelemetryStore

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger("MainEngine")


class TradingEngine:
    def __init__(self):
        self.ib = IB()

        # 1. STRATEGY (Loaded first so we can tag telemetry with its name)
        self.strategy = self._load_strategy()

        # 2. TELEMETRY STORE
        # One store per engine boot. New boot = new run_id. Written immediately
        # so a crash before connect() still leaves a session manifest behind.
        telemetry_path = getattr(config, 'TELEMETRY_PATH', config.DATA_DIR / 'telemetry')
        self.telemetry = TelemetryStore(
            base_path=telemetry_path,
            strategy_name=self.strategy.__class__.__name__,
        )

        # 3. INFRASTRUCTURE
        self.data_manager = DataManager(self.ib, config.INSTRUMENTS)
        self.executor = ExecutionHandler(self.ib, telemetry=self.telemetry)
        self.risk = RiskManager()

        # Portfolio Manager is constructed at boot, after IB connects.
        self.portfolio = None

        logger.info(f"🤖 BLUEGREY ENGINE INITIALIZED.")
        logger.info(f"   Loaded Strategy: {self.strategy.__class__.__name__}")
        logger.info(f"   Universe: {list(config.INSTRUMENTS.keys())}")
        logger.info(f"   Run ID: {self.telemetry.run_id}")

    def _load_strategy(self):
        """
        Dynamically loads the strategy class defined in config.py.
        This keeps the Engine completely blind to the strategy logic.
        """
        strategy_name = config.STRATEGY_CLASS

        try:
            # Fallback direct import for the specific Kalman strategy
            if strategy_name == "KalmanPairsStrategy":
                from strategies.kalman_pairs import KalmanPairsStrategy
                return KalmanPairsStrategy(config.INSTRUMENTS, config.STRATEGY_PARAMS)

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
            self.ib.reqMarketDataType(3)  # Delayed Data (switch to 1 for Live)
            logger.info("✅ Connected.")
        except Exception as e:
            logger.error(f"❌ Connection Failed: {e}")
            return

        # --- BOOTSTRAP SEQUENCE ---

        # 1. Prime the Strategy (Historical Warmup)
        self._prime_strategy()

        # 2. Sync Live Portfolio (Inject broker reality into strategy state).
        if not self._sync_portfolio():
            logger.critical("🛑 Boot aborted: portfolio sync failed.")
            self.ib.disconnect()
            sys.exit(1)

        # --- BIND EVENTS (CRITICAL FOR EXECUTION) ---
        self.ib.orderStatusEvent += self.executor.on_order_status
        self.ib.execDetailsEvent += self.executor.on_exec_details
        self.ib.pendingTickersEvent += self.on_tick_event

        # Start Data Feed
        self.data_manager.subscribe()

        logger.info("🟢 ENGINE RUNNING. Listening for Market Data...")
        self.ib.run()

    def _prime_strategy(self):
        """Fetches historical bars and builds the matrix dictionary for warmup."""
        lookback = self.strategy.get_warmup_lookback()

        if lookback <= 0:
            logger.info("⏩ Strategy requires no warmup. Skipping historical fetch.")
            return

        logger.info(f"⏳ Strategy requested {lookback} bars for warmup. Fetching from IBKR...")

        close_data = {}
        for key, contract in config.INSTRUMENTS.items():
            self.ib.qualifyContracts(contract)

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

        hist_matrix = pd.DataFrame(close_data).dropna().tail(lookback)
        structured_data = {'close': hist_matrix}

        if len(hist_matrix) < lookback:
            logger.warning(f"⚠️ Only fetched {len(hist_matrix)} aligned bars. Lookback requested is {lookback}.")

        self.strategy.prime_state(structured_data)

    def _sync_portfolio(self) -> bool:
        """
        Reconciles broker state into strategy state.
        Solves the State Ephemerality problem on engine restart.
        """
        logger.info("🔄 Syncing Live Portfolio...")

        self.portfolio = PortfolioManager(self.ib, config.INSTRUMENTS)
        self.portfolio.initialize()

        policy = getattr(config, 'BOOT_ANOMALY_POLICY', PortfolioManager.POLICY_HALT)
        logger.info(f"   Anomaly Policy: {policy}")

        success = self.portfolio.sync_strategy_state(self.strategy, anomaly_policy=policy)

        if success:
            logger.info("✅ Portfolio State Synced.")
        return success

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

        if event.has_new_bar:
            latest_bars = self.data_manager.get_latest_bars()
            signal = self.strategy.on_bar(latest_bars)

            # --- TELEMETRY: record EVERY bar, unconditionally ---
            # This includes warmup, low-vol, and flat-no-transition bars.
            # The whole point is matching every timestamp the backtest saw.
            self._record_decision(latest_bars, signal)

        # 4. EXECUTE
        if signal:
            self._handle_signal(signal)

    def _record_decision(self, latest_bars: pd.DataFrame, signal):
        """
        Persists a decisions row for every on_bar call.
        Pulls the market snapshot from the bars matrix and the strategy state
        from the signal's meta dict (uniform across warmup/active bars).
        """
        if not self.telemetry:
            return

        try:
            # Use the bar's own timestamp if available, else now()
            ts_value = None
            if 'time' in latest_bars.columns and not latest_bars['time'].empty:
                ts_value = latest_bars['time'].iloc[0]
            ts = pd.Timestamp(ts_value) if ts_value is not None else datetime.now(timezone.utc)

            # Build market snapshot: {symbol_key: close_price}
            snapshot = {}
            if 'close' in latest_bars.columns:
                for sym in latest_bars.index:
                    val = latest_bars.loc[sym, 'close']
                    if pd.notna(val):
                        snapshot[str(sym)] = float(val)

            meta = signal.meta if signal and signal.meta else {}

            self.telemetry.record_decision(
                timestamp=ts,
                signal_type=signal.signal_type if signal else "NONE",
                current_pos=meta.get('current_pos', 0),
                held_qty_y=self._safe_state_get('held_qty_y'),
                held_qty_x=self._safe_state_get('held_qty_x'),
                meta=meta,
                market_snapshot=snapshot,
            )
        except Exception as e:
            logger.error(f"❌ Telemetry record_decision failed: {e}", exc_info=False)

    def _safe_state_get(self, key: str, default: float = 0.0) -> float:
        """Defensive read of strategy state — not every strategy uses these keys."""
        try:
            return float(self.strategy.state.get(key, default))
        except (AttributeError, TypeError):
            return default

    def _handle_signal(self, signal):
        """Routes the signal to Risk and Execution."""
        if not signal.orders:
            return

        if self.risk.check(signal):
            self.executor.execute_signal(signal)
        else:
            logger.warning("⛔ Signal rejected by Risk Manager.")


if __name__ == "__main__":
    eng = TradingEngine()
    eng.start()