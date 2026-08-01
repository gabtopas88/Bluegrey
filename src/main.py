import sys
import os
import time
import asyncio
import logging
import importlib
import pandas as pd
from datetime import datetime, timezone
from ib_async import *

# Local Imports
from src import config
from src.data import DataManager
from src.execution import ExecutionHandler
from src.risk import RiskManager
from src.portfolio import PortfolioManager
from src.telemetry import TelemetryStore

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s')
logger = logging.getLogger("MainEngine")

# Human-readable labels for IBKR market data modes (reqMarketDataType).
_MARKET_DATA_MODES = {1: 'LIVE', 2: 'FROZEN', 3: 'DELAYED', 4: 'DELAYED-FROZEN'}

# ==========================================
# 🛡️ CONNECTION SUPERVISOR TUNABLES
# ==========================================
# The engine is TICK-DRIVEN: on_tick_event is bound to ib.pendingTickersEvent
# and is the only thing that produces bars, decisions and orders. If the tick
# stream stops, the engine goes permanently silent WITHOUT crashing — the
# container stays "up", no new run_id is minted, and even the data-gap detector
# cannot fire because it lives inside on_tick_event.
#
# That is exactly what a hard 'Peer closed connection.' does: IB Gateway's daily
# restart (or any socket-level drop) tears down the session and the market-data
# subscriptions with it. A soft blip (Error 1100 -> 1102 'data maintained') is
# self-healing and needs no intervention; a peer-close does not heal itself.
#
# ib_async ships a Watchdog class for this, but it REQUIRES an IBC controller
# object so it can restart the TWS/Gateway application itself. Our gateway runs
# in a separate container with its own IBC, so the engine cannot (and must not)
# drive it. Hence a purpose-built supervisor here that only manages OUR side of
# the socket.
#
# Env-overridable so the cadence can be tuned on the VM without a code change.
# Promote to config.py if these ever need to be shared with another entrypoint.
SUPERVISOR_POLL_SECONDS = float(os.getenv("SUPERVISOR_POLL_SECONDS", "1.0"))
TICK_SILENCE_TIMEOUT_S = float(os.getenv("TICK_SILENCE_TIMEOUT_S", "300"))
PROBE_TIMEOUT_S = float(os.getenv("PROBE_TIMEOUT_S", "10"))
RECONNECT_INITIAL_BACKOFF_S = float(os.getenv("RECONNECT_INITIAL_BACKOFF_S", "5"))
RECONNECT_MAX_BACKOFF_S = float(os.getenv("RECONNECT_MAX_BACKOFF_S", "120"))
# 0 = retry forever. The gateway's daily restart can take several minutes, and
# giving up would reintroduce exactly the silent-death failure mode.
RECONNECT_MAX_ATTEMPTS = int(os.getenv("RECONNECT_MAX_ATTEMPTS", "0"))


class TradingEngine:
    def __init__(self):
        self.ib = IB()

        # 1. STRATEGY (Loaded first so we can tag telemetry with its name)
        self.strategy = self._load_strategy()

        # 2. RISK MANAGER (built before telemetry so the EFFECTIVE, validated
        # enforcement mode can be recorded in the session manifest). Receives the
        # universe so its symbol whitelist is derived from the same source of
        # truth as every other component. RISK_MODE is env/config-driven:
        # 'ENFORCE' (default) gates and resizes orders; 'SHADOW' only logs what it
        # would do and lets orders through untouched (paper diagnostics only).
        self.risk = RiskManager(
            instruments=config.INSTRUMENTS,
            mode=getattr(config, 'RISK_MODE', RiskManager.MODE_ENFORCE),
        )

        # 3. TELEMETRY STORE
        # One store per engine boot. New boot = new run_id. Written immediately
        # so a crash before connect() still leaves a session manifest behind.
        # session_context tags the manifest with the effective risk mode and the
        # market-data mode, so every run_id is self-describing for the parity
        # harness and for audit ("was this run risk-gated?").
        telemetry_path = getattr(config, 'TELEMETRY_PATH', config.DATA_DIR / 'telemetry')
        self.telemetry = TelemetryStore(
            base_path=telemetry_path,
            strategy_name=self.strategy.__class__.__name__,
            session_context={
                'risk_mode':         self.risk.mode,
                'market_data_type':  getattr(config, 'IB_MARKET_DATA_TYPE', 1),
            },
        )

        # 4. INFRASTRUCTURE
        self.data_manager = DataManager(self.ib, config.INSTRUMENTS)
        self.executor = ExecutionHandler(self.ib, telemetry=self.telemetry)

        # Portfolio Manager is constructed at boot, after IB connects.
        self.portfolio = None

        # 5. SUPERVISOR STATE
        # _last_tick_utc is stamped by on_tick_event and is the liveness signal
        # the watchdog reads. _session_started_utc is the fallback reference
        # before the first tick of a (re)connected session ever arrives.
        self._last_tick_utc = None
        self._session_started_utc = None
        self._last_probe_utc = None
        self._shutdown = False

        logger.info(f"🤖 BLUEGREY ENGINE INITIALIZED.")
        logger.info(f"   Loaded Strategy: {self.strategy.__class__.__name__}")
        logger.info(f"   Universe: {list(config.INSTRUMENTS.keys())}")
        logger.info(f"   Risk Mode: {self.risk.mode}")
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

    def _connect(self) -> bool:
        """
        Opens the IBKR session and sets the market-data mode.

        Factored out of start() so the supervisor can reuse the EXACT same
        connect semantics on every reconnect — no drift between the boot path
        and the recovery path.
        """
        try:
            # IB_HOST is env-driven (defaults to 127.0.0.1). In Docker the
            # compose file sets it to host.docker.internal so the container can
            # reach TWS / IB Gateway on the host. Native runs are unaffected.
            logger.info(f"🔌 Connecting to IBKR at {config.IB_HOST}:{config.IB_PORT} ...")
            self.ib.connect(config.IB_HOST, config.IB_PORT, clientId=config.IB_CLIENT_ID)

            # Market data mode is config-driven. Paper accounts without
            # live FX entitlements fall back to DELAYED(3); entitled runs set 1
            # for LIVE via IB_MARKET_DATA_TYPE. The resolved mode is logged so
            # the parity harness knows what data the live session actually traded on.
            md_type = getattr(config, 'IB_MARKET_DATA_TYPE', 1)
            self.ib.reqMarketDataType(md_type)
            logger.info(
                f"📶 Market data mode: {md_type} "
                f"({_MARKET_DATA_MODES.get(md_type, 'UNKNOWN')})"
            )
            logger.info("✅ Connected.")

            # Reset liveness references for the new session so the watchdog
            # measures silence from THIS connection, not the dead one.
            self._session_started_utc = datetime.now(timezone.utc)
            self._last_tick_utc = None
            self._last_probe_utc = None
            return True
        except Exception as e:
            logger.error(f"❌ Connection Failed: {e}")
            return False

    def start(self):
        """Boot Sequence."""
        if not self._connect():
            return

        # --- BOOTSTRAP SEQUENCE ---

        # 1. Prime the Strategy (Historical Warmup)
        self._prime_strategy()

        # 2. BIND EVENTS EARLY so that boot-time liquidations (if any) get
        # captured by on_exec_details into telemetry. The portfolio sync may
        # route liquidation orders through the executor; without the bindings
        # in place, fills would still happen but the telemetry rows would not.
        self.ib.orderStatusEvent += self.executor.on_order_status
        self.ib.execDetailsEvent += self.executor.on_exec_details

        # Observability: make every disconnect explicit in the log trail. This
        # handler ONLY logs — recovery is driven by the supervisor loop on the
        # main thread, never from inside an event callback.
        self.ib.disconnectedEvent += self._on_disconnected

        # 3. Sync Live Portfolio (Inject broker reality into strategy state).
        if not self._sync_portfolio():
            logger.critical("🛑 Boot aborted: portfolio sync failed.")
            self.ib.disconnect()
            sys.exit(1)

        # 4. Bind the tick handler last — we don't want on_tick firing before
        # state is reconciled.
        self.ib.pendingTickersEvent += self.on_tick_event

        # Start Data Feed
        self.data_manager.subscribe()

        logger.info("🟢 ENGINE RUNNING. Listening for Market Data...")

        # Supervised event loop. Replaces a bare self.ib.run(), which spins
        # forever on a dead socket after a peer-close and never recovers.
        self._supervise()

    # ==========================================
    # 🛡️ CONNECTION SUPERVISOR
    # ==========================================
    def _on_disconnected(self):
        """
        ib_async disconnectedEvent handler. LOGGING ONLY.

        Reconnection deliberately does NOT happen here: this fires from inside
        the asyncio event loop, and ib.connect() is blocking. Re-entering the
        loop from a callback is precisely the class of bug that silently killed
        order placement. The supervisor loop notices !isConnected() within one
        poll interval and recovers from the main thread instead.
        """
        logger.error("🔌 IBKR connection lost (disconnectedEvent). Supervisor will recover.")

    def _supervise(self):
        """
        Main supervisory loop — owns the process for the life of the engine.

        ib.sleep() pumps the asyncio event loop for the given duration and then
        returns control here, so callbacks (ticks, order status, fills) run
        exactly as they did under ib.run(), while this thread stays free to
        perform BLOCKING recovery work between pumps.

        Two independent failure detectors:
          1. isConnected() — catches a clean socket teardown.
          2. Tick watchdog — catches a ZOMBIE socket that still reports
             connected but has stopped delivering data.
        """
        try:
            while not self._shutdown:
                # Pump the event loop. All strategy work happens inside here.
                self.ib.sleep(SUPERVISOR_POLL_SECONDS)

                if not self.ib.isConnected():
                    logger.error("🔌 Socket is down. Entering reconnect sequence.")
                    self._reconnect()
                    continue

                self._check_tick_watchdog()

        except KeyboardInterrupt:
            logger.info("⏹️ Manual stop requested.")
        finally:
            self._shutdown = True
            if self.ib.isConnected():
                self.ib.disconnect()
            logger.info("🛑 ENGINE STOPPED.")

    def _check_tick_watchdog(self):
        """
        Detects a connection that claims to be alive but has stopped delivering
        market data.

        IMPORTANT: silence alone is NOT proof of a fault. FX is closed over the
        weekend, so a healthy engine is legitimately tickless for ~48h. Forcing
        a reconnect on silence alone would churn the session pointlessly every
        weekend.

        So silence only triggers a PROBE (a cheap reqCurrentTime round-trip).
        If the probe answers, the socket is healthy and the market is simply
        quiet — we do nothing. If the probe fails or times out, the socket is a
        zombie and we force a reconnect.
        """
        now = datetime.now(timezone.utc)
        reference = self._last_tick_utc or self._session_started_utc
        if reference is None:
            return

        if (now - reference).total_seconds() < TICK_SILENCE_TIMEOUT_S:
            return

        # Rate-limit probing so a genuinely closed market produces one probe per
        # timeout window rather than one per poll.
        if self._last_probe_utc is not None:
            if (now - self._last_probe_utc).total_seconds() < TICK_SILENCE_TIMEOUT_S:
                return
        self._last_probe_utc = now

        logger.warning(
            f"⏳ No ticks for > {TICK_SILENCE_TIMEOUT_S:.0f}s. Probing the API connection..."
        )

        if self._probe_connection():
            logger.info(
                "   -> Probe OK. Socket is healthy; market is closed or quiet. No action taken."
            )
            return

        logger.critical(
            "🚨 Probe FAILED: connection is a zombie (reports connected, delivers nothing). "
            "Forcing a reconnect."
        )
        try:
            self.ib.disconnect()
        except Exception as e:
            logger.error(f"   -> Error while tearing down the zombie socket: {e}")
        self._reconnect()

    def _probe_connection(self) -> bool:
        """
        Liveness probe: ask IBKR for its current time with a hard timeout.

        Runs from the supervisor thread (not a callback), so driving the
        coroutine via ib.run() is safe here. A timeout is essential — a zombie
        socket typically hangs rather than raising.
        """
        try:
            self.ib.run(
                asyncio.wait_for(self.ib.reqCurrentTimeAsync(), PROBE_TIMEOUT_S)
            )
            return True
        except Exception as e:
            logger.error(f"   -> Connection probe failed: {e}")
            return False

    def _reconnect(self):
        """
        Reconnect with exponential backoff, then re-establish market data.

        Runs on the supervisor thread, so the blocking ib.connect() is safe.
        """
        delay = RECONNECT_INITIAL_BACKOFF_S
        attempt = 0

        while not self._shutdown:
            attempt += 1
            if RECONNECT_MAX_ATTEMPTS and attempt > RECONNECT_MAX_ATTEMPTS:
                logger.critical(
                    f"🛑 Giving up after {RECONNECT_MAX_ATTEMPTS} reconnect attempts. "
                    f"Engine is stopping so the container restart policy can take over."
                )
                self._shutdown = True
                return

            logger.warning(f"♻️ Reconnect attempt {attempt} in {delay:.0f}s ...")
            time.sleep(delay)

            # Always tear down any half-open socket before redialling, otherwise
            # IBKR can reject the clientId as already in use.
            try:
                if self.ib.isConnected():
                    self.ib.disconnect()
            except Exception as e:
                logger.error(f"   -> Pre-reconnect teardown error (continuing): {e}")

            if self._connect():
                self._resubscribe()
                return

            delay = min(delay * 2, RECONNECT_MAX_BACKOFF_S)

    def _resubscribe(self):
        """
        Re-establish market data after a reconnect.

        Market-data subscriptions do NOT survive a socket teardown, so without
        this the engine reconnects and then sits silent forever — the same
        outcome as no reconnect at all. subscribe() also re-qualifies every
        contract, which refreshes the conId that the execution path relies on.

        Event handlers are deliberately NOT re-bound: ib_async keeps handler
        registrations on the IB instance across a disconnect, so re-binding
        would attach on_tick_event twice and duplicate every decision row.
        """
        try:
            self.data_manager.subscribe()
            # Give the watchdog a fresh reference so it doesn't immediately
            # probe again while the first ticks are still in flight.
            self._session_started_utc = datetime.now(timezone.utc)
            self._last_tick_utc = None
            self._last_probe_utc = None
            logger.info("✅ Market data re-subscribed after reconnect.")
        except Exception as e:
            logger.critical(
                f"🚨 Re-subscribe FAILED after reconnect: {e}. "
                f"The engine is connected but will receive no data.",
                exc_info=True,
            )

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

        Passes the ExecutionHandler into PortfolioManager so that boot-time
        liquidations (under LIQUIDATE policy) flow through the same order path
        as runtime trades and land in the telemetry orders/fills streams.
        """
        logger.info("🔄 Syncing Live Portfolio...")

        self.portfolio = PortfolioManager(
            self.ib,
            config.INSTRUMENTS,
            executor=self.executor,
        )
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
        # 0. LIVENESS STAMP — read by the supervisor's tick watchdog. Recorded
        # before any other work so a downstream exception can never make a live
        # feed look dead.
        self._last_tick_utc = datetime.now(timezone.utc)

        # 1. INGEST & CHECK HEALTH
        event = self.data_manager.on_tick(tickers)

        # --- RISK HEARTBEAT ---
        # TODO: Replace dummy equity values with real account values from
        # ib.accountSummary() before connecting real capital. Currently the
        # drawdown kill switch cannot fire because these values never change.
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

        # If the strategy has soft-halted (e.g. data gap during live exposure),
        # do not feed it bars. We still drain the IBKR connection so order
        # callbacks for any in-flight orders continue to land in telemetry.
        if getattr(self.strategy, 'is_halted', lambda: False)():
            return

        signal = None

        if event.has_new_bar:
            latest_bars = self.data_manager.get_latest_bars()
            signal = self.strategy.on_bar(latest_bars)

            # --- TELEMETRY: record EVERY bar, unconditionally ---
            # This includes warmup, low-vol, and flat-no-transition bars.
            # The whole point is matching every timestamp the backtest saw.
            # Note: this fires BEFORE Risk runs, so the decision row captures
            # the strategy's INTENT including any staged pending_transition.
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

            # Build market snapshot: {symbol_key: close_price}.
            # NaN closes (tickless legs) are excluded so the snapshot
            # only ever carries real prices.
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
        """
        Routes the signal to Risk and Execution.
 
        Pending-transition protocol: the strategy stages state mutations into
        self.state['pending_transition'] inside on_bar. Risk approval commits
        the staged state and sends orders; rejection rolls the staged state
        back so the strategy doesn't think it has a position it never opened.
 
        Issue 6: RiskManager.check() may shrink order quantities IN PLACE. We
        therefore pass the POST-Risk signal into commit_pending_transition() so
        the strategy reconciles held_qty_* against what was actually approved/
        sent — not the pre-Risk staged size — keeping a later exit correctly sized.
 
        Risk modes: this routing is mode-agnostic. In ENFORCE mode check() may
        resize/veto; in SHADOW mode check() only logs what it would do and always
        returns True with the orders untouched. Either way, an approval commits
        the staged transition from the (possibly-resized) signal and sends it; a
        rejection rolls back. No special-casing is needed here — the mode lives
        entirely inside the RiskManager.
        """
        if not signal.orders:
            return
 
        if self.risk.check(signal):
            # Risk approved (ENFORCE may have resized orders in place; SHADOW
            # leaves them untouched). Commit the strategy's staged state
            # transition from the POST-Risk signal BEFORE sending orders so our
            # internal state reflects the trade we make.
            self.strategy.commit_pending_transition(signal)
            placed = self.executor.execute_signal(signal)

            # STATE-HONESTY ALERT: the commit above already mutated current_pos
            # and held_qty_*. If nothing actually reached IBKR, the strategy now
            # believes it holds a position the broker has never heard of — the
            # exact divergence the parity harness exists to catch. We cannot
            # simply roll back here (the pending transition is already consumed,
            # and a partial fill must NOT be rolled back), so we escalate loudly
            # instead of letting it pass unnoticed.
            if not placed:
                logger.critical(
                    "🚨 STATE DESYNC: Risk approved and the strategy COMMITTED its "
                    f"transition for {signal.signal_type}, but ZERO orders reached "
                    "IBKR. Strategy state and broker state have diverged — "
                    "investigate before trusting any further position telemetry."
                )
        else:
            # Risk rejected (ENFORCE only — SHADOW never rejects). Roll back the
            # staged transition so the strategy doesn't believe it has a position
            # it never opened.
            logger.warning("⛔ Signal rejected by Risk Manager.")
            self.strategy.rollback_pending_transition()
 
 
if __name__ == "__main__":
    eng = TradingEngine()
    eng.start()