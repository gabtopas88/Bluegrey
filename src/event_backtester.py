import os
import logging
from datetime import timedelta
from pathlib import Path
from typing import Optional
import pandas as pd
from src import config
from src.store import DataStore
from src.risk import RiskManager
from src.fees import IBKRFeeModel
from src.bar_source import (
    BarSource,
    ArcticBarSource,
    POLICY_REJECT,
    POLICY_FFILL,
    VALID_MISSING_BAR_POLICIES,
)
from src.telemetry import (
    TelemetryStore,
    canonical_params_payload,
    compute_params_hash,
    make_backtest_run_id,
    RUN_KIND_BACKTEST,
)

# Minimal Logging for Backtest (Clean Output)
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

class VirtualBroker:
    """
    Simulates the Exchange and the Account.
    Tracks Cash, Positions, and handles Order Execution logic precisely.
    """
    def __init__(self, initial_capital=100000.0, fee_params: Optional[dict] = None):
        self.initial_capital = initial_capital
        
        # Gross vs Net Tracking
        self.net_cash = initial_capital
        self.gross_cash = initial_capital
        
        self.positions = {} # { 'ASSET_KEY': quantity }
        self.equity_curve = []
        self.last_known_prices = {} 
        
        # Institutional Cost Model.
        # Parameters come from config.FEE_MODEL_PARAMS rather than a literal, so
        # the event backtester, the vector optimizer and the run params_hash all
        # read one definition. Three hardcoded copies of a cost assumption is
        # three chances for them to silently disagree — and a cost model that
        # differs between engines invalidates any comparison between them.
        self.fee_params = dict(fee_params if fee_params is not None
                               else getattr(config, 'FEE_MODEL_PARAMS', {}))
        self.fee_model = IBKRFeeModel(**self.fee_params)
        
        # Fee Ledger
        self.total_commissions_paid = 0.0
        self.total_regulatory_paid = 0.0
        self.total_slippage_incurred = 0.0

    def mark_to_market(self, prices, timestamp):
        """
        Calculates total Liquidation Value of the portfolio (both Gross and Net).
        """
        for key, price in prices.items():
            if price is not None and not pd.isna(price) and price > 0:
                self.last_known_prices[key] = price

        net_equity = self.net_cash
        gross_equity = self.gross_cash
        
        for key, qty in self.positions.items():
            price = prices.get(key)
            if pd.isna(price) or price is None:
                price = self.last_known_prices.get(key, 0)
            
            position_value = qty * price
            net_equity += position_value
            gross_equity += position_value
        
        self.equity_curve.append({
            'time': timestamp, 
            'net_equity': net_equity,
            'gross_equity': gross_equity,
            'total_fees': self.total_commissions_paid + self.total_regulatory_paid
        })
        return net_equity

    def execute(self, orders, market_prices):
        """
        Fills orders and applies exact micro-structure costs.
        """
        fills = []
        for order in orders:
            key = order.get('symbol')
            action = order['action']
            qty = order['qty']
            contract = order.get('contract')
            
            if not contract:
                logger.error(f"❌ Order rejected: Missing Contract object for {key}")
                continue

            # --- INSTITUTIONAL ASSET CLASS ROUTING ---
            # Extract the actual security type from the IBKR Contract object
            sec_type = getattr(contract, 'secType', 'STK')
            
            # Map IBKR secTypes to our FeeModel format
            if sec_type == 'CASH':
                asset_class = 'FX'
            elif sec_type == 'CRYPTO':
                asset_class = 'CRYPTO'
            else:
                asset_class = 'STK'
            
            # Check price availability.
            # _bt_market_key is the UNIVERSE key ('C:AUDUSD'), resolved by the
            # engine from contract identity. order['symbol'] is the contract's
            # localSymbol ('AUD.USD'), which is what the Risk Manager and the
            # live telemetry use — it is NOT a key into market_prices, and
            # looking prices up by it silently filled nothing at all.
            lookup_key = order.get('_bt_market_key') or key
            price = market_prices.get(lookup_key)
            if pd.isna(price) or price is None:
                continue 
            
            # --- 1. CALCULATE TRUE COSTS ---
            cost_details = self.fee_model.calculate_event(asset_class, action, qty, price)
            total_cost_usd = cost_details['total_cost']
            
            # Convert total slippage back to per-share to adjust the fill price
            slippage_impact = cost_details['slippage'] / qty 
            
            # --- 2. APPLY SLIPPAGE TO FILL PRICE ---
            fill_price = price + slippage_impact if action == 'BUY' else price - slippage_impact
            trade_value = fill_price * qty
            frictionless_trade_value = price * qty
            
            # --- 3. ACCOUNTING ---
            if action == 'BUY':
                self.net_cash -= (trade_value + cost_details['commission'] + cost_details['regulatory'])
                self.gross_cash -= frictionless_trade_value
                self.positions[key] = self.positions.get(key, 0) + qty
            elif action == 'SELL':
                self.net_cash += (trade_value - cost_details['commission'] - cost_details['regulatory'])
                self.gross_cash += frictionless_trade_value
                self.positions[key] = self.positions.get(key, 0) - qty
                
            # Ledger Updates
            self.total_commissions_paid += cost_details['commission']
            self.total_regulatory_paid += cost_details['regulatory']
            self.total_slippage_incurred += cost_details['slippage']
                
            fills.append({
                # Universe key, used for position bookkeeping.
                'asset': order.get('_bt_market_key') or key,
                # localSymbol, matching what the live telemetry records so the
                # two fills streams carry comparable labels.
                'symbol': key,
                'action': action,
                'qty': qty,
                'price': fill_price,
                'commission': cost_details['commission'],
                'regulatory': cost_details['regulatory'],
                'slippage': cost_details['slippage'],
                # Frictionless reference price. Persisted so modelled slippage is
                # directly comparable to the live telemetry's realized
                # slippage_bps without recomputing it from a join.
                'estimated_price': price,
                # Carries the synthetic order id assigned by the engine so a
                # fill row can be joined back to its order row, exactly as
                # ib_order_id links the two live streams.
                'bt_order_id': order.get('_bt_order_id'),
            })
        return fills

class BacktestEngine:
    """
    The Time Machine.
    Simulates the Live Engine's Event Loop over historical data.

    Data acquisition is delegated to a BarSource (see src/bar_source.py). The
    default is ArcticBarSource, so existing research calls are unaffected:

        BacktestEngine(strategy_class, instruments, params, data_library='fx/min')

    still works exactly as before. Injecting a bar_source is opt-in and is what
    the parity harness uses to replay a live tape.
    """
    def __init__(self, strategy_class, instruments, params, data_library: str = None, start_cap=100000,
                 risk_mode: str = None,
                 bar_source: BarSource = None,
                 missing_bar_policy: str = POLICY_REJECT,
                 execution_delay_bars: int = 1,
                 max_orders_per_minute: float = None,
                 gap_threshold: timedelta = None,
                 run_id: str = None,
                 fee_params: dict = None,
                 emit_telemetry: bool = False,
                 telemetry_base_path=None):
        """
        :param data_library: ArcticDB library name. Required unless bar_source
            is supplied explicitly.
        :param bar_source: optional BarSource override. When None, an
            ArcticBarSource is built from data_library — the normal research
            path. Nothing here depends on the parity harness.
        :param missing_bar_policy: 'reject' (default, live-faithful) leaves a
            tickless leg as NaN so the strategy refuses the incomplete
            cross-section, exactly as the live DataManager does. 'ffill' is the
            legacy behaviour, retained so earlier research results remain
            reproducible. See src/bar_source.py for why ffill inflates alpha.
        :param execution_delay_bars: how many bars after signal generation an
            order fills. Default 1 preserves existing behaviour (fill at the
            NEXT bar's close). Note the live engine is effectively 0: it places
            a market order immediately after the bar closes and fills within
            seconds at roughly that close. Set 0 to model that.

            Measured in BARS, so it scales with the timeframe automatically —
            but the COST of a 1-bar delay does not. One bar of lag is ~60s on
            minute data and a full hour on hourly data, where it becomes the
            dominant term in any fill-price comparison.
        :param max_orders_per_minute: velocity cap. When None (default), the
            RiskManager's own limit applies — the same one the live engine
            enforces. Previously this was hardcoded to infinity, which let
            backtests fire order sequences no real IBKR account would accept.
            Pass float('inf') to restore the old uncapped behaviour.
        :param gap_threshold: bar-timestamp delta above which strategy state is
            reset. When None (default) it is DERIVED from the data's actual bar
            interval as max(1 hour, 3 x bar_interval).

            A fixed 1-hour threshold only works for intraday bars up to 1h. On
            4-hour or daily bars every normal bar-to-bar step exceeds it, so the
            strategy would reset on EVERY bar and silently produce a flat equity
            curve that looks like "no signals found" rather than a failure. The
            1-hour floor keeps minute-bar behaviour byte-identical to before.

            ⚠️ ASYMMETRY (surfaced deliberately, not silently patched): the live
            engine detects gaps PER-LEG from tick staleness inside DataManager,
            while this engine detects them from the delta between consecutive
            aligned bar timestamps. The two mechanisms fire at different moments
            on the same market data. The parity harness reports gap-adjacent
            bars as a finding rather than pretending they reconcile.
        :param run_id: optional deterministic run identifier. When None one is
            derived from the params_hash. Computing it costs nothing and writes
            nothing — telemetry emission is a separate, opt-in concern.
        :param fee_params: override for the cost model parameters. Defaults to
            config.FEE_MODEL_PARAMS.
        :param emit_telemetry: when True, writes the SAME three streams the live
            engine writes (decisions / orders / fills) into an isolated tree at
            config.BACKTEST_PATH/{run_id}/. Default False, so the research path
            performs zero extra I/O.

            Deliberately a separate flag rather than being triggered by passing
            run_id: run_id is always computed for identity, and overloading an
            identifier to also start writing files makes a harmless-looking
            argument have side effects.
        :param telemetry_base_path: override for the backtest telemetry root.
            Defaults to config.BACKTEST_PATH. Never point this at the live
            telemetry tree — see src/telemetry.py on why backtest rows must not
            share day-partition files with live rows.
        """
        if missing_bar_policy not in VALID_MISSING_BAR_POLICIES:
            raise ValueError(
                f"missing_bar_policy must be one of {VALID_MISSING_BAR_POLICIES}, "
                f"got {missing_bar_policy!r}"
            )
        if execution_delay_bars < 0:
            raise ValueError("execution_delay_bars must be >= 0.")

        self.instruments = instruments
        self.params = params
        self.missing_bar_policy = missing_bar_policy
        self.execution_delay_bars = int(execution_delay_bars)

        # Bar source. Default keeps the research path identical to before.
        if bar_source is not None:
            self.bar_source = bar_source
        else:
            if not data_library:
                raise ValueError("Either data_library or bar_source must be provided.")
            self.bar_source = ArcticBarSource(
                library_name=data_library,
                instrument_keys=list(instruments.keys()),
                missing_bar_policy=missing_bar_policy,
            )

        # Retained for backwards compatibility: notebooks and tools reference
        # engine.store directly. Points at the same DataStore the source uses
        # when one was constructed, rather than opening a second connection.
        self.store = getattr(self.bar_source, 'store', None)

        # Qualification substitute: must run BEFORE the RiskManager whitelist is
        # ever consulted and before the strategy builds orders.
        self._local_to_key = {}
        self._ensure_local_symbols()

        self.strategy = strategy_class(instruments, params)
        self.broker = VirtualBroker(start_cap, fee_params=fee_params)
        # RiskManager needs the universe so its symbol whitelist resolves
        # against contract.localSymbol — mirrors the live engine wiring.
        #
        # risk_mode mirrors the live engine's enforcement mode so a no-risk
        # paper run can be matched by a no-risk backtest (parity). If not passed
        # explicitly (notebook control), it falls back to config.RISK_MODE so
        # setting RISK_MODE=shadow in the environment toggles BOTH engines
        # symmetrically. ENFORCE is the safe default.
        resolved_risk_mode = (
            risk_mode if risk_mode is not None
            else getattr(config, 'RISK_MODE', RiskManager.MODE_ENFORCE)
        )
        self.risk = RiskManager(instruments=instruments, mode=resolved_risk_mode)
        
        # Velocity cap. Only overridden when the caller asks; otherwise the
        # RiskManager's own default stands, which is what the live engine runs.
        if max_orders_per_minute is not None:
            self.risk.max_orders_per_minute = max_orders_per_minute
        self.max_orders_per_minute = self.risk.max_orders_per_minute
        
        self.trades = []
        self.history = [] 
        # Resolved in _run_simulation once the bar interval is known — a fixed
        # threshold cannot serve every timeframe (see _resolve_gap_threshold).
        # Seeded with the 1-minute-era default so the attribute is always valid
        # even if someone inspects the engine before running it.
        self._gap_threshold_override = gap_threshold
        self.gap_threshold = gap_threshold if gap_threshold is not None else timedelta(hours=1)
        self.bar_interval = None

        # --- RUN IDENTITY ---
        # params_hash folds strategy params, the EFFECTIVE risk mode and the fee
        # model params together, so a run can never silently match another that
        # traded under different assumptions. Read the mode off the constructed
        # RiskManager, never the raw config: an unrecognised value is downgraded
        # to ENFORCE internally, so hashing config would record a mode that was
        # never in force.
        self.params_hash = compute_params_hash(
            strategy_params=self.params,
            risk_mode=self.risk.mode,
            fee_params=self.broker.fee_params,
        )
        self.run_id = run_id if run_id is not None else make_backtest_run_id(self.params_hash)

        # Diagnostics populated during run(), reported in the tearsheet.
        self.rejected_bar_count = 0
        self.gap_event_count = 0

        # --- TELEMETRY (opt-in) ---
        # Configuration only. The store is constructed in _run_simulation, once
        # the bar interval and the derived gap threshold are known, so the
        # session manifest records the policies that were ACTUALLY in force
        # rather than the unresolved arguments. It also means a run that never
        # executes leaves no orphan directory behind.
        self.telemetry = None
        self._emit_telemetry = bool(emit_telemetry)
        self._telemetry_base_path = telemetry_base_path
        self._bt_order_seq = 0
        self._fill_seq = 0

    def _ensure_local_symbols(self):
        """
        Populates localSymbol on any unqualified contract.

        IBKR sets localSymbol during qualifyContracts(), which never happens in a
        backtest — there is no connection. Two things break as a result:

          1. RiskManager._allowed_local_symbols() builds its whitelist from
             truthy localSymbols. With every contract unqualified the whitelist
             is EMPTY, so in ENFORCE mode every order is rejected and the
             backtest silently trades nothing.
          2. Backtest telemetry would label fills differently from live
             telemetry, which records contract.localSymbol.

        We synthesize the same string IBKR would produce ('AUD.USD' for
        Forex, the plain symbol otherwise) so both problems disappear and the
        two engines' telemetry is directly comparable. Contracts already
        carrying a localSymbol are left untouched.
        """
        synthesized = {}
        for kkey, contract in self.instruments.items():
            if getattr(contract, 'localSymbol', ''):
                continue
            sym = getattr(contract, 'symbol', '') or str(kkey)
            if getattr(contract, 'secType', '') == 'CASH':
                cur = getattr(contract, 'currency', '')
                local = f"{sym}.{cur}" if cur else sym
            else:
                local = sym
            try:
                contract.localSymbol = local
                synthesized[kkey] = local
            except Exception:
                pass
        if synthesized:
            logger.info(f"🏷️ Synthesized localSymbol for unqualified contracts: {synthesized}")
        # Reverse map from localSymbol -> universe key, used to resolve market
        # prices for orders (which carry localSymbol, not the universe key).
        self._local_to_key = {
            getattr(c, 'localSymbol', '') or k: k for k, c in self.instruments.items()
        }

    def _init_telemetry(self, start_date, end_date, n_bars: int):
        """
        Builds the backtest TelemetryStore. Called from _run_simulation once the
        run policies are fully resolved, so the manifest is a faithful record of
        the run rather than of its arguments.

        Buffered, because a backtest emits thousands of rows per second and the
        live engine's write-on-event path is O(n^2) at that rate. A backtest is
        deterministic and re-runnable, so the crash-safety that cost buys is
        worth nothing here.
        """
        if not self._emit_telemetry:
            return

        base = self._telemetry_base_path if self._telemetry_base_path is not None else getattr(
            config, 'BACKTEST_PATH', config.DATA_DIR / 'backtests')

        self.telemetry = TelemetryStore(
            base_path=Path(base) / self.run_id,
            strategy_name=self.strategy.__class__.__name__,
            run_id=self.run_id,
            run_kind=RUN_KIND_BACKTEST,
            buffered=True,
            session_context={
                'risk_mode':            self.risk.mode,
                'params_hash':          self.params_hash,
                'params':               canonical_params_payload(
                    strategy_params=self.params,
                    risk_mode=self.risk.mode,
                    fee_params=self.broker.fee_params,
                ),
                # The run policies belong in the manifest too. A parity result
                # is meaningless without knowing which execution delay, bar
                # interval and missing-bar policy produced it.
                'bar_source':            self.bar_source.description,
                'missing_bar_policy':    self.missing_bar_policy,
                'execution_delay_bars':  self.execution_delay_bars,
                'max_orders_per_minute': self.max_orders_per_minute,
                'bar_interval_seconds':  self.bar_interval.total_seconds() if self.bar_interval is not None else None,
                'gap_threshold_seconds': self.gap_threshold.total_seconds(),
                'gap_threshold_source':  'explicit' if self._gap_threshold_override is not None else 'derived',
                # Same config over different windows shares a params_hash by
                # design, so the window is what distinguishes those runs.
                'start_date':            str(start_date) if start_date is not None else None,
                'end_date':              str(end_date) if end_date is not None else None,
                'bar_count':             int(n_bars),
                'first_bar_utc':         self._first_bar_utc,
                'last_bar_utc':           self._last_bar_utc,
            },
        )

    def _resolve_gap_threshold(self, index) -> timedelta:
        """
        Derives the gap threshold from the data's own bar interval.

        A gap means "bars are missing", which is only definable relative to how
        far apart bars are SUPPOSED to be. The interval is taken as the median
        consecutive delta — robust to weekends, holidays and genuine gaps in a
        way the mean or min is not.

        Floor of 1 hour preserves the historical minute-bar default exactly, so
        existing research at 1m/5m/15m/1h is unchanged. Above that the threshold
        scales, so 4h and daily data stop resetting on every bar.
        """
        if self._gap_threshold_override is not None:
            return self._gap_threshold_override

        if index is None or len(index) < 3:
            return timedelta(hours=1)

        deltas = pd.Series(index).diff().dropna()
        if deltas.empty:
            return timedelta(hours=1)

        interval = deltas.median()
        if pd.isna(interval) or interval <= pd.Timedelta(0):
            return timedelta(hours=1)

        return max(timedelta(hours=1), interval * 3)

    def _print_run_banner(self, n_bars: int):
        """
        Prints the ACTIVE policies for this run.

        Silent behaviour changes between runs are how research results quietly
        stop being comparable. Every knob that can move the numbers is printed,
        every run, so a differing result always has a visible cause.
        """
        print("\n⚙️  --- ACTIVE RUN POLICIES ---")
        print(f"   Bar Source:        {self.bar_source.description}")
        print(f"   Missing Bar:       {self.missing_bar_policy}"
              f"{'  (live-faithful)' if self.missing_bar_policy == POLICY_REJECT else '  (LEGACY)'}")
        print(f"   Exec Delay:        {self.execution_delay_bars} bar(s)"
              f"{'  (live ≈ 0)' if self.execution_delay_bars else ''}")
        print(f"   Max Orders/Min:    {self.max_orders_per_minute}")
        print(f"   Risk Mode:         {self.risk.mode}")
        print(f"   Bar Interval:      {self.bar_interval}")
        print(f"   Gap Threshold:     {self.gap_threshold}"
              f"{'  (explicit)' if self._gap_threshold_override is not None else '  (derived)'}")
        print(f"   Fee Params:        {self.broker.fee_params}")
        print(f"   Params Hash:       {self.params_hash}")
        print(f"   Run ID:            {self.run_id}")
        print(f"   Bars:              {n_bars}")
        print("-------------------------------\n")

    def _safe_state_get(self, key: str, default: float = 0.0) -> float:
        """Defensive read of strategy state — not every strategy uses these keys.
        Mirrors TradingEngine._safe_state_get so both engines emit the same
        columns even for strategies that don't track leg quantities."""
        try:
            return float(self.strategy.state.get(key, default))
        except (AttributeError, TypeError):
            return default

    def _record_decision(self, timestamp, signal, market_snapshot: dict):
        """
        Persists a decisions row for every on_bar call.

        Mirrors TradingEngine._record_decision exactly, including the details
        that look incidental but are load-bearing for Tier 1 replay parity:

          - Fires on EVERY bar, unconditionally, including warmup, rejected and
            no-transition bars. A backtest that only logged trading bars could
            not be aligned against the live tape at all.
          - Fires BEFORE Risk runs, so the row captures the strategy's INTENT
            including any staged pending_transition.
          - NaN closes are EXCLUDED from the snapshot. The live engine omits
            tickless legs entirely; emitting NaN here instead would make two
            identical market states serialise differently and show up as a false
            divergence.
        """
        if not self.telemetry:
            return

        try:
            snapshot = {
                str(sym): float(val)
                for sym, val in market_snapshot.items()
                if val is not None and not pd.isna(val)
            }
            meta = signal.meta if signal and signal.meta else {}

            self.telemetry.record_decision(
                timestamp=timestamp,
                signal_type=signal.signal_type if signal else "NONE",
                current_pos=meta.get('current_pos', 0),
                held_qty_y=self._safe_state_get('held_qty_y'),
                held_qty_x=self._safe_state_get('held_qty_x'),
                meta=meta,
                market_snapshot=snapshot,
            )
        except Exception as e:
            logger.error(f"❌ Telemetry record_decision failed: {e}")

    def _record_order(self, order, signal_type: str, timestamp, market_snapshot: dict):
        """
        Persists an orders row at STAGING time.

        The live engine writes this row the moment placeOrder() returns, which
        is immediately after the decision. Staging is the backtest's equivalent
        moment — recording at fill time instead would misattribute the order to
        a later bar and break the timing comparison.

        estimated_price is the frictionless bar price at staging, matching the
        live engine's pre-trade estimate, so the two are directly comparable.
        """
        if not self.telemetry:
            return

        try:
            contract = order.get('contract')
            price = market_snapshot.get(order.get('_bt_market_key') or order.get('symbol'))
            self.telemetry.record_order(
                ib_order_id=order.get('_bt_order_id', 0),
                signal_type=signal_type,
                symbol=order.get('symbol'),
                con_id=getattr(contract, 'conId', 0) or 0,
                action=order.get('action', ''),
                qty=order.get('qty', 0.0),
                # StrategySignal.add_order stores these under 'type', 'price'
                # and 'volatility' (see strategies/base.py). The alternate keys
                # are accepted too so hand-built order dicts still work.
                order_type=order.get('type', order.get('order_type', 'MKT')),
                estimated_price=float(price) if price is not None and not pd.isna(price) else float('nan'),
                limit_price=order.get('price', order.get('limit_price')),
                estimated_volatility=order.get('volatility', order.get('estimated_volatility')),
                timestamp=timestamp,
            )
        except Exception as e:
            logger.error(f"❌ Telemetry record_order failed: {e}")

    def _record_fill(self, fill, timestamp):
        """
        Persists a fills row.

        Two mappings are deliberate:

          - side is emitted as BOT/SLD, not BUY/SELL, because that is what IBKR
            reports and the column has to be comparable without translation.
          - commission carries commission + regulatory. IBKR's CommissionReport
            is an all-in figure, whereas our fee model splits the two; summing
            is the closest analogue. For FX the regulatory term is zero anyway.

        realized_pnl is emitted as 0.0 and is NOT comparable to live. IBKR
        computes it under its own lot-matching convention; the VirtualBroker
        tracks net cash, not lots. Guessing a convention would produce a column
        that looks comparable and silently isn't — an explicit zero is the
        honest option. Closing this gap needs lot tracking in VirtualBroker.
        """
        if not self.telemetry:
            return

        try:
            action = str(fill.get('action', '')).upper()
            side = 'BOT' if action in ('BUY', 'BOT') else 'SLD'
            order_id = fill.get('bt_order_id') or 0
            self._fill_seq += 1

            self.telemetry.record_fill(
                ib_order_id=int(order_id),
                exec_id=f"bt-{order_id}-{self._fill_seq}",
                symbol=fill.get('symbol') or fill.get('asset'),
                con_id=getattr(self.instruments.get(fill.get('asset')), 'conId', 0) or 0,
                side=side,
                shares=fill.get('qty', 0.0),
                price=fill.get('price', 0.0),
                commission=fill.get('commission', 0.0) + fill.get('regulatory', 0.0),
                realized_pnl=0.0,
                estimated_price=fill.get('estimated_price', float('nan')),
                timestamp=timestamp,
            )
        except Exception as e:
            logger.error(f"❌ Telemetry record_fill failed: {e}")

    def _drain_ready_orders(self, bar_index: int, market_snapshot: dict, timestamp):
        """
        Executes every queued order whose delay has elapsed.

        Orders that cannot fill (missing price for their leg) stay queued and
        retry on the next bar, matching the previous behaviour where an unfilled
        order remained pending.
        """
        if not self._order_queue:
            return

        still_pending = []
        ready = []
        for ready_index, order in self._order_queue:
            if ready_index <= bar_index:
                ready.append((ready_index, order))
            else:
                still_pending.append((ready_index, order))

        if not ready:
            self._order_queue = still_pending
            return

        fills = self.broker.execute([o for _, o in ready], market_snapshot)

        # Track fills by ORDER ID, not by symbol. Symbol matching is ambiguous —
        # the order carries localSymbol while the fill carries the universe key,
        # and two orders on the same leg in one batch are indistinguishable by
        # symbol anyway. An id mismatch here silently leaves filled orders on the
        # queue, where they re-fill on every subsequent bar.
        filled_ids = {f.get('bt_order_id') for f in fills}
        for fill in fills:
            self.trades.append({**fill, 'time': timestamp})
            self._record_fill(fill, timestamp)

        # Anything ready but unfilled goes back on the queue at the same
        # readiness, so it retries immediately next bar.
        for ready_index, order in ready:
            if order.get('_bt_order_id') not in filled_ids:
                still_pending.append((ready_index, order))

        self._order_queue = still_pending

    def run(self, start_date, end_date):
        """
        Runs the simulation.

        Thin guard around _run_simulation so buffered telemetry is flushed even
        if the run raises. close() is idempotent, so the normal completion path
        flushing first costs nothing.
        """
        try:
            return self._run_simulation(start_date, end_date)
        finally:
            if self.telemetry:
                self.telemetry.close()

    def _run_simulation(self, start_date, end_date):
        print(f"⏳ Loading Data for {len(self.instruments)} assets ({start_date} to {end_date})...")

        universe = self.bar_source.load(start_date, end_date)

        if universe is None or universe.empty:
            print("❌ Universe is empty after alignment.")
            return pd.DataFrame()

        # Resolve timeframe-dependent policy from the data itself, then build
        # telemetry so the manifest captures the resolved values.
        deltas = pd.Series(universe.index).diff().dropna()
        self.bar_interval = deltas.median() if not deltas.empty else None
        self.gap_threshold = self._resolve_gap_threshold(universe.index)

        if self.bar_interval is not None and self.gap_threshold <= self.bar_interval:
            print(
                f"⚠️ gap_threshold ({self.gap_threshold}) is not greater than the bar "
                f"interval ({self.bar_interval}). Strategy state will reset on every "
                f"bar and no position will ever be held."
            )

        self._first_bar_utc = str(universe.index[0]) if len(universe) else None
        self._last_bar_utc = str(universe.index[-1]) if len(universe) else None
        self._init_telemetry(start_date, end_date, len(universe))

        cols = universe.columns
        print(f"▶️ Simulating {len(universe)} ticks...")
        self._print_run_banner(len(universe))

        last_time = None
        # Queue of (ready_bar_index, order). Replaces the old flat pending list
        # so execution_delay_bars can be honoured, including 0.
        self._order_queue = []

        for bar_index, row in enumerate(universe.itertuples(index=True, name=None)):
            timestamp = row[0]
            
            if last_time:
                delta = timestamp - last_time
                if delta > self.gap_threshold:
                    self.gap_event_count += 1
                    if hasattr(self.strategy, 'reset'):
                        self.strategy.reset()
                    self._order_queue = []
            
            last_time = timestamp

            bar_dict = {}
            market_snapshot = {}
            
            for i, (asset, metric) in enumerate(cols):
                val = row[i+1]
                if asset not in bar_dict:
                    bar_dict[asset] = {}
                bar_dict[asset][metric] = val
                if metric == 'close':
                    market_snapshot[asset] = val
            
            latest_bars = pd.DataFrame.from_dict(bar_dict, orient='index')
            latest_bars['time'] = timestamp

            # Diagnostic only — the strategy is what actually rejects the bar.
            # Counted so the tearsheet can report how much of the sample was
            # untradeable rather than leaving it invisible.
            if any(pd.isna(v) for v in market_snapshot.values()):
                self.rejected_bar_count += 1

            # Fill orders queued on an earlier bar.
            self._drain_ready_orders(bar_index, market_snapshot, timestamp)

            signal = self.strategy.on_bar(latest_bars)

            # --- TELEMETRY: record EVERY bar, unconditionally ---
            # Placed here, before Risk, so the row captures strategy INTENT —
            # identical to the live engine's ordering.
            self._record_decision(timestamp, signal, market_snapshot)

            if signal:
                if signal.meta:
                    record = signal.meta.copy()
                    record['timestamp'] = timestamp
                    self.history.append(record)
                    
                if signal.orders:
                    current_eq = self.broker.equity_curve[-1]['net_equity'] if self.broker.equity_curve else self.broker.initial_capital
                    self.risk.update_state(
                        current_equity=current_eq,
                        start_of_day_equity=self.broker.initial_capital 
                    )
                    
                    # Pending-transition protocol: mirror the live engine.
                    # On Risk approval, commit the strategy's staged state
                    # transition and queue the orders for next-bar execution.
                    # On rejection, roll back the staged transition so the
                    # strategy doesn't drift into a state it never reached.
                    #
                    # Issue 6: Risk may resize orders in place, so commit from the
                    # POST-Risk signal — identical to the live engine — so held_qty_*
                    # reflects approved sizes and the two paths stay in parity.
                    #
                    # Mode-agnostic, exactly like the live engine: in SHADOW mode
                    # check() only logs and returns True with orders untouched, so
                    # the matched no-risk backtest stays in parity with a no-risk
                    # paper run.
                    if self.risk.check(signal, current_time=timestamp.timestamp()):
                        self.strategy.commit_pending_transition(signal)
                        ready_index = bar_index + self.execution_delay_bars
                        for order in signal.orders:
                            # Shallow-copy before tagging so the strategy's own
                            # order dicts are never mutated by the engine.
                            self._bt_order_seq += 1
                            queued = dict(order)
                            queued['_bt_order_id'] = self._bt_order_seq
                            queued['_bt_market_key'] = self._local_to_key.get(
                                order.get('symbol'), order.get('symbol'))
                            self._record_order(queued, signal.signal_type, timestamp, market_snapshot)
                            self._order_queue.append((ready_index, queued))
                    else:
                        self.strategy.rollback_pending_transition()

            # With zero delay, orders staged on this bar are already fillable.
            # Drained AFTER signal generation so the strategy never sees state
            # from a fill it has not yet caused — no look-ahead.
            if self.execution_delay_bars == 0:
                self._drain_ready_orders(bar_index, market_snapshot, timestamp)

            self.broker.mark_to_market(market_snapshot, timestamp)

        # Buffered telemetry is worthless unless it reaches disk. Flushed here
        # and again in the finally-guard below so an exception mid-run still
        # leaves a partial, diagnosable log.
        if self.telemetry:
            self.telemetry.close()
            print(f"📒 Backtest telemetry written: {self.telemetry.base_path}")

        print("✅ Backtest Complete.")
        self._generate_tearsheet()
        
        return pd.DataFrame(self.broker.equity_curve).set_index('time')

    def _generate_tearsheet(self):
        if not self.broker.equity_curve:
            print("⚠️ No trades executed. Equity curve is empty.")
            return
            
        start_equity = self.broker.initial_capital
        end_gross = self.broker.equity_curve[-1]['gross_equity']
        end_net = self.broker.equity_curve[-1]['net_equity']
        
        gross_pnl = end_gross - start_equity
        net_pnl = end_net - start_equity
        
        print("\n📊 --- INSTITUTIONAL RESULTS ---")
        print(f"   Initial Cap:       ${start_equity:,.2f}")
        print(f"   Gross Equity:      ${end_gross:,.2f} (PnL: ${gross_pnl:,.2f})")
        print(f"   Net Equity:        ${end_net:,.2f} (PnL: ${net_pnl:,.2f})")
        print("---------------------------------")
        print(f"   Total Commissions: ${self.broker.total_commissions_paid:,.2f}")
        print(f"   Total Regulatory:  ${self.broker.total_regulatory_paid:,.2f}")
        print(f"   Est. Slippage:     ${self.broker.total_slippage_incurred:,.2f}")
        print(f"   Total Trades:      {len(self.trades)}")
        print("---------------------------------")
        # Data-quality diagnostics. An incomplete cross-section is not an error,
        # but a run where most bars were untradeable is a very different result
        # from one where none were — and that difference was previously invisible.
        print(f"   Incomplete Bars:   {self.rejected_bar_count} (policy: {self.missing_bar_policy})")
        print(f"   Gap Resets:        {self.gap_event_count}")
        print("---------------------------------\n")

        print("📈 Generating Institutional Tearsheet (quantstats)...")
        try:
            import quantstats as qs
            df = pd.DataFrame(self.broker.equity_curve)
            df.set_index('time', inplace=True)
            
            # You can now choose to analyze gross or net returns
            daily_net_equity = df['net_equity'].resample('D').last().dropna()
            returns = daily_net_equity.pct_change(fill_method=None).dropna()
            
            if returns.std() == 0:
                print("⚠️ Returns volatility is zero (no active trades). Skipping Tearsheet.")
                return
            
            os.makedirs("research/Tearsheets", exist_ok=True) 
            report_path = "research/Tearsheets/event_backtest_tearsheet.html"
            
            qs.reports.html(returns, output=report_path, title="Bluegrey Net PnL Tearsheet")
            print(f"✅ Tearsheet saved successfully to: {report_path}")
            
        except ImportError:
            print("⚠️ QuantStats not installed. Run 'pip install quantstats'.")
        except Exception as e:
            print(f"⚠️ Could not generate tearsheet: {e}")