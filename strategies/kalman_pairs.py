import numpy as np
import pandas as pd
from collections import deque
import logging
from typing import Dict, Tuple, TYPE_CHECKING

from strategies.base import BaseStrategy, StrategySignal

if TYPE_CHECKING:
    from src.portfolio import LivePosition

logger = logging.getLogger(__name__)


class KalmanPairsStrategy(BaseStrategy):
    """
    Dynamic Cointegration Pairs Trading via 1D Linear Kalman Filter.
    Designed for highly correlated FX pairs (e.g., AUD/USD vs NZD/USD).
    """
    def __init__(self, instruments: dict, params: dict):
        super().__init__(instruments, params)

        # Strategy Parameters
        self.leg_y = params.get('leg_y', 'C:AUDUSD')  # The Dependent Variable
        self.leg_x = params.get('leg_x', 'C:NZDUSD')  # The Independent Variable

        self.entry_z = params.get('entry_z', 2.0)
        self.exit_z = params.get('exit_z', 0.0)
        self.z_lookback = params.get('z_lookback', 120)  # 2 hours of 1-min bars

        # Kalman Filter State Variances
        self.delta = params.get('delta', 1e-5)  # How fast beta can adapt
        self.vt = params.get('vt', 1e-3)        # Observation noise variance

        # Production Execution Parameters
        self.base_qty = params.get('base_qty', 100000)  # Base unit for the Y leg

        # Minimum tradable size per leg. Guards against a vanishing β (or tiny
        # base_qty) rounding a leg quantity down to 0, which would fire a
        # one-legged spread (zero-qty order rejected by IBKR; the other leg left
        # naked). Configurable so FX-realistic odd-lot minimums can be raised.
        self.min_order_qty = params.get('min_order_qty', 1)

        # Drift threshold for hedge-ratio warning (in %). If the live held ratio
        # differs from the current model β by more than this on boot, log loudly.
        self.hedge_drift_threshold_pct = params.get('hedge_drift_threshold_pct', 20.0)

        # Event-Driven State Memory
        self.state = {
            'beta': None,
            'P': 1.0,
            'errors': deque(maxlen=self.z_lookback),  # O(1) rolling window
            'current_pos': 0,                          # 0: Flat, 1: Long Spread, -1: Short Spread
            'held_qty_y': 0.0,                         # Signed broker-confirmed quantity for leg Y
            'held_qty_x': 0.0,                         # Signed broker-confirmed quantity for leg X
            'halted': False,                           # Soft-halt flag set by reset() if blind
            'halt_reason': None,                       # Human-readable reason, surfaced in meta
            # 'pending_transition': dict | None
            # Staged by _generate_orders when on_bar decides to change position.
            # Holds the (current_pos, held_qty_y, held_qty_x) values that WOULD
            # become real if Risk approves. Engine calls commit_pending_transition()
            # to apply, or rollback_pending_transition() to discard.
            'pending_transition': None,
        }

    # ==========================================
    # ⚙️ LIFECYCLE & STATE PRIMING (Matrix Dictionary Input)
    # ==========================================
    def get_warmup_lookback(self) -> int:
        """Informs the Engine of historical data requirements."""
        return self.z_lookback

    def prime_state(self, historical_data: Dict[str, pd.DataFrame]):
        """
        Fast-forwards internal Kalman state variables using a dictionary of matrices.
        """
        close_matrix = historical_data.get('close')

        if close_matrix is None or close_matrix.empty:
            logger.warning("Priming failed: 'close' matrix missing. Strategy will cold-start.")
            return

        if self.leg_y not in close_matrix.columns or self.leg_x not in close_matrix.columns:
            logger.error("Priming failed: Requested legs not found in historical matrix.")
            return

        logger.info(f"Priming state with {len(close_matrix)} historical bars...")

        y = close_matrix[self.leg_y].values
        x = close_matrix[self.leg_x].values
        n = len(y)

        # Vectorized Kalman Pass
        beta = np.zeros(n)
        beta[0] = y[0] / x[0]
        P = np.zeros(n)
        P[0] = 1.0
        e = np.zeros(n)
        Q = np.zeros(n)
        wt = self.delta / (1 - self.delta)

        for t in range(1, n):
            beta_hat = beta[t-1]
            P_hat = P[t-1] + wt
            e[t] = y[t] - (beta_hat * x[t])
            Q[t] = P_hat * (x[t]**2) + self.vt
            K = P_hat * x[t] / Q[t]
            beta[t] = beta_hat + K * e[t]
            P[t] = P_hat * (1 - K * x[t])

        # Overwrite Live State Memory
        self.state['beta'] = beta[-1]
        self.state['P'] = P[-1]

        # Populate the ring buffer
        lookback_slice = min(self.z_lookback, n)
        self.state['errors'].extend(e[-lookback_slice:])

        logger.info(f"🧠 Warm-Up Complete | Beta: {self.state['beta']:.4f} | P: {self.state['P']:.6f}")

    def sync_positions(self, positions: Dict[str, 'LivePosition']) -> bool:
        """
        Reconciles broker-confirmed positions against the spread state machine.

        Maps (qty_y, qty_x) tuples onto current_pos ∈ {-1, 0, 1}:
            - (0, 0)       -> FLAT
            - (+, -)       -> LONG SPREAD
            - (-, +)       -> SHORT SPREAD
            - anything else -> ANOMALY (returns False)

        Held quantities are stored explicitly so exit orders use the *actual*
        broker-confirmed sizes — not recomputed against the current β, which may
        have drifted since the position was opened.

        Boot-time sync clears any stale pending_transition: at boot, the broker
        is the source of truth and any in-memory staged transition is irrelevant.
        """
        # A boot-time sync supersedes any in-memory staged transition.
        self.state['pending_transition'] = None

        y_pos = positions.get(self.leg_y)
        x_pos = positions.get(self.leg_x)

        y_qty = y_pos.quantity if y_pos else 0.0
        x_qty = x_pos.quantity if x_pos else 0.0

        # CASE 1: Both legs flat.
        if y_qty == 0 and x_qty == 0:
            self.state['current_pos'] = 0
            self.state['held_qty_y'] = 0.0
            self.state['held_qty_x'] = 0.0
            logger.info("🔄 Sync: Both legs flat. Strategy resumes in FLAT state.")
            return True

        # CASE 2: ANOMALY — naked leg. A spread strategy must never hold one side
        # without the other. This indicates a failed exit, a partial fill, or a
        # position from a different strategy that happens to share a symbol.
        if (y_qty != 0) != (x_qty != 0):
            logger.critical(
                f"🚨 SPREAD ANOMALY: Naked leg detected. "
                f"{self.leg_y}={y_qty}, {self.leg_x}={x_qty}. "
                f"A pairs strategy cannot reconcile a one-legged position."
            )
            return False

        # CASE 3: ANOMALY — both legs in the same direction. Not a hedged spread.
        if (y_qty > 0 and x_qty > 0) or (y_qty < 0 and x_qty < 0):
            logger.critical(
                f"🚨 SPREAD ANOMALY: Both legs same direction (not a hedged spread). "
                f"{self.leg_y}={y_qty}, {self.leg_x}={x_qty}."
            )
            return False

        # CASE 4: LONG SPREAD (Long Y, Short X).
        if y_qty > 0 and x_qty < 0:
            self.state['current_pos'] = 1
            self.state['held_qty_y'] = y_qty
            self.state['held_qty_x'] = x_qty
            self._log_hedge_ratio_drift(y_qty, x_qty)
            logger.info(
                f"🔄 Sync: LONG SPREAD resumed. "
                f"Long {abs(y_qty):,.0f} {self.leg_y} / Short {abs(x_qty):,.0f} {self.leg_x}"
            )
            return True

        # CASE 5: SHORT SPREAD (Short Y, Long X).
        if y_qty < 0 and x_qty > 0:
            self.state['current_pos'] = -1
            self.state['held_qty_y'] = y_qty
            self.state['held_qty_x'] = x_qty
            self._log_hedge_ratio_drift(y_qty, x_qty)
            logger.info(
                f"🔄 Sync: SHORT SPREAD resumed. "
                f"Short {abs(y_qty):,.0f} {self.leg_y} / Long {abs(x_qty):,.0f} {self.leg_x}"
            )
            return True

        # Defensive: should be unreachable given the cases above.
        logger.error("🚨 Unreachable branch in sync_positions. Treating as anomaly.")
        return False

    def _log_hedge_ratio_drift(self, y_qty: float, x_qty: float):
        """
        Warns if the held hedge ratio significantly differs from the current model β.
        Drift happens naturally (β adapts over time) or unnaturally (positions came
        from a different parameterization). Either way, exits will use ACTUAL held
        quantities — this method only surfaces visibility.
        """
        if self.state['beta'] is None or self.state['beta'] == 0:
            return

        held_ratio = abs(x_qty) / abs(y_qty)
        model_ratio = abs(self.state['beta'])
        drift_pct = abs(held_ratio - model_ratio) / model_ratio * 100

        if drift_pct > self.hedge_drift_threshold_pct:
            logger.warning(
                f"⚠️ HEDGE RATIO DRIFT: Held ratio = {held_ratio:.4f}, "
                f"Current Model β = {model_ratio:.4f} ({drift_pct:.1f}% drift). "
                f"Exit orders will use ACTUAL held quantities."
            )

    def reset(self):
        """
        Called by the Engine when a data gap is detected.

        SAFETY-CRITICAL: Clearing β and the errors buffer makes the strategy
        BLIND for ~z_lookback bars while the buffer refills (no z-score can be
        computed during warmup). During that window, exit signals are impossible.

        If we hold a live spread when this happens, we cannot manage that exposure
        until the math re-warms. That is not an acceptable risk on real capital.

        Policy:
          - If FLAT: clear arrays normally. Strategy will re-warm and resume.
          - If HOLDING: set a soft-halt flag. The engine reads this and refuses to
            process further bars / signals. We do NOT raise SystemExit from a
            deep callback — that would orphan IBKR connections and any in-flight
            orders. The engine drains cleanly and the operator decides next steps
            (manual liquidate, restart with sync, etc.).
        """
        logger.warning("Resetting Strategy State due to data gap.")

        if self.state['current_pos'] != 0:
            reason = (
                "Data gap occurred while holding live spread exposure "
                f"(current_pos={self.state['current_pos']}, "
                f"held_y={self.state['held_qty_y']}, held_x={self.state['held_qty_x']}). "
                "Kalman state cannot be safely reset without re-warmup. Halting strategy."
            )
            logger.critical(f"🛑 STRATEGY HALT: {reason}")
            self.state['halted'] = True
            self.state['halt_reason'] = reason
            # Do NOT clear arrays. Held quantities and last-known math remain
            # available for inspection / manual liquidation tooling. Engine
            # must check is_halted() before invoking on_bar() again.
            return

        # Safe path: flat, no exposure at risk.
        self.state['beta'] = None
        self.state['P'] = 1.0
        self.state['errors'].clear()
        # Clear any stale pending_transition defensively (shouldn't exist between
        # bars under normal flow, but reset() runs after a data gap where
        # ordering guarantees may have broken).
        self.state['pending_transition'] = None
        # Note: We do NOT reset held_qty_* here. A data gap doesn't change broker
        # positions; only re-sync via PortfolioManager should mutate held quantities.

    def is_halted(self) -> bool:
        """
        Engine polls this each bar to decide whether to invoke on_bar().
        Lives on the base interface so all strategies can opt into the same
        soft-halt mechanism without engine-side strategy-type checks.
        """
        return bool(self.state.get('halted', False))

    # ==========================================
    # 🔄 PENDING-TRANSITION PROTOCOL
    # ==========================================
    def commit_pending_transition(self, approved_signal: StrategySignal = None):
        """
        Apply the staged state transition.
        Called by the engine after Risk approves the signal's orders.

        ISSUE — post-Risk reconciliation: RiskManager.check() can shrink order
        quantities IN PLACE before approval. The pending_transition was staged
        from the PRE-Risk intended sizes, so committing it verbatim would record
        a held quantity larger than what we actually send — and a later exit,
        sized from held_qty_*, would then over-liquidate. For ENTRIES we instead
        derive held_qty_* from the ACTUAL approved orders (post-Risk). Exits
        flatten to zero regardless, so they keep the staged (0.0) values.

        :param approved_signal: The post-Risk signal whose orders reflect any
            resizing. If omitted, falls back to the staged values (preserves the
            simple no-resize path and backward compatibility).
        """
        pending = self.state.get('pending_transition')
        if pending is None:
            return

        new_pos = pending['current_pos']

        if approved_signal is not None and new_pos != 0:
            # ENTRY: reconcile against what was actually approved/sent.
            held_y, held_x = self._held_from_orders(approved_signal)
            self.state['held_qty_y'] = held_y
            self.state['held_qty_x'] = held_x
        else:
            # EXIT (-> flat) or no signal supplied: use the staged values.
            self.state['held_qty_y'] = pending['held_qty_y']
            self.state['held_qty_x'] = pending['held_qty_x']

        self.state['current_pos'] = new_pos
        self.state['pending_transition'] = None

        logger.debug(
            f"✅ Committed transition: current_pos={self.state['current_pos']}, "
            f"held=({self.state['held_qty_y']:.0f}, {self.state['held_qty_x']:.0f})"
        )

    def _held_from_orders(self, signal: StrategySignal) -> Tuple[float, float]:
        """
        Derives signed held quantities (leg_y, leg_x) from a signal's ACTUAL
        orders, so committed state reflects approved (possibly Risk-resized)
        sizes rather than the pre-Risk staged guess.

        Orders are matched to legs by contract object identity — the same
        contract instances from self.instruments are passed into add_order — so
        this is robust even when contracts are unqualified (localSymbol empty),
        as happens inside the event backtester.
        """
        contract_y = self.instruments.get(self.leg_y)
        contract_x = self.instruments.get(self.leg_x)

        held_y = 0.0
        held_x = 0.0
        for o in signal.orders:
            signed = float(o['qty']) if o['action'].upper() == 'BUY' else -float(o['qty'])
            if o.get('contract') is contract_y:
                held_y = signed
            elif o.get('contract') is contract_x:
                held_x = signed
        return held_y, held_x

    def rollback_pending_transition(self):
        """
        Discard the staged state transition.
        Called by the engine after Risk rejects the signal's orders.
        State remains at whatever it was before the rejected on_bar.
        """
        pending = self.state.get('pending_transition')
        if pending is None:
            return

        self.state['pending_transition'] = None
        logger.warning(
            f"🔄 Rolled back transition: would have moved from "
            f"current_pos={pending['from_pos']} to {pending['current_pos']}. "
            f"State remains at current_pos={self.state['current_pos']}, "
            f"held=({self.state['held_qty_y']:.0f}, {self.state['held_qty_x']:.0f})."
        )

    # ==========================================
    # 📋 TELEMETRY HELPERS
    # ==========================================
    def _build_meta(self, z: float = None) -> dict:
        """
        Returns the full strategy diagnostic state as a JSON-serialisable dict.

        Called from on_bar() on every return path so the telemetry layer can
        persist uniform decision rows for every bar — including warmup, invalid-
        price, and low-volatility bars where z is not yet computable. This makes
        the live decisions stream directly comparable with the backtester's
        equivalent stream during parity-checks.

        Includes pending_transition fields so a parity harness can detect
        bars where the strategy proposed a transition that was rejected by Risk
        (current_pos unchanged, but pending_current_pos was set).
        """
        pending = self.state.get('pending_transition')
        return {
            'z':                   float(z) if z is not None and not pd.isna(z) else None,
            'beta':                float(self.state['beta']) if self.state['beta'] is not None else None,
            'P':                   float(self.state['P']),
            'current_pos':         int(self.state['current_pos']),
            'errors_buffer':       len(self.state['errors']),
            'leg_y':               self.leg_y,
            'leg_x':               self.leg_x,
            'halted':              bool(self.state.get('halted', False)),
            'halt_reason':         self.state.get('halt_reason'),
            'pending_current_pos': pending['current_pos']  if pending else None,
            'pending_held_qty_y':  pending['held_qty_y']   if pending else None,
            'pending_held_qty_x':  pending['held_qty_x']   if pending else None,
        }

    # ==========================================
    # 🔬 RESEARCH & BACKTESTING (Matrix Dictionary Input)
    # ==========================================
    def generate_signals(self, data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Calculates target weights for both legs across the historical DataFrame."""
        close_matrix = data.get('close')
        if close_matrix is None:
            raise ValueError("generate_signals requires a 'close' matrix in the data dictionary.")

        y = close_matrix[self.leg_y].values
        x = close_matrix[self.leg_x].values
        n = len(y)

        # Initialize State Variables
        beta = np.zeros(n)
        beta[0] = y[0] / x[0]
        P = np.zeros(n)
        P[0] = 1.0
        e = np.zeros(n)
        Q = np.zeros(n)
        wt = self.delta / (1 - self.delta)

        # Vectorized Kalman Filter Pass
        for t in range(1, n):
            beta_hat = beta[t-1]
            P_hat = P[t-1] + wt
            e[t] = y[t] - (beta_hat * x[t])
            Q[t] = P_hat * (x[t]**2) + self.vt
            K = P_hat * x[t] / Q[t]
            beta[t] = beta_hat + K * e[t]
            P[t] = P_hat * (1 - K * x[t])

        # Dynamic Z-Scoring
        err_series = pd.Series(e)
        rolling_mean = err_series.rolling(window=self.z_lookback).mean()
        rolling_std = err_series.rolling(window=self.z_lookback).std()

        z_scores = np.where(rolling_std > 1e-8, (err_series - rolling_mean) / rolling_std, 0)

        # Generate State-Machine Signals
        signal_y = np.zeros(n)
        current_pos = 0

        for t in range(n):
            z = z_scores[t]
            if pd.isna(z) or t < self.z_lookback:
                continue

            if current_pos == 0:
                if z < -self.entry_z:
                    current_pos = 1
                elif z > self.entry_z:
                    current_pos = -1
            elif current_pos == 1:
                if z >= self.exit_z:
                    current_pos = 0
            elif current_pos == -1:
                if z <= -self.exit_z:
                    current_pos = 0

            signal_y[t] = current_pos

        # Construct Allocation Weights
        weights = pd.DataFrame(index=close_matrix.index)
        raw_weight_y = signal_y
        raw_weight_x = -signal_y * beta

        gross_exposure = np.abs(raw_weight_y) + np.abs(raw_weight_x)
        safe_exposure = np.where(gross_exposure > 0, gross_exposure, 1.0)

        weights[self.leg_y] = raw_weight_y / safe_exposure
        weights[self.leg_x] = raw_weight_x / safe_exposure

        return weights.fillna(0.0)

    # ==========================================
    # 🏭 LIVE EXECUTION / EVENT-DRIVEN (Cross-Sectional DF Input)
    # ==========================================
    def on_bar(self, latest_bars: pd.DataFrame) -> StrategySignal:
        """
        Executes the recursive 1D Kalman Filter on a single new cross-sectional bar.

        Every return path attaches the strategy's full diagnostic state via
        _build_meta(), so the telemetry layer can persist a uniform decision row
        on every bar — including warmup/invalid bars where z is not computable.

        State transitions (current_pos / held_qty_*) are STAGED into
        pending_transition rather than mutated directly. The engine commits the
        staged state after Risk approval, or rolls it back on rejection — see
        the PENDING-TRANSITION PROTOCOL section above.
        """
        # Defence in depth: even if the engine forgets to gate on is_halted(),
        # never emit a trading signal from a halted state.
        if self.state.get('halted'):
            return StrategySignal(signal_type="HALTED", meta=self._build_meta())

        if self.leg_y not in latest_bars.index or self.leg_x not in latest_bars.index:
            return StrategySignal(signal_type="AWAITING_DATA", meta=self._build_meta())

        y_t = latest_bars.loc[self.leg_y, 'close']
        x_t = latest_bars.loc[self.leg_x, 'close']

        # A NaN close on either leg means that leg produced no bar this minute
        # (DataManager now writes an explicit NaN row for tickless legs.
        # Reject the incomplete cross-section rather than compute a
        # spread from one fresh and one stale leg.
        if pd.isna(y_t) or pd.isna(x_t) or y_t <= 0 or x_t <= 0:
            return StrategySignal(signal_type="INVALID_PRICE", meta=self._build_meta())

        # 1. Warm-Up Initialization (If not primed via Engine)
        if self.state['beta'] is None:
            self.state['beta'] = y_t / x_t
            logger.info(f"Cold-Start Initialized Kalman Beta: {self.state['beta']:.4f}")
            return StrategySignal(signal_type="WARMUP_BETA", meta=self._build_meta())

        # 2. Kalman Filter Recursion
        beta_hat = self.state['beta']
        wt = self.delta / (1 - self.delta)
        P_hat = self.state['P'] + wt

        e_t = y_t - (beta_hat * x_t)
        Q_t = P_hat * (x_t**2) + self.vt
        K = P_hat * x_t / Q_t

        self.state['beta'] = beta_hat + K * e_t
        self.state['P'] = P_hat * (1 - K * x_t)

        # 3. Dynamic Z-Scoring
        self.state['errors'].append(e_t)

        if len(self.state['errors']) < self.z_lookback:
            return StrategySignal(signal_type="WARMUP_ZSCORE", meta=self._build_meta())

        errors_array = np.array(self.state['errors'])
        std_e = np.std(errors_array)

        if std_e < 1e-8:
            return StrategySignal(signal_type="LOW_VOLATILITY", meta=self._build_meta())

        mean_e = np.mean(errors_array)
        z = (e_t - mean_e) / std_e

        # 4. State Machine Logic
        current_pos = self.state['current_pos']
        new_pos = current_pos
        signal = StrategySignal(signal_type="FLAT", meta=self._build_meta(z=z))

        if current_pos == 0:
            if z < -self.entry_z:
                new_pos = 1   # Long Spread
            elif z > self.entry_z:
                new_pos = -1  # Short Spread
        elif current_pos == 1 and z >= self.exit_z:
            new_pos = 0       # Flatten Long
        elif current_pos == -1 and z <= -self.exit_z:
            new_pos = 0       # Flatten Short

        # 5. Order Generation
        if new_pos != current_pos:
            signal = self._generate_orders(new_pos, current_pos, signal, y_t, x_t)
            # State transition is STAGED in self.state['pending_transition'] by
            # _generate_orders. We do NOT mutate current_pos here. The engine
            # calls commit_pending_transition() after Risk approval (or
            # rollback_pending_transition() on rejection).
            #
            # Refresh meta AFTER staging so it reflects the new pending_* fields.
            signal.meta = self._build_meta(z=z)

        return signal

    def _generate_orders(self, new_pos: int, old_pos: int, base_signal: StrategySignal,
                         price_y: float, price_x: float) -> StrategySignal:
        """
        Translates a state transition into explicit IBKR orders.

        Entries are sized using base_qty and the current β.
        Exits are sized using the ACTUAL broker-confirmed held quantities so we
        flatten cleanly regardless of β drift between entry and exit.

        Signal-type labelling (Issue 7): flatten transitions are labelled
        EXIT_<old>_TO_0 and entries ENTRY_<old>_TO_<new>. RiskManager keys its
        entry-sizing bypass AND its kill-switch liquidation allowance off the
        substring "EXIT", so a flatten must carry that token to be treated as a
        liquidation rather than misclassified as a fresh entry.

        STAGES the resulting state into self.state['pending_transition'] rather
        than mutating current_pos / held_qty_* directly. The actual mutation
        happens in commit_pending_transition() after Risk approval.
        """
        if new_pos == 0:
            base_signal.signal_type = f"EXIT_{old_pos}_TO_0"
        else:
            base_signal.signal_type = f"ENTRY_{old_pos}_TO_{new_pos}"

        contract_y = self.instruments.get(self.leg_y)
        contract_x = self.instruments.get(self.leg_x)

        if not contract_y or not contract_x:
            logger.error("Contracts not found in instruments mapping!")
            return base_signal

        # --- EXIT PATH: Use actual held quantities ---
        if new_pos == 0:
            qty_y = int(abs(self.state['held_qty_y']))
            qty_x = int(abs(self.state['held_qty_x']))

            if qty_y == 0 or qty_x == 0:
                logger.error(
                    f"🚨 EXIT REQUESTED but held quantities are zero. "
                    f"State desync detected. held_y={qty_y}, held_x={qty_x}"
                )
                return base_signal

            if old_pos == 1:  # Was Long Spread -> Sell Y, Buy X
                base_signal.add_order(contract_y, action='SELL', qty=qty_y, estimated_price=price_y)
                base_signal.add_order(contract_x, action='BUY', qty=qty_x, estimated_price=price_x)
            elif old_pos == -1:  # Was Short Spread -> Buy Y, Sell X
                base_signal.add_order(contract_y, action='BUY', qty=qty_y, estimated_price=price_y)
                base_signal.add_order(contract_x, action='SELL', qty=qty_x, estimated_price=price_x)

            # STAGE the exit transition. Actual clearing of held_qty_* happens
            # in commit_pending_transition() after Risk approval.
            self.state['pending_transition'] = {
                'current_pos': 0,
                'held_qty_y':  0.0,
                'held_qty_x':  0.0,
                'from_pos':    old_pos,
            }
            return base_signal

        # --- ENTRY PATH: Use base_qty and current β ---
        qty_y = int(abs(self.base_qty))
        qty_x = int(abs(self.base_qty * self.state['beta']))

        # Issue guard: a vanishing β (or a tiny base_qty) can round qty_x to
        # 0, which would fire a one-legged spread (zero-qty order rejected by
        # IBKR; the other leg left naked). Reject the whole entry if either leg
        # is below the minimum tradable size. No transition is staged, so the
        # strategy stays flat and the next bar re-evaluates cleanly.
        if qty_y < self.min_order_qty or qty_x < self.min_order_qty:
            logger.error(
                f"🚨 ENTRY REJECTED: leg size below minimum tradable "
                f"({self.min_order_qty}). qty_y={qty_y}, qty_x={qty_x}, "
                f"beta={self.state['beta']:.6f}. No orders staged; strategy stays flat."
            )
            base_signal.signal_type = f"ENTRY_REJECTED_MINQTY_{old_pos}_TO_{new_pos}"
            return base_signal

        if new_pos == 1:  # Long Spread: Buy Y, Sell X
            base_signal.add_order(contract_y, action='BUY', qty=qty_y, estimated_price=price_y)
            base_signal.add_order(contract_x, action='SELL', qty=qty_x, estimated_price=price_x)
            self.state['pending_transition'] = {
                'current_pos': 1,
                'held_qty_y':  float(qty_y),
                'held_qty_x':  float(-qty_x),
                'from_pos':    old_pos,
            }

        elif new_pos == -1:  # Short Spread: Sell Y, Buy X
            base_signal.add_order(contract_y, action='SELL', qty=qty_y, estimated_price=price_y)
            base_signal.add_order(contract_x, action='BUY', qty=qty_x, estimated_price=price_x)
            self.state['pending_transition'] = {
                'current_pos': -1,
                'held_qty_y':  float(-qty_y),
                'held_qty_x':  float(qty_x),
                'from_pos':    old_pos,
            }

        return base_signal