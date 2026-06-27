import time
import logging
from collections import deque
from typing import Optional, Dict, Any

logger = logging.getLogger("RiskManager")

class RiskManager:
    """
    Institutional Risk Firewall.
    Enforces Daily Drawdown limits, Rolling Velocity checks, and Vol-Targeted Sizing.
    """
    def __init__(self, instruments: Optional[Dict[str, Any]] = None):
        """
        :param instruments: The same {universe_key: Contract} dict used by every
            other engine component. The symbol whitelist is derived from these
            contracts' localSymbol at check-time (after IBKR qualification has
            populated them), instead of being hardcoded. This eliminates the
            three-way format drift that previously made the whitelist dead code.
        """
        # 1. Kill Switch Parameters
        self.max_daily_drawdown_pct = 0.03  # 3% Max Daily Loss
        self.is_halted = False

        # 2. Velocity Parameters (Anti-Spam)
        self.max_orders_per_minute = 5
        self.order_timestamps = deque()  # Rolling 60-second window

        # 3. Vol-Targeting & Exposure Parameters
        self.target_trade_risk_pct = 0.01      # Risk exactly 1% of account equity per trade
        self.max_absolute_exposure_pct = 0.10  # Hard ceiling: Never allocate >10% of capital to one trade

        # 4. Portfolio State (Updated by main.py / event_backtester.py)
        # NOTE: main.py currently passes static dummy values; the drawdown
        # kill switch is effectively dormant until real equity is wired in.
        self.current_equity = 100000.0
        self.start_of_day_equity = 100000.0

        # 5. Universe reference for the symbol whitelist.
        # Contracts are passed by reference; ib_async populates localSymbol
        # in-place during qualifyContracts(). By the time check() runs in the
        # event loop, every contract here has been qualified and localSymbol
        # is set. We resolve the whitelist lazily in _allowed_local_symbols()
        # rather than caching at init time, so we always reflect the current
        # state of the qualified contracts.
        self.instruments = instruments or {}
        if not self.instruments:
            logger.warning(
                "⚠️ RiskManager constructed without instruments. "
                "Symbol whitelist will be empty and every order rejected."
            )

    def _allowed_local_symbols(self) -> set:
        """
        Derives the accepted set of order symbols from the universe.

        StrategySignal.add_order() stores contract.localSymbol as the order's
        'symbol' field, so the whitelist must contain those exact strings (e.g.
        'AUD.USD' for FX after qualification — NOT 'AUDUSD' or 'C:AUDUSD').
        """
        return {
            c.localSymbol
            for c in self.instruments.values()
            if getattr(c, 'localSymbol', None)
        }

    def update_state(self, current_equity: float, start_of_day_equity: float):
        """Called periodically by the Engine to keep Risk state accurate."""
        self.current_equity = current_equity
        self.start_of_day_equity = start_of_day_equity

        # Check Kill Switch passively
        drawdown = (self.start_of_day_equity - self.current_equity) / self.start_of_day_equity
        if drawdown >= self.max_daily_drawdown_pct and not self.is_halted:
            self.is_halted = True
            logger.critical(f"🛑 KILL SWITCH TRIGGERED: Drawdown {drawdown*100:.2f}% breached 3% limit. Engine Halted.")

    def check(self, signal, current_time: float = None):
        """
        The Gatekeeper. Evaluates a StrategySignal and shrinks order sizes
        dynamically based on current market volatility.
        """
        if not signal or not signal.orders:
            return False

        # --- PILLAR 1: THE KILL SWITCH ---
        if self.is_halted:
            if "EXIT" not in signal.signal_type.upper():
                logger.warning("🛡️ RISK BLOCK: Engine is HALTED. Only liquidation orders allowed.")
                return False

        # --- PILLAR 2: ROLLING VELOCITY LOCK ---
        now = current_time if current_time else time.time()
        while self.order_timestamps and self.order_timestamps[0] < now - 60:
            self.order_timestamps.popleft()

        if len(self.order_timestamps) >= self.max_orders_per_minute:
            logger.warning(f"🛡️ RISK BLOCK: Velocity limit breached ({self.max_orders_per_minute} orders/min).")
            return False

        # --- PILLAR 3: VOLATILITY TARGETING & HARD CAPS ---
        # The maximum dollar amount we are allowed to lose on this specific trade
        dollar_risk_budget = self.current_equity * self.target_trade_risk_pct
        # The absolute maximum capital we are allowed to deploy (regardless of leverage)
        max_capital_budget = self.current_equity * self.max_absolute_exposure_pct

        # Resolve the universe whitelist once per check (cheap; tiny set).
        allowed_local_symbols = self._allowed_local_symbols()

        for instruction in signal.orders:
            symbol = instruction.get('symbol', 'UNKNOWN')

            # Whitelist Check: data-driven from config.INSTRUMENTS, so the
            # universe is the single source of truth. No hardcoded list to
            # drift from the actual strategy in use.
            if symbol not in allowed_local_symbols:
                logger.warning(
                    f"🛡️ RISK BLOCK: Symbol '{symbol}' is not in the configured "
                    f"universe. Allowed: {sorted(allowed_local_symbols) or '(empty)'}."
                )
                return False

            # Sizing logic only applies to entries, not exits
            if "EXIT" not in signal.signal_type.upper():
                requested_qty = instruction['qty']
                est_price = instruction.get('estimated_price', 1.0)

                # Volatility is the expected absolute price move (e.g., Average True Range)
                # If strategy doesn't provide it, we conservatively assume a 1% price shock
                asset_volatility = instruction.get('volatility')
                if not asset_volatility or asset_volatility <= 0:
                    asset_volatility = est_price * 0.01
                    logger.warning(f"⚠️ Risk: No volatility provided for {symbol}. Assuming 1% shock (${asset_volatility:.2f}).")

                # Constraint 1: Maximum shares based on Volatility (Risk Parity)
                # If asset moves 1 ATR against us, we only lose our dollar_risk_budget
                max_shares_vol = int(dollar_risk_budget / asset_volatility)

                # Constraint 2: Maximum shares based on Hard Capital Limits
                max_shares_cap = int(max_capital_budget / est_price)

                # Find the safest (smallest) allowed size
                safe_qty = min(requested_qty, max_shares_vol, max_shares_cap)

                if safe_qty < requested_qty:
                    logger.warning(
                        f"⚖️ VOL-TARGET OVERRIDE: {symbol} requested {requested_qty} units. "
                        f"Vol Limit: {max_shares_vol} | Cap Limit: {max_shares_cap}. "
                        f"Shrinking order to {safe_qty} units."
                    )

                if safe_qty <= 0:
                    logger.warning(f"🛡️ RISK BLOCK: Volatility is too high. Minimum safe position is 0.")
                    return False

                # Modify payload in place
                instruction['qty'] = safe_qty

        # Log execution time and approve
        self.order_timestamps.append(now)
        return True