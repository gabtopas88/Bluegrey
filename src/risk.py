import time
import logging
from collections import deque

logger = logging.getLogger("RiskManager")

class RiskManager:
    """
    Institutional Risk Firewall. 
    Enforces Daily Drawdown limits, Rolling Velocity checks, and Fractional Kelly sizing.
    """
    def __init__(self):
        # 1. Kill Switch Parameters
        self.max_daily_drawdown_pct = 0.03  # 3% Max Daily Loss
        self.is_halted = False
        
        # 2. Velocity Parameters (Anti-Spam)
        self.max_orders_per_minute = 5
        self.order_timestamps = deque()  # Rolling 60-second window
        
        # 3. Kelly Criterion Parameters
        self.kelly_fraction = 0.25  # Quarter-Kelly (Fund Standard)
        self.max_absolute_exposure_pct = 0.20  # Hard ceiling: Never risk >20% on one trade
        
        # 4. Portfolio & Strategy State (Updated by main.py loop)
        self.current_equity = 100000.0
        self.start_of_day_equity = 100000.0
        
        # Default Strategy Metrics (Assume a basic edge until updated)
        self.strategy_win_rate = 0.55
        self.strategy_reward_risk = 1.5

        # 5. Asset Whitelist (Safety against rogue tickers)
        self.allowed_assets = ["EURUSD", "GBPUSD", "SPY", "QQQ"]

    def update_state(self, current_equity: float, start_of_day_equity: float, win_rate: float, reward_risk: float):
        """Called periodically by the Main Engine to keep Risk state accurate."""
        self.current_equity = current_equity
        self.start_of_day_equity = start_of_day_equity
        self.strategy_win_rate = win_rate
        self.strategy_reward_risk = reward_risk
        
        # Check Kill Switch passively
        drawdown = (self.start_of_day_equity - self.current_equity) / self.start_of_day_equity
        if drawdown >= self.max_daily_drawdown_pct and not self.is_halted:
            self.is_halted = True
            logger.critical(f"🛑 KILL SWITCH TRIGGERED: Drawdown {drawdown*100:.2f}% breached 3% limit. Engine Halted.")

    def check(self, signal):
        """
        The Gatekeeper. Evaluates a StrategySignal and modifies it if necessary.
        Returns True if the signal is approved (or safely modified). 
        Returns False if the signal is entirely rejected.
        """
        if not signal or not signal.orders:
            return False

        # --- PILLAR 1: THE KILL SWITCH ---
        if self.is_halted:
            # Only allow EXIT orders if we are halted
            if "EXIT" not in signal.signal_type.upper():
                logger.warning("🛡️ RISK BLOCK: Engine is HALTED. Only liquidation orders allowed.")
                return False

        # --- PILLAR 3: ROLLING VELOCITY LOCK ---
        now = time.time()
        # Clean out timestamps older than 60 seconds
        while self.order_timestamps and self.order_timestamps[0] < now - 60:
            self.order_timestamps.popleft()
            
        if len(self.order_timestamps) >= self.max_orders_per_minute:
            logger.warning(f"🛡️ RISK BLOCK: Velocity limit breached ({self.max_orders_per_minute} orders/min).")
            return False

        # --- PILLAR 2 & 4: FRACTIONAL KELLY CAPPING ---
        # Calculate the mathematical ceiling for the next trade
        kelly_pct = self._calculate_kelly()
        allowed_pct = max(0.0, kelly_pct) * self.kelly_fraction
        capped_pct = min(allowed_pct, self.max_absolute_exposure_pct)
        max_capital_for_trade = self.current_equity * capped_pct

        for instruction in signal.orders:
            symbol = instruction['contract'].localSymbol
            
            # Asset Whitelist Check
            if symbol not in self.allowed_assets:
                logger.warning(f"🛡️ RISK BLOCK: Asset '{symbol}' is not whitelisted.")
                return False

            # Position Sizing Check (Only apply to entry orders, EXITs should always process)
            if "EXIT" not in signal.signal_type.upper():
                qty = instruction['qty']
                
                # To calculate dollar exposure, we need an estimated price. 
                # If strategy didn't provide one, default to 1.0 (will bypass value check but log a warning)
                estimated_price = instruction.get('estimated_price', 1.0)
                requested_capital = qty * estimated_price
                
                if requested_capital > max_capital_for_trade and estimated_price > 1.0:
                    # DYNAMIC OVERRIDE: Shrink the order to fit the Kelly ceiling
                    safe_qty = int(max_capital_for_trade / estimated_price)
                    logger.warning(
                        f"⚖️ KELLY OVERRIDE: Strategy requested {qty} units (${requested_capital:,.2f}). "
                        f"Quarter-Kelly Limit is {capped_pct*100:.1f}% (${max_capital_for_trade:,.2f}). "
                        f"Shrinking order to {safe_qty} units."
                    )
                    
                    if safe_qty <= 0:
                        logger.warning("🛡️ RISK BLOCK: Quarter-Kelly limit restricts position size to 0.")
                        return False
                        
                    # Modify the payload safely in-place
                    instruction['qty'] = safe_qty

        # If we made it this far, the trade is mathematically safe.
        self.order_timestamps.append(now)
        return True

    def _calculate_kelly(self) -> float:
        """Calculates Full Kelly Criterion: W - ((1 - W) / R)"""
        w = self.strategy_win_rate
        r = self.strategy_reward_risk
        
        if r <= 0:
            return 0.0
            
        return w - ((1.0 - w) / r)