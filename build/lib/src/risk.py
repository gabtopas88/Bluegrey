import logging

logger = logging.getLogger()

class RiskManager:
    def __init__(self):
        self.max_orders_per_minute = 5
        self.order_count = 0
        self.last_reset_time = 0
        self.forbidden_hours = [4, 5] # Example: Don't trade at 4 AM
    
    def check(self, signal):
        """
        Returns True if the trade is SAFE.
        Returns False if the trade is DANGEROUS.
        """
        if signal is None or signal == "HOLD":
            return False
            
        # Example Check: Is it a valid signal string?
        allowed_signals = ["LONG_SPREAD", "SHORT_SPREAD", "EXIT"]
        if signal not in allowed_signals:
            logger.warning(f"🛡️ RISK BLOCK: Unknown signal '{signal}'")
            return False
            
        # (In Phase 5 we add PnL limits here)
        return True