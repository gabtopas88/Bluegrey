from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd
from ib_async import Contract
import logging

logger = logging.getLogger(__name__)

class StrategySignal:
    """
    Standardized Trade Instruction Container.
    Decouples 'Logic' (The Strategy) from 'Execution' (The Broker Adapter).
    """
    def __init__(self, signal_type: str):
        self.signal_type = signal_type  # e.g., "ENTRY_LONG", "EXIT_ALL", "HEARTBEAT"
        self.orders: List[Dict] = []    # List of execution dictionaries
        self.meta: Dict = {}            # Telemetry (Z-Scores, Beta, etc.) for the Dashboard

    def add_order(self, contract: Contract, action: str, qty: float, order_type: str = 'MKT', limit_price: float = 0.0):
        """
        Adds a concrete execution instruction to this signal.
        """
        order = {
            'contract': contract,
            'action': action,
            'qty': qty,
            'type': order_type
        }
        if order_type == 'LMT':
            order['lmtPrice'] = limit_price
            
        self.orders.append(order)

    def __repr__(self):
        return f"<Signal: {self.signal_type} | Orders: {len(self.orders)}>"


class BaseStrategy(ABC):
    """
    The Abstract Base Class for all Bluegrey strategies.
    
    Enforces a strict lifecycle:
    1. Init (Configuration)
    2. Warmup (Data buffering)
    3. Loop (On_Tick / On_Bar)
    4. Safety (Reset on Gaps)
    5. Persistence (Save/Load State)
    """
    def __init__(self, instruments: Dict[str, Contract], params: Dict[str, Any]):
        self.instruments = instruments  # {'ASSET_A': Contract(...), ...}
        self.params = params
        
        # Standardize 'Warmup' logic
        # Strategies shouldn't trade until they have seen X bars of history
        self.warmup_period = params.get('warmup_period', 0)
        self.is_ready = False 

    @abstractmethod
    def on_tick(self, prices: pd.Series) -> Optional[StrategySignal]:
        """
        FAST LOOP: Fired on every incoming price update.
        Use for: Stop-losses, Take-profits, or Entry Trigger checks.
        
        input: prices -> pd.Series with index as instrument keys (e.g. 'ASSET_A')
        """
        pass

    def on_bar(self, bars: pd.DataFrame) -> Optional[StrategySignal]:
        """
        SLOW LOOP: Fired when a 1-minute (or n-minute) candle closes.
        Use for: Updating Math (Kalman Filters, EMAs, Z-Scores).
        
        input: bars -> DataFrame with columns like ('ASSET_A', 'close')
        """
        return None

    @abstractmethod
    def reset(self):
        """
        CRITICAL: Gap Handling.
        Called by DataHandler when a time-gap > threshold is detected.
        Must clear internal buffers (history, covariances) to prevent 'teleporting' math.
        
        Example:
        self.kalman_cov = np.ones((2,2)) # Reset uncertainty to max
        """
        pass

    # --- PERSISTENCE (For Live Trading Resilience) ---
    
    def get_state(self) -> Dict[str, Any]:
        """
        Exports the strategy's internal brain.
        Used to save state to disk/DB in case of crash.
        """
        return {}

    def restore_state(self, state: Dict[str, Any]):
        """
        Imports a saved brain.
        Used to restart the strategy without re-learning from zero.
        """
        pass