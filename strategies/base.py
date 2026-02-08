from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd
from ib_async import Contract

class StrategySignal:
    """
    A container for trading instructions.
    Carries both the Broker Object (Contract) and the Internal ID (key).
    """
    def __init__(self, signal_type: str):
        self.signal_type = signal_type  # e.g., "ENTRY_LONG"
        self.orders = []                # List of execution instructions
        self.meta = {}                  # For dashboards (z-scores, betas, etc.)

    def add_order(self, key: str, contract: Contract, action: str, qty: float, order_type: str = 'MKT'):
        """
        Adds a concrete execution instruction.
        
        key: The internal configuration key (e.g., 'MSFT_STK'). CRITICAL for Backtesting.
        contract: The IBKR Contract object. CRITICAL for Live Execution.
        """
        self.orders.append({
            'key': key,          # <--- ADDED THIS
            'contract': contract,
            'action': action,
            'qty': qty,
            'type': order_type
        })

class BaseStrategy(ABC):
    def __init__(self, instruments: Dict[str, Contract], params: Dict[str, Any]):
        self.instruments = instruments # Dictionary of {'Key': Contract}
        self.params = params

    @abstractmethod
    def on_tick(self, data: pd.DataFrame) -> StrategySignal:
        """
        The Core Logic Loop.
        Input: A DataFrame where columns are the Keys defined in INSTRUMENTS.
        Output: MUST return a StrategySignal (or None).
        """
        pass