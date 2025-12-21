from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd
from ib_async import Contract

class StrategySignal:
    """
    A container for trading instructions.
    Crucially, it carries the specific CONTRACT object to trade.
    """
    def __init__(self, signal_type: str):
        self.signal_type = signal_type  # e.g., "ENTRY_LONG"
        self.orders = []                # List of execution instructions
        self.meta = {}                  # For dashboards (indicators, etc.)

    def add_order(self, contract: Contract, action: str, qty: float, order_type: str = 'MKT'):
        """
        Adds a concrete execution instruction to this signal.

        contract: The IBKR Contract object (Stock, Future, etc.)
        action: 'BUY' or 'SELL'
        qty: Amount
        order_type: 'MKT' or 'LMT'
        """
        self.orders.append({
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
        Output: MUST return a StrategySignal every tick. 
        If nothing to do, return signal_type="HOLD" with empty orders.
        """
        pass