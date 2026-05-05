"""
strategies/base.py
The immutable contract for all Alpha models across Research and Production.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Union
import pandas as pd

@dataclass
class StrategySignal:
    """
    The universal language between the Strategy and the Live/Event Engine.
    Enforces a strict structure so execution never crashes.
    """
    signal_type: str
    orders: List[Dict[str, Any]] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

    def add_order(self, contract, action: str, qty: int, order_type: str = 'MKT', estimated_price: float = 1.0, volatility: float = None):
        """
        Helper method to guarantee all orders have the exact keys required by the Risk Manager.
        """
        symbol = contract.localSymbol if hasattr(contract, 'localSymbol') else str(contract)
        
        self.orders.append({
            'symbol': symbol,
            'contract': contract,
            'action': action.upper(),
            'qty': qty,
            'type': order_type.upper(),
            'estimated_price': estimated_price,
            'volatility': volatility
        })

class BaseStrategy:
    """
    All future strategies MUST inherit from this class.
    Enforces dual-compatibility for both Vectorized Research (Portfolio/Matrix) and Live Execution.
    """
    def __init__(self, instruments: dict, params: dict):
        self.instruments = instruments
        self.params = params if params is not None else {}

    # ==========================================
    # 🔬 RESEARCH INTERFACE (For the Optimizer)
    # ==========================================
    def generate_signals(self, data: Union[pd.DataFrame, Dict[str, pd.DataFrame]]) -> pd.DataFrame:
        """
        Calculates target weights across the entire historical dataset.
        MUST be implemented for CPCV and Portfolio Vector Backtesting.
        
        :param data: Can be a MultiIndex DataFrame (Date x Symbol -> Features) 
                     or a Dictionary of 2D feature matrices (e.g., data['close'], data['volume']).
        :return: pandas DataFrame of target weights (-1.0 to 1.0). 
                 Index = Datetime, Columns = Tickers.
        """
        raise NotImplementedError("Strategy must implement generate_signals() returning an N-dimensional DataFrame.")

    # ==========================================
    # 🏭 PRODUCTION INTERFACE (For Live/Event)
    # ==========================================
    def on_tick(self, market_snapshot: dict) -> StrategySignal:
        """Called by the Engine on raw price updates."""
        return StrategySignal(signal_type="NONE")

    def on_bar(self, latest_bars: dict) -> StrategySignal:
        """
        Called by the Engine when a new bar closes.
        In a multi-asset setup, 'latest_bars' should be a dictionary mapping 
        symbols to their most recently closed OHLCV data.
        """
        return StrategySignal(signal_type="NONE")

    def reset(self):
        """Called when a data gap is detected to clear strategy memory/state."""
        pass