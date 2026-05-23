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

    def add_order(self, contract: Any, action: str, qty: int, order_type: str = 'MKT', estimated_price: float = 1.0, volatility: float = None):
        """
        Helper method to guarantee all orders have the exact keys required by the Risk Manager.
        'contract' is kept as Any to keep this layer agnostic to IBKR's specific object classes.
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
    # ⚙️ LIFECYCLE INTERFACE (Engine Communication)
    # ==========================================
    def get_warmup_lookback(self) -> int:
        """
        Returns the number of historical bars required to prime the strategy before live trading.
        """
        return 0 # Default: No warmup required

    def prime_state(self, historical_data: Dict[str, pd.DataFrame]):
        """
        Receives historical data structured as a dictionary of matrices (e.g. {'close': df}).
        Override this in stateful strategies to fast-forward internal memory.
        """
        pass # Default: Do nothing

    # ==========================================
    # 🔬 RESEARCH INTERFACE (For the Optimizer)
    # ==========================================
    def generate_signals(self, data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Calculates target weights across the entire historical dataset.
        MUST be implemented for CPCV and Portfolio Vector Backtesting.

        :param data: A Dictionary of 2D feature matrices (e.g., data['close'] is Time x Tickers).
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

    def on_bar(self, latest_bars: pd.DataFrame) -> StrategySignal:
        """
        Called by the Engine when a new bar closes.
        latest_bars is a cross-sectional DataFrame representing the current timeframe's closed bar (Index: Tickers, Columns: OHLCV).
        """
        return StrategySignal(signal_type="NONE")

    def reset(self):
        """Called when a data gap is detected to clear strategy memory/state."""
        pass