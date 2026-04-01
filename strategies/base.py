from dataclasses import dataclass, field
from typing import List, Dict, Any

@dataclass
class StrategySignal:
    """
    The universal language between the Strategy and the Engine.
    Enforces a strict structure so execution and backtesting never crash.
    """
    signal_type: str
    orders: List[Dict[str, Any]] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

    def add_order(self, contract, action: str, qty: int, order_type: str = 'MKT', estimated_price: float = 1.0):
        """
        Helper method to guarantee all orders have the exact keys required by the Risk Manager.
        """
        # Safely extract the symbol whether it's an IBKR Contract or a backtest string
        symbol = contract.localSymbol if hasattr(contract, 'localSymbol') else str(contract)
        
        self.orders.append({
            'symbol': symbol,
            'contract': contract,
            'action': action.upper(),
            'qty': qty,
            'type': order_type.upper(),
            'estimated_price': estimated_price
        })

class BaseStrategy:
    """
    All future strategies MUST inherit from this class.
    It guarantees the Engine can call these methods safely.
    """
    def __init__(self, instruments: dict, params: dict):
        self.instruments = instruments
        self.params = params

    def on_tick(self, market_snapshot: dict) -> StrategySignal:
        """Called by the Engine on raw price updates."""
        return StrategySignal(signal_type="NONE")

    def on_bar(self, latest_bars) -> StrategySignal:
        """Called by the Engine when a new 1-minute OHLCV bar closes."""
        return StrategySignal(signal_type="NONE")

    def reset(self):
        """Called when a data gap is detected to clear strategy memory."""
        pass