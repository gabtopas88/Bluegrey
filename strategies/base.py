"""
strategies/base.py
The immutable contract for all Alpha models across Research and Production.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, TYPE_CHECKING
import pandas as pd

if TYPE_CHECKING:
    # Avoid circular import at runtime; PortfolioManager imports BaseStrategy too.
    from src.portfolio import LivePosition


@dataclass
class StrategySignal:
    """
    The universal language between the Strategy and the Live/Event Engine.
    Enforces a strict structure so execution never crashes.
    """
    signal_type: str
    orders: List[Dict[str, Any]] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

    def add_order(self, contract: Any, action: str, qty: int, order_type: str = 'MKT',
                  estimated_price: float = 1.0, volatility: float = None):
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
        return 0  # Default: No warmup required

    def prime_state(self, historical_data: Dict[str, pd.DataFrame]):
        """
        Receives historical data structured as a dictionary of matrices (e.g. {'close': df}).
        Override this in stateful strategies to fast-forward internal memory.
        """
        pass  # Default: Do nothing

    def sync_positions(self, positions: Dict[str, 'LivePosition']) -> bool:
        """
        Called by PortfolioManager at engine boot AFTER prime_state.
        Reconciles broker-confirmed positions against the strategy's internal state machine.

        :param positions: Dict of {symbol_key: LivePosition}. Empty dict means flat.
        :return: True if positions map cleanly to a valid internal state.
                 False if positions are anomalous and the engine should halt or liquidate.

        Default behavior:
          - Empty positions (flat) is always reconcilable -> True.
          - Any held positions raise NotImplementedError, which PortfolioManager
            interprets as "this strategy doesn't support resuming from non-flat state."

        Stateful strategies that can resume across restarts MUST override this.
        """
        if not positions:
            return True  # Flat is always reconcilable.

        raise NotImplementedError(
            f"{self.__class__.__name__} received live positions but does not "
            f"implement sync_positions(). Override this method or run with "
            f"no held positions at broker."
        )

    # ==========================================
    # 🔄 PENDING-TRANSITION PROTOCOL (Risk-aware state)
    # ==========================================
    # When a strategy decides to change position (e.g. enter/exit a spread), it
    # STAGES the new state on itself but does NOT commit the mutation. The engine
    # then calls risk.check() on the signal. Only if Risk approves does the
    # engine call commit_pending_transition() — at which point the strategy
    # actually mutates current_pos / held_qty_*. If Risk rejects, the engine
    # calls rollback_pending_transition() and the strategy's state is unchanged.
    #
    # This prevents the cascade where a rejected entry leaves the strategy
    # believing it holds a position that was never sent to the broker, causing
    # the next bar's "exit" to actually open a phantom position in the opposite
    # direction.
    #
    # Strategies that don't stage transitions (simple stateless models) can
    # leave both methods as no-ops.

    def commit_pending_transition(self):
        """
        Apply the strategy's staged state transition after Risk approves the signal.
        Default: no-op. Override in stateful strategies.
        """
        pass

    def rollback_pending_transition(self):
        """
        Discard the strategy's staged state transition after Risk rejects the signal.
        Default: no-op. Override in stateful strategies.
        """
        pass

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