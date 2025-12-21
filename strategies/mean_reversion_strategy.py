import numpy as np
import pandas as pd
from collections import deque
from typing import List, Dict, Any, Optional
from .base import BaseStrategy, StrategySignal

class MeanReversionStrategy(BaseStrategy):
    def __init__(self, symbols: List[str], params: Dict[str, Any]):
        super().__init__(symbols, params)
        self.history = deque(maxlen=params['window'])
        
        # Expect symbols=['Target', 'Hedge']
        
        if len(symbols) != 2:
            raise ValueError("PairsTradingStrategy requires exactly 2 symbols.")
        
        self.ticker_a = symbols[0] 
        self.ticker_b = symbols[1]
        
    def on_tick(self, data: pd.DataFrame) -> Optional[StrategySignal]:
        # 1. Validation
        if self.ticker_a not in data or self.ticker_b not in data:
            return None
            
        pa = data[self.ticker_a]
        pb = data[self.ticker_b]
        
        # 2. Math
        spread = pa - (self.params['hedge_ratio'] * pb)
        self.history.append(spread)
        
        if len(self.history) < 20: return None # Warmup
        
        mean = np.mean(self.history)
        std = np.std(self.history)
        if std == 0: return None
        
        z = (spread - mean) / std
        
        # 3. Signal Generation
        qty = self.params['trading_qty']
        ratio = self.params['hedge_ratio']
        hedge_qty = int(qty * ratio)
        
        signal_obj = None
        
        # LOGIC: ENTRY LONG (Spread is too low -> Buy A, Sell B)
        if z < -self.params['std_dev_threshold']:
            orders = [
                {'symbol': self.ticker_a, 'action': 'BUY', 'qty': qty},
                {'symbol': self.ticker_b, 'action': 'SELL', 'qty': hedge_qty}
            ]
            signal_obj = StrategySignal("ENTRY_LONG", orders)

        # LOGIC: ENTRY SHORT (Spread is too high -> Sell A, Buy B)
        elif z > self.params['std_dev_threshold']:
            orders = [
                {'symbol': self.ticker_a, 'action': 'SELL', 'qty': qty},
                {'symbol': self.ticker_b, 'action': 'BUY', 'qty': hedge_qty}
            ]
            signal_obj = StrategySignal("ENTRY_SHORT", orders)
            
        # Attach Metadata (for Dashboard/Logs)
        if signal_obj:
            signal_obj.meta = {'z_score': z, 'spread': spread}
        elif abs(z) < 0.5:
             # Optional: Could emit an EXIT signal here if we held a position
             pass    
        return signal_obj