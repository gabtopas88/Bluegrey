# strategies/template.py
import pandas as pd
import numpy as np
from typing import Optional
from .base import BaseStrategy, StrategySignal

class GenericStrategy(BaseStrategy):
    """
    A Template Strategy.
    Currently configured to demonstrate how to access generic assets from Config.
    """
    def __init__(self, instruments, params):
        super().__init__(instruments, params)
        # We assume the config provided keys 'ASSET_A' and 'ASSET_B'
        self.asset_a = instruments.get('ASSET_A')
        self.asset_b = instruments.get('ASSET_B')
        
    def on_tick(self, data: pd.DataFrame) -> Optional[StrategySignal]:
        """
        Analyzes the market and returns a signal.
        """
        # 1. Validation: Ensure we have data for our assets
        if 'ASSET_A' not in data or 'ASSET_B' not in data:
            return None
            
        price_a = data['ASSET_A']
        price_b = data['ASSET_B']
        
        # 2. Your Alpha Logic (Math) goes here
        # Example: Simple ratio calculation
        ratio = price_a / price_b
        
        # 3. Constructing a Signal
        signal = None
        
        # DUMMY LOGIC for demonstration:
        # If Ratio > X, Buy A, Sell B (Just an example!)
        if ratio > 1000: # Impossible number, just for safety
            signal = StrategySignal("DEMO_ENTRY")
            
            # Note: We pass the CONTRACT OBJECT, not a string
            signal.add_order(self.asset_a, "BUY", self.params['qty'])
            signal.add_order(self.asset_b, "SELL", self.params['qty'])
            
            signal.meta = {'ratio': ratio}

        return signal