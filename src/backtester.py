import pandas as pd
import numpy as np
from datetime import timedelta
import logging
from src.store import DataStore
from strategies.base import StrategySignal

logger = logging.getLogger(__name__)

class BacktestEngine:
    """
    The Time Machine.
    Simulates the Live Engine's Event Loop over historical data.
    Crucially: It enforces the 'Reset' logic on data gaps.
    """
    def __init__(self, strategy_class, instruments, params, start_date, end_date):
        self.store = DataStore()
        self.instruments = instruments
        self.params = params
        self.start_date = start_date
        self.end_date = end_date
        
        # Instantiate Strategy
        self.strategy = strategy_class(instruments, params)
        
        # Results Buffer
        self.trades = []
        self.history = [] # For plotting indicators (Z-score, etc.)
        
        # Gap Settings
        self.gap_threshold = timedelta(hours=1)

    def run(self):
        print(f"⏳ Loading Data for {len(self.instruments)} assets...")
        
        # 1. LOAD & ALIGN DATA
        # We align all assets to the same index (Outer Join) to simulate the 'Market Clock'
        dfs = {}
        for key in self.instruments.keys():
            df = self.store.load(key, self.start_date, self.end_date)
            if df is None or df.empty:
                print(f"❌ Critical: No data for {key}")
                return None
            dfs[key] = df
            
        # Combine into a single Panel (using Closing prices for the 'Bar' update)
        # Note: In a real tick-backtest, this is more complex. For 'on_bar' strategies, this is sufficient.
        universe = pd.concat(dfs, axis=1, keys=self.instruments.keys())
        universe.sort_index(inplace=True)
        
        # Forward Fill: If Asset A ticks but Asset B doesn't, B retains previous price
        # Dropna: We need at least one valid row to start
        universe.fillna(method='ffill', inplace=True)
        universe.dropna(inplace=True)
        
        print(f"▶️ Simulating {len(universe)} bars...")
        
        # 2. THE EVENT LOOP
        last_time = None
        
        # We iterate through the 'Clock' (The Index)
        for timestamp, row in universe.iterrows():
            
            # --- A. CHECK FOR GAPS (The Heartbeat) ---
            if last_time:
                delta = timestamp - last_time
                if delta > self.gap_threshold:
                    # ⚠️ GAP DETECTED -> RESET STRATEGY
                    # This ensures the backtest mimics the Live Engine's safety
                    self.strategy.reset()
            
            last_time = timestamp
            
            # --- B. PREPARE DATA ---
            # Reconstruct the 'bars' DataFrame expected by strategy.on_bar()
            # row is a MultiIndex Series: (ASSET_A, close), (ASSET_A, open)...
            # We transform it back to a clean DataFrame for the strategy
            
            # Extract prices for easy access
            current_prices = row.xs('close', level=1) # Series: [ASSET_A: 100, ASSET_B: 200]
            
            # --- C. UPDATE STRATEGY (The Math) ---
            # Ideally, we pass the full OHLCV row, but for now we pass the prices as a proxy for the 'bar'
            # In a full impl, we'd reconstruct the OHLC DataFrame.
            signal = self.strategy.on_bar(current_prices) 
            
            # --- D. EXECUTE (The Trade) ---
            if signal:
                self._record_activity(timestamp, signal, current_prices)

        print("✅ Backtest Complete.")
        return pd.DataFrame(self.trades), pd.DataFrame(self.history)

    def _record_activity(self, timestamp, signal, prices):
        """
        Logs trades and Strategy State (Z-score, Beta) for analysis.
        """
        # 1. Log Metadata (The "Why")
        if signal.meta:
            record = signal.meta.copy()
            record['timestamp'] = timestamp
            self.history.append(record)
            
        # 2. Log Trades (The "What")
        for order in signal.orders:
            # Match Contract to Config Key
            # (Simple reverse lookup for the backtest report)
            asset_key = "UNKNOWN"
            for k, v in self.instruments.items():
                if v.symbol == order['contract'].symbol:
                    asset_key = k
                    break
            
            # Simulate Fill (Slippage = 0 for now)
            fill_price = prices.get(asset_key, 0)
            
            self.trades.append({
                'time': timestamp,
                'signal_type': signal.signal_type,
                'asset': asset_key,
                'action': order['action'],
                'qty': order['qty'],
                'price': fill_price,
                'value': fill_price * order['qty']
            })