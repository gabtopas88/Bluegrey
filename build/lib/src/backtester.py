"""
This is the Simulation Engine. It mimics the main.py loop but moves through historical time instead of real time.
Crucial: It uses the exact same Strategy class as the live engine. 
This guarantees that if it works in the backtest, the logic is identical in live trading.
"""
import pandas as pd
import logging
from src.store import DataStore

# Minimal Logging for Backtest (Clean Output)
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

class BacktestEngine:
    def __init__(self, strategy_class, config_instruments, config_params):
        """
        The Simulator.
        strategy_class: The Class itself (e.g., GenericStrategy), not an instance.
        """
        self.store = DataStore()
        self.instruments = config_instruments
        self.params = config_params
        
        # Initialize the Strategy (just like Live Engine does)
        self.strategy = strategy_class(self.instruments, self.params)
        
        self.results = []

    def run(self, start_date, end_date):
        print(f"⏳ Loading Data ({start_date} to {end_date})...")
        
        # 1. LOAD & ALIGN DATA
        # We need to create a single synchronized timeline for all assets
        dfs = {}
        for key in self.instruments.keys():
            df = self.store.load(key, start_date, end_date)
            if df is None:
                print(f"❌ Missing data for {key}. Aborting.")
                return
            # Keep only Close price for simplicity in this template
            # (In reality, you'd keep OHLC)
            dfs[key] = df['close'] 
        
        # Merge into one Master DataFrame (Outer Join to keep all timestamps)
        # This simulates the "Price Board" in the live engine
        universe = pd.DataFrame(dfs).fillna(method='ffill').dropna()
        
        if universe.empty:
            print("❌ Universe is empty after alignment. Check data dates.")
            return

        print(f"▶️ Running Backtest on {len(universe)} ticks...")

        # 2. EVENT LOOP (The Time Machine)
        for timestamp, row in universe.iterrows():
            # row is a Series with keys ['ASSET_A', 'ASSET_B']
            # This matches exactly what strategy.on_tick receives in Live Mode!
            
            signal = self.strategy.on_tick(row)
            
            if signal:
                self._record_trade(timestamp, signal, row)

        print("✅ Backtest Complete.")
        return pd.DataFrame(self.results)

    def _record_trade(self, timestamp, signal, prices):
        """
        Logs the simulated trade.
        """
        for order in signal.orders:
            # We log what would have happened
            contract = order['contract']
            # We need to find which config key this contract corresponds to
            # (Simplified matching for demo)
            asset_key = [k for k, v in self.instruments.items() if v.symbol == contract.symbol][0]
            
            fill_price = prices[asset_key]
            
            self.results.append({
                'time': timestamp,
                'signal': signal.signal_type,
                'asset': asset_key,
                'action': order['action'],
                'qty': order['qty'],
                'price': fill_price,
                'meta': signal.meta
            })