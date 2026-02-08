import pandas as pd
import numpy as np
import logging
from datetime import timedelta
from src.store import DataStore

# Minimal Logging for Backtest (Clean Output)
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

class VirtualBroker:
    """
    Simulates the Exchange and the Account.
    Tracks Cash, Positions, and handles Order Execution logic (Slippage/Comm).
    """
    def __init__(self, initial_capital=100000.0):
        self.cash = initial_capital
        self.positions = {} # { 'ASSET_KEY': quantity }
        self.commission_per_share = 0.005 # IBKR Pro Tier approx
        self.slippage_ticks = 1 # Assume 1 tick slippage against us
        self.equity_curve = []

    def mark_to_market(self, prices, timestamp):
        """Calculates total Liquidation Value of the portfolio"""
        equity = self.cash
        for key, qty in self.positions.items():
            # If we hold an asset but have no current price, use 0 (or last known)
            # This 'get(key, 0)' is a safety fallback.
            price = prices.get(key, 0)
            equity += qty * price
        
        self.equity_curve.append({'time': timestamp, 'equity': equity})
        return equity

    def execute(self, orders, market_prices):
        """
        Fills orders based on the passed market_prices.
        """
        fills = []
        for order in orders:
            key = order['key'] 
            action = order['action'] # BUY/SELL
            qty = order['qty']
            
            if key not in market_prices or pd.isna(market_prices[key]):
                continue # Can't fill if no price
            
            price = market_prices[key]
            
            # SLIPPAGE MODEL
            slip = 0.01 * self.slippage_ticks 
            fill_price = price + slip if action == 'BUY' else price - slip
            
            # COST MODEL
            commission = max(1.0, qty * self.commission_per_share)
            cost = fill_price * qty
            
            if action == 'BUY':
                self.cash -= (cost + commission)
                self.positions[key] = self.positions.get(key, 0) + qty
            elif action == 'SELL':
                self.cash += (cost - commission)
                self.positions[key] = self.positions.get(key, 0) - qty
                
            fills.append({
                'asset': key,
                'action': action,
                'qty': qty,
                'price': fill_price,
                'comm': commission
            })
        return fills

class BacktestEngine:
    """
    The Time Machine.
    Simulates the Live Engine's Event Loop over historical data.
    Enforces 'Reset' on data gaps and eliminates Look-Ahead Bias.
    """
    def __init__(self, strategy_class, instruments, params, start_cap=100000):
        self.store = DataStore()
        self.instruments = instruments
        self.params = params
        
        # Instantiate Strategy
        self.strategy = strategy_class(instruments, params)
        
        # The Accountant
        self.broker = VirtualBroker(start_cap)
        
        # Results Buffers
        self.trades = []
        self.history = [] # For Strategy Metadata (Z-scores, etc.)
        
        # Gap Settings
        self.gap_threshold = timedelta(hours=1)

    def run(self, start_date, end_date):
        print(f"⏳ Loading Data for {len(self.instruments)} assets ({start_date} to {end_date})...")
        
        # 1. LOAD & ALIGN DATA
        dfs = {}
        for key in self.instruments.keys():
            df = self.store.load(key, start_date, end_date)
            if df is None or df.empty:
                print(f"❌ Critical: No data for {key}")
                return pd.DataFrame() # Return empty on failure
            dfs[key] = df['close'] # Flatten to single price series per asset
            
        # Merge into Master Price Board
        universe = pd.DataFrame(dfs).fillna(method='ffill').dropna()
        
        if universe.empty:
            print("❌ Universe is empty after alignment.")
            return pd.DataFrame()

        print(f"▶️ Simulating {len(universe)} ticks...")

        # 2. THE EVENT LOOP
        last_time = None
        pending_orders = [] # Orders generated at T, to be filled at T+1

        for timestamp, row in universe.iterrows():
            
            # --- A. CHECK FOR GAPS (The Heartbeat) ---
            if last_time:
                delta = timestamp - last_time
                if delta > self.gap_threshold:
                    # ⚠️ GAP DETECTED -> RESET STRATEGY
                    if hasattr(self.strategy, 'reset'):
                        self.strategy.reset()
                    pending_orders = [] # Cancel pending orders on gap? (Optional, usually safer)
            
            last_time = timestamp

            # --- B. EXECUTION PHASE (Fill orders from PREVIOUS tick) ---
            if pending_orders:
                fills = self.broker.execute(pending_orders, row)
                for fill in fills:
                    self.trades.append({**fill, 'time': timestamp})
                pending_orders = []

            # --- C. STRATEGY PHASE (Generate signals based on CURRENT prices) ---
            signal = self.strategy.on_tick(row)
            
            # --- D. RECORDING (Do NOT execute yet) ---
            if signal:
                # 1. Store Metadata (Z-scores, etc.)
                if signal.meta:
                    record = signal.meta.copy()
                    record['timestamp'] = timestamp
                    self.history.append(record)

                # 2. Queue Orders for NEXT tick
                if signal.orders:
                    # Orders already have 'key' thanks to base.py update
                    pending_orders = signal.orders

            # --- E. REPORTING ---
            self.broker.mark_to_market(row, timestamp)

        print("✅ Backtest Complete.")
        
        # Return Equity Curve (Primary) and History (Secondary)
        # We can attach history to the object or return a tuple if needed.
        # For now, we return Equity Curve to match the notebook call.
        return pd.DataFrame(self.broker.equity_curve).set_index('time')