import pandas as pd
import numpy as np
import logging
from src.store import DataStore

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
        for symbol, qty in self.positions.items():
            if symbol in prices:
                equity += qty * prices[symbol]
        
        self.equity_curve.append({'time': timestamp, 'equity': equity})
        return equity

    def execute(self, orders, market_prices):
        fills = []
        for order in orders:
            # Resolve symbol from Contract object or config key
            # (Assumes order['contract'] has a mapped symbol or we pass the key directly)
            # For this snippet, we assume the strategy passes the CONFIG KEY as a string or we map it.
            # In your current setup, you need a way to map Contract -> Config Key here.
            # Let's assume we fixed that mapping.
            symbol = order['key'] 
            action = order['action'] # BUY/SELL
            qty = order['qty']
            
            if symbol not in market_prices:
                continue # Can't fill if no price
            
            price = market_prices[symbol]
            
            # SLIPPAGE MODEL
            # If buying, we pay more. If selling, we get less.
            slip = 0.01 * self.slippage_ticks # Assuming 1 tick = 1 cent for stocks
            fill_price = price + slip if action == 'BUY' else price - slip
            
            # COST MODEL
            commission = max(1.0, qty * self.commission_per_share) # Min $1 ticket
            
            cost = fill_price * qty
            if action == 'BUY':
                self.cash -= (cost + commission)
                self.positions[symbol] = self.positions.get(symbol, 0) + qty
            elif action == 'SELL':
                self.cash += (cost - commission)
                self.positions[symbol] = self.positions.get(symbol, 0) - qty
                
            fills.append({
                'symbol': symbol,
                'action': action,
                'qty': qty,
                'price': fill_price,
                'comm': commission
            })
        return fills

class BacktestEngine:
    def __init__(self, strategy_class, config_instruments, config_params, start_cap=100000):
        self.store = DataStore()
        self.instruments = config_instruments
        self.params = config_params
        self.strategy = strategy_class(self.instruments, self.params)
        self.broker = VirtualBroker(start_cap)
        self.results = []

    def run(self, start_date, end_date):
        print(f"⏳ Loading Data ({start_date} to {end_date})...")
        
        # [Data Loading Logic - Same as before]
        dfs = {}
        for key in self.instruments.keys():
            df = self.store.load(key, start_date, end_date)
            if df is None: return
            dfs[key] = df['close']
        
        universe = pd.DataFrame(dfs).fillna(method='ffill').dropna()
        print(f"▶️ Running Backtest on {len(universe)} ticks...")

        pending_orders = [] # Orders generated at T, to be filled at T+1

        # 2. EVENT LOOP (Corrected for Look-Ahead Bias)
        for timestamp, row in universe.iterrows():
            
            # A. EXECUTION PHASE (Fill orders from PREVIOUS tick)
            if pending_orders:
                # We fill using CURRENT prices (Open/Close of this bar)
                # Note: Using 'row' (Close) implies we fill at Close of T.
                # Ideally, if we have OHLC, we fill at Open of T.
                # For now, filling at Close of T is "Next Tick Execution" relative to T-1.
                fills = self.broker.execute(pending_orders, row)
                for fill in fills:
                    self.results.append({**fill, 'time': timestamp})
                pending_orders = []

            # B. STRATEGY PHASE (Generate signals based on CURRENT prices)
            signal = self.strategy.on_tick(row)
            
            # C. ORDER RECORDING (Do NOT execute yet)
            if signal and signal.orders:
                # We must map the contract object back to the string key for the Broker
                # This requires a quick helper or modification in the Signal object
                formatted_orders = []
                for o in signal.orders:
                    # Quick hack to find key:
                    # Real fix: Pass keys in Strategy or store map in Signal
                    key = [k for k, v in self.instruments.items() if v.symbol == o['contract'].symbol][0]
                    formatted_orders.append({
                        'key': key, 
                        'action': o['action'], 
                        'qty': o['qty']
                    })
                
                pending_orders = formatted_orders

            # D. REPORTING
            self.broker.mark_to_market(row, timestamp)

        print("✅ Backtest Complete.")
        
        # Return Equity Curve for Analysis
        return pd.DataFrame(self.broker.equity_curve).set_index('time')