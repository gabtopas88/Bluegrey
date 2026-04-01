import os
import logging
from datetime import timedelta
import pandas as pd
from src.store import DataStore
from src.risk import RiskManager

# Minimal Logging for Backtest (Clean Output)
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

class VirtualBroker:
    """
    Simulates the Exchange and the Account.
    Tracks Cash, Positions, and handles Order Execution logic (Slippage/Comm).
    """
    def __init__(self, initial_capital=100000.0):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions = {} # { 'ASSET_KEY': quantity }
        self.commission_per_share = 0.005 # IBKR Pro Tier approx
        self.slippage_ticks = 1 # Assume 1 tick slippage against us
        self.equity_curve = []
        
        # Valuation Cache
        self.last_known_prices = {} 

    def mark_to_market(self, prices, timestamp):
        """
        Calculates total Liquidation Value of the portfolio.
        Uses last known price if current price is missing (NaN).
        """
        # Update price cache with valid new data
        for key, price in prices.items():
            if price is not None and not pd.isna(price) and price > 0:
                self.last_known_prices[key] = price

        equity = self.cash
        for key, qty in self.positions.items():
            # Use current price, fallback to last known, fallback to 0 (bankruptcy protection)
            price = prices.get(key)
            if pd.isna(price) or price is None:
                price = self.last_known_prices.get(key, 0)
            
            equity += qty * price
        
        self.equity_curve.append({'time': timestamp, 'equity': equity})
        return equity

    def execute(self, orders, market_prices):
        """
        Fills orders based on the passed market_prices.
        """
        fills = []
        for order in orders:
            key = order.get('symbol', order.get('key'))
            action = order['action'] # BUY/SELL
            qty = order['qty']
            
            # Check price availability
            price = market_prices.get(key)
            if pd.isna(price) or price is None:
                # OPTIONAL: usage of last_known_prices for execution is risky.
                # Real exchanges won't fill you if there's no liquidity. We skip.
                continue 
            
            # SLIPPAGE MODEL
            slip = 0.01 * self.slippage_ticks 
            fill_price = price + slip if action == 'BUY' else price - slip
            
            # COST MODEL (Handles FX vs Equities)
            cost = fill_price * qty
            if qty >= 10000:
                commission = max(2.0, cost * 0.00002) # 0.20 bps for FX
            else:
                commission = max(1.0, qty * self.commission_per_share)
            
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
                'comm': commission,
                'cost': cost
            })
        return fills

class BacktestEngine:
    """
    The Time Machine.
    Simulates the Live Engine's Event Loop over historical data.
    Enforces 'Reset' on data gaps and eliminates Look-Ahead Bias.
    """
    def __init__(self, strategy_class, instruments, params, data_library: str,start_cap=100000):
        self.store = DataStore(library_name=data_library) # Pass the target library
        self.instruments = instruments
        self.params = params
        
        # Instantiate Strategy
        self.strategy = strategy_class(instruments, params)
        
        # The Accountant
        self.broker = VirtualBroker(start_cap)
        self.risk = RiskManager()
        
        # Important: Turn off the physical time velocity lock for backtests
        self.risk.max_orders_per_minute = float('inf') 
        
        # Results Buffers
        self.trades = []
        self.history = [] # For Strategy Metadata (Z-scores, etc.)
        
        # Gap Settings
        self.gap_threshold = timedelta(hours=1)

    def run(self, start_date, end_date):
        print(f"⏳ Loading Data for {len(self.instruments)} assets ({start_date} to {end_date})...")
        
        # 1. LOAD & ALIGN FULL OHLCV DATA
        dfs = {}
        for key in self.instruments.keys():
            df = self.store.load(key, start_date, end_date)
            if df is None or df.empty:
                print(f"❌ Critical: No data for {key}")
                return pd.DataFrame() # Return empty on failure
            dfs[key] = df['close'] # Flatten to single price series per asset
            
            # Keep all relevant columns
            dfs[key] = df[['open', 'high', 'low', 'close', 'volume']]
        
        # Create a unified MultiIndex DataFrame and forward fill gaps
        universe = pd.concat(dfs, axis=1).ffill()
        
        if universe.empty:
            print("❌ Universe is empty after alignment.")
            return pd.DataFrame()
        
        # Cache the MultiIndex columns before the loop starts
        # Example: [('C:EURUSD', 'open'), ('C:EURUSD', 'high'), ...]
        cols = universe.columns

        print(f"▶️ Simulating {len(universe)} ticks...")

        # 2. THE EVENT LOOP
        last_time = None
        pending_orders = [] # Orders generated at T, to be filled at T+1

        # PERFORMANCE OPTIMIZATION: itertuples() is ~50x faster than iterrows()
        for row in universe.itertuples(index=True, name=None):
            timestamp = row[0]
            
            # --- A. CHECK FOR GAPS (The Heartbeat) ---
            if last_time:
                delta = timestamp - last_time
                if delta > self.gap_threshold:
                    # ⚠️ GAP DETECTED -> RESET STRATEGY
                    if hasattr(self.strategy, 'reset'):
                        self.strategy.reset()
                    pending_orders = [] # Safety: Cancel pending orders on gap
            
            last_time = timestamp

            # --- B. CONSTRUCT STRATEGY PAYLOAD ---
            bar_dict = {}
            market_snapshot = {}
            
            # Map the raw tuple values back to their Asset and Metric
            # row[1:] slices off the timestamp to match the 'cols' array perfectly
            for i, (asset, metric) in enumerate(cols):
                val = row[i+1]
                
                if asset not in bar_dict:
                    bar_dict[asset] = {}
                bar_dict[asset][metric] = val
                
                if metric == 'close':
                    market_snapshot[asset] = val
            
            # Build the DataFrame from a native dict (Much faster than unstacking a Series)
            latest_bars = pd.DataFrame.from_dict(bar_dict, orient='index')
            latest_bars['time'] = timestamp

            # --- C. EXECUTION PHASE ---
            if pending_orders:
                fills = self.broker.execute(pending_orders, market_snapshot)
                filled_keys = []
                for fill in fills:
                    self.trades.append({**fill, 'time': timestamp})
                    filled_keys.append(fill['asset'])
                pending_orders = [o for o in pending_orders if o.get('symbol', o.get('key')) not in filled_keys]

            # --- D. STRATEGY PHASE ---
            signal = self.strategy.on_bar(latest_bars)
            
            # --- E. RECORDING ---
            if signal:
                if signal.meta:
                    record = signal.meta.copy()
                    record['timestamp'] = timestamp
                    self.history.append(record)
                    
                if signal.orders:
                    current_eq = self.broker.equity_curve[-1]['equity'] if self.broker.equity_curve else self.broker.initial_capital
                    self.risk.update_state(
                        current_equity=current_eq,
                        start_of_day_equity=self.broker.initial_capital 
                    )
                    
                    if self.risk.check(signal, current_time=timestamp.timestamp()):
                        pending_orders.extend(signal.orders)

            # --- F. REPORTING ---
            self.broker.mark_to_market(market_snapshot, timestamp)

        print("✅ Backtest Complete.")
        self._generate_tearsheet()
        
        return pd.DataFrame(self.broker.equity_curve).set_index('time')

    def _generate_tearsheet(self):
        """Prints a quick summary and generates a professional QuantStats Tearsheet."""
        if not self.broker.equity_curve:
            print("⚠️ No trades executed. Equity curve is empty.")
            return
            
        start_equity = self.broker.initial_capital
        end_equity = self.broker.equity_curve[-1]['equity']
        pnl = end_equity - start_equity
        ret = (pnl / start_equity) * 100
        
        print("\n📊 --- QUICK RESULTS ---")
        print(f"   Initial Cap: ${start_equity:,.2f}")
        print(f"   Final Equity: ${end_equity:,.2f}")
        print(f"   Net PnL: ${pnl:,.2f} ({ret:.2f}%)")
        print(f"   Total Trades: {len(self.trades)}")
        print("-------------------------\n")

        print("📈 Generating Institutional Tearsheet (quantstats)...")
        try:
            import quantstats as qs
            
            # 1. Convert equity curve to DataFrame
            df = pd.DataFrame(self.broker.equity_curve)
            df.set_index('time', inplace=True)
            
            # 2. Resample to Daily Equity (Quantstats expects daily data)
            daily_equity = df['equity'].resample('D').last().dropna()
            
            # 3. Calculate Daily Returns (Percentage change)
            returns = daily_equity.pct_change(fill_method=None).dropna()
            
            # SAFETY CHECK (Prevent Zero-Variance Crash)
            if returns.std() == 0:
                print("⚠️ Returns volatility is zero (no active trades). Skipping Tearsheet.")
                return
            
            # 4. Generate the HTML Report
            os.makedirs("research/Tearsheets", exist_ok=True) 
            report_path = "research/Tearsheets/event_backtest_tearsheet.html"
            
            qs.reports.html(returns, output=report_path, title="Bluegrey Strategy Tearsheet")
            print(f"✅ Tearsheet saved successfully to: {report_path}")
            print("   (Open this file in your web browser to view Sharpe, Drawdowns, etc.)")
            
        except ImportError:
            print("⚠️ QuantStats not installed. Run 'pip install quantstats' for full tearsheet.")
        except Exception as e:
            print(f"⚠️ Could not generate tearsheet: {e}")