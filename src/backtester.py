import os
import logging
from datetime import timedelta
import pandas as pd
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
        CRITICAL FIX: Uses last known price if current price is missing (NaN).
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
            key = order['key'] 
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
            
        # Merge into Master Price Board (Pandas Deprecation Fix)
        universe = pd.DataFrame(dfs).ffill()
        
        if universe.empty:
            print("❌ Universe is empty after alignment.")
            return pd.DataFrame()

        print(f"▶️ Simulating {len(universe)} ticks...")

        # 2. THE EVENT LOOP
        last_time = None
        pending_orders = [] # Orders generated at T, to be filled at T+1

        # PERFORMANCE OPTIMIZATION: itertuples() is ~50x faster than iterrows()
        for row in universe.itertuples():
            timestamp = row.Index  # type: ignore
            
            # --- A. CHECK FOR GAPS (The Heartbeat) ---
            if last_time:
                delta = timestamp - last_time
                if delta > self.gap_threshold:
                    # ⚠️ GAP DETECTED -> RESET STRATEGY
                    if hasattr(self.strategy, 'reset'):
                        self.strategy.reset()
                    pending_orders = [] # Safety: Cancel pending orders on gap
            
            last_time = timestamp

            # Prepare data for Strategy/Broker (Convert NamedTuple to Dict)
            market_snapshot = row._asdict()  # type: ignore
            del market_snapshot['Index']

            # --- B. EXECUTION PHASE (Fill orders from PREVIOUS tick) ---
            if pending_orders:
                fills = self.broker.execute(pending_orders, market_snapshot)
                
                filled_keys = []
                for fill in fills:
                    self.trades.append({**fill, 'time': timestamp})
                    filled_keys.append(fill['asset'])
                
                # Keep orders that were NOT filled yet
                pending_orders = [o for o in pending_orders if o['key'] not in filled_keys]

            # --- C. STRATEGY PHASE (Generate signals based on CURRENT prices) ---
            # Strategies expect a dict {Key: Price}
            signal = self.strategy.on_tick(market_snapshot)
            
            # --- D. RECORDING (Do NOT execute yet) ---
            if signal:
                # 1. Store Metadata 
                if signal.meta:
                    record = signal.meta.copy()
                    record['timestamp'] = timestamp
                    self.history.append(record)

                # 2. Queue Orders for NEXT tick
                if signal.orders:
                    pending_orders.extend(signal.orders)

            # --- E. REPORTING ---
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
            returns = daily_equity.pct_change().dropna()
            
            # SAFETY CHECK (Prevent Zero-Variance Crash)
            if returns.std() == 0:
                print("⚠️ Returns volatility is zero (no active trades). Skipping Tearsheet.")
                return
            
            # 4. Generate the HTML Report
            os.makedirs("research", exist_ok=True) # Ensure the research folder exists
            report_path = "research/backtest_tearsheet.html"
            
            qs.reports.html(returns, output=report_path, title="Bluegrey Strategy Tearsheet")
            print(f"✅ Tearsheet saved successfully to: {report_path}")
            print("   (Open this file in your web browser to view Sharpe, Drawdowns, etc.)")
            
        except ImportError:
            print("⚠️ QuantStats not installed. Run 'pip install quantstats' for full tearsheet.")
        except Exception as e:
            print(f"⚠️ Could not generate tearsheet: {e}")