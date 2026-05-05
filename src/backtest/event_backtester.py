import os
import logging
from datetime import timedelta
import pandas as pd
from src.data.store import DataStore
from src.engine.risk import RiskManager
from src.portfolio.fees import IBKRFeeModel

# Minimal Logging for Backtest (Clean Output)
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

class VirtualBroker:
    """
    Simulates the Exchange and the Account.
    Tracks Cash, Positions, and handles Order Execution logic precisely.
    """
    def __init__(self, initial_capital=100000.0):
        self.initial_capital = initial_capital
        
        # Gross vs Net Tracking
        self.net_cash = initial_capital
        self.gross_cash = initial_capital
        
        self.positions = {} # { 'ASSET_KEY': quantity }
        self.equity_curve = []
        self.last_known_prices = {} 
        
        # Institutional Cost Model
        self.fee_model = IBKRFeeModel(default_slippage_bps=1.0)
        
        # Fee Ledger
        self.total_commissions_paid = 0.0
        self.total_regulatory_paid = 0.0
        self.total_slippage_incurred = 0.0

    def mark_to_market(self, prices, timestamp):
        """
        Calculates total Liquidation Value of the portfolio (both Gross and Net).
        """
        for key, price in prices.items():
            if price is not None and not pd.isna(price) and price > 0:
                self.last_known_prices[key] = price

        net_equity = self.net_cash
        gross_equity = self.gross_cash
        
        for key, qty in self.positions.items():
            price = prices.get(key)
            if pd.isna(price) or price is None:
                price = self.last_known_prices.get(key, 0)
            
            position_value = qty * price
            net_equity += position_value
            gross_equity += position_value
        
        self.equity_curve.append({
            'time': timestamp, 
            'net_equity': net_equity,
            'gross_equity': gross_equity,
            'total_fees': self.total_commissions_paid + self.total_regulatory_paid
        })
        return net_equity

    def execute(self, orders, market_prices):
        """
        Fills orders and applies exact micro-structure costs.
        """
        fills = []
        for order in orders:
            key = order.get('symbol')
            action = order['action']
            qty = order['qty']
            contract = order.get('contract')
            
            if not contract:
                logger.error(f"❌ Order rejected: Missing Contract object for {key}")
                continue

            # --- INSTITUTIONAL ASSET CLASS ROUTING ---
            # Extract the actual security type from the IBKR Contract object
            sec_type = getattr(contract, 'secType', 'STK')
            
            # Map IBKR secTypes to our FeeModel format
            if sec_type == 'CASH':
                asset_class = 'FX'
            elif sec_type == 'CRYPTO':
                asset_class = 'CRYPTO'
            else:
                asset_class = 'STK'
            
            # Check price availability
            price = market_prices.get(key)
            if pd.isna(price) or price is None:
                continue 
            
            # --- 1. CALCULATE TRUE COSTS ---
            cost_details = self.fee_model.calculate_event(asset_class, action, qty, price)
            total_cost_usd = cost_details['total_cost']
            
            # Convert total slippage back to per-share to adjust the fill price
            slippage_impact = cost_details['slippage'] / qty 
            
            # --- 2. APPLY SLIPPAGE TO FILL PRICE ---
            fill_price = price + slippage_impact if action == 'BUY' else price - slippage_impact
            trade_value = fill_price * qty
            frictionless_trade_value = price * qty
            
            # --- 3. ACCOUNTING ---
            if action == 'BUY':
                self.net_cash -= (trade_value + cost_details['commission'] + cost_details['regulatory'])
                self.gross_cash -= frictionless_trade_value
                self.positions[key] = self.positions.get(key, 0) + qty
            elif action == 'SELL':
                self.net_cash += (trade_value - cost_details['commission'] - cost_details['regulatory'])
                self.gross_cash += frictionless_trade_value
                self.positions[key] = self.positions.get(key, 0) - qty
                
            # Ledger Updates
            self.total_commissions_paid += cost_details['commission']
            self.total_regulatory_paid += cost_details['regulatory']
            self.total_slippage_incurred += cost_details['slippage']
                
            fills.append({
                'asset': key,
                'action': action,
                'qty': qty,
                'price': fill_price,
                'commission': cost_details['commission'],
                'regulatory': cost_details['regulatory'],
                'slippage': cost_details['slippage']
            })
        return fills

class BacktestEngine:
    """
    The Time Machine.
    Simulates the Live Engine's Event Loop over historical data.
    """
    def __init__(self, strategy_class, instruments, params, data_library: str, start_cap=100000):
        self.store = DataStore(library_name=data_library) 
        self.instruments = instruments
        self.params = params
        
        self.strategy = strategy_class(instruments, params)
        self.broker = VirtualBroker(start_cap)
        self.risk = RiskManager()
        
        self.risk.max_orders_per_minute = float('inf') 
        
        self.trades = []
        self.history = [] 
        self.gap_threshold = timedelta(hours=1)

    def run(self, start_date, end_date):
        print(f"⏳ Loading Data for {len(self.instruments)} assets ({start_date} to {end_date})...")
        
        dfs = {}
        for key in self.instruments.keys():
            df = self.store.load(key, start_date, end_date)
            if df is None or df.empty:
                print(f"❌ Critical: No data for {key}")
                return pd.DataFrame() 
            dfs[key] = df[['open', 'high', 'low', 'close', 'volume']]
        
        universe = pd.concat(dfs, axis=1).ffill()
        
        if universe.empty:
            print("❌ Universe is empty after alignment.")
            return pd.DataFrame()
        
        cols = universe.columns
        print(f"▶️ Simulating {len(universe)} ticks...")

        last_time = None
        pending_orders = [] 

        for row in universe.itertuples(index=True, name=None):
            timestamp = row[0]
            
            if last_time:
                delta = timestamp - last_time
                if delta > self.gap_threshold:
                    if hasattr(self.strategy, 'reset'):
                        self.strategy.reset()
                    pending_orders = [] 
            
            last_time = timestamp

            bar_dict = {}
            market_snapshot = {}
            
            for i, (asset, metric) in enumerate(cols):
                val = row[i+1]
                if asset not in bar_dict:
                    bar_dict[asset] = {}
                bar_dict[asset][metric] = val
                if metric == 'close':
                    market_snapshot[asset] = val
            
            latest_bars = pd.DataFrame.from_dict(bar_dict, orient='index')
            latest_bars['time'] = timestamp

            if pending_orders:
                fills = self.broker.execute(pending_orders, market_snapshot)
                filled_keys = []
                for fill in fills:
                    self.trades.append({**fill, 'time': timestamp})
                    filled_keys.append(fill['asset'])
                pending_orders = [o for o in pending_orders if o.get('symbol', o.get('key')) not in filled_keys]

            signal = self.strategy.on_bar(latest_bars)
            
            if signal:
                if signal.meta:
                    record = signal.meta.copy()
                    record['timestamp'] = timestamp
                    self.history.append(record)
                    
                if signal.orders:
                    current_eq = self.broker.equity_curve[-1]['net_equity'] if self.broker.equity_curve else self.broker.initial_capital
                    self.risk.update_state(
                        current_equity=current_eq,
                        start_of_day_equity=self.broker.initial_capital 
                    )
                    
                    if self.risk.check(signal, current_time=timestamp.timestamp()):
                        pending_orders.extend(signal.orders)

            self.broker.mark_to_market(market_snapshot, timestamp)

        print("✅ Backtest Complete.")
        self._generate_tearsheet()
        
        return pd.DataFrame(self.broker.equity_curve).set_index('time')

    def _generate_tearsheet(self):
        if not self.broker.equity_curve:
            print("⚠️ No trades executed. Equity curve is empty.")
            return
            
        start_equity = self.broker.initial_capital
        end_gross = self.broker.equity_curve[-1]['gross_equity']
        end_net = self.broker.equity_curve[-1]['net_equity']
        
        gross_pnl = end_gross - start_equity
        net_pnl = end_net - start_equity
        
        print("\n📊 --- INSTITUTIONAL RESULTS ---")
        print(f"   Initial Cap:       ${start_equity:,.2f}")
        print(f"   Gross Equity:      ${end_gross:,.2f} (PnL: ${gross_pnl:,.2f})")
        print(f"   Net Equity:        ${end_net:,.2f} (PnL: ${net_pnl:,.2f})")
        print("---------------------------------")
        print(f"   Total Commissions: ${self.broker.total_commissions_paid:,.2f}")
        print(f"   Total Regulatory:  ${self.broker.total_regulatory_paid:,.2f}")
        print(f"   Est. Slippage:     ${self.broker.total_slippage_incurred:,.2f}")
        print(f"   Total Trades:      {len(self.trades)}")
        print("---------------------------------\n")

        print("📈 Generating Institutional Tearsheet (quantstats)...")
        try:
            import quantstats as qs
            df = pd.DataFrame(self.broker.equity_curve)
            df.set_index('time', inplace=True)
            
            # You can now choose to analyze gross or net returns
            daily_net_equity = df['net_equity'].resample('D').last().dropna()
            returns = daily_net_equity.pct_change(fill_method=None).dropna()
            
            if returns.std() == 0:
                print("⚠️ Returns volatility is zero (no active trades). Skipping Tearsheet.")
                returnen 
            
            os.makedirs("research/Tearsheets", exist_ok=True) 
            report_path = "research/Tearsheets/event_backtest_tearsheet.html"
            
            qs.reports.html(returns, output=report_path, title="Bluegrey Net PnL Tearsheet")
            print(f"✅ Tearsheet saved successfully to: {report_path}")
            
        except ImportError:
            print("⚠️ QuantStats not installed. Run 'pip install quantstats'.")
        except Exception as e:
            print(f"⚠️ Could not generate tearsheet: {e}")
