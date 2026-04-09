import os
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from src.config import ROOT_DIR  
from src.fees import IBKRFeeModel

# Suppress pandas fragmentation warnings for clean notebook output
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

class VectorEngine:
    """
    Fund-Grade Vectorized Backtester for Jupyter Research.
    Features: Continuous sizing, Turnover-based Transaction Costs (TC) via IBKR exact routing, 
    Look-Ahead Bias prevention, and QuantStats Institutional Tearsheets.
    """
    def __init__(self, prices: pd.Series, signals: pd.Series, asset_class: str, initial_capital: float = 100000.0, execution_delay: int = 1):
        """
        :param prices: pd.Series of asset prices (must have a DatetimeIndex)
        :param signals: pd.Series of target allocations (e.g., 1.0 = 100% Long, -0.5 = 50% Short)
        :param asset_class: 'STK', 'FX', or 'CRYPTO' to route to correct fee model structure.
        :param initial_capital: Required to calculate exact share sizing for per-share commissions.
        :param execution_delay: Number of bars between signal generation and execution (Default: 1)
        """
        # Align series perfectly by index to prevent mismatched dates
        self.df = pd.DataFrame({'price': prices, 'target_weight': signals}).dropna()
        self.asset_class = asset_class
        self.initial_capital = initial_capital
        self.delay = execution_delay
        
        # Initialize the Institutional Cost Model
        self.fee_model = IBKRFeeModel(default_slippage_bps=1.0)
        self.results = None

    def run(self):
        """Executes the vectorized simulation with AUM-aware share sizing."""
        df = self.df.copy()
        
        # 1. Calculate underlying asset returns
        df['market_returns'] = df['price'].pct_change(fill_method=None)
        
        # 2. Shift weights to simulate Execution Delay (Zero Look-Ahead Bias)
        df['actual_weight'] = df['target_weight'].shift(self.delay).fillna(0)
        
        # 3. Calculate Frictionless (Gross) Returns & Equity
        # This shows what the Alpha is worth before market structure degrades it.
        df['gross_returns'] = df['actual_weight'].shift(1) * df['market_returns']
        df['gross_equity'] = self.initial_capital * (1 + df['gross_returns'].fillna(0)).cumprod()
        
        # 4. Calculate Share Turnover dynamically based on Gross AUM
        # Target Dollars = Equity * Target Weight
        # Target Shares = Target Dollars / Price
        df['target_shares'] = (df['gross_equity'] * df['actual_weight']) / df['price']
        
        # Calculate Turnover (Absolute change in position size measured in shares)
        df['share_diff'] = df['target_shares'].diff().fillna(0)
        
        # 5. Vectorized Cost Calculation
        # Pass the entire array of share differences to the fee model instantly
        costs = self.fee_model.calculate_vector(self.asset_class, df['share_diff'], df['price'])
        
        df['commission_usd'] = costs['commission']
        df['regulatory_usd'] = costs['regulatory']
        df['slippage_usd'] = costs['slippage']
        df['total_cost_usd'] = costs['total_cost']
        
        # 6. Calculate Net Equity Curve & Returns
        # Subtract absolute costs directly from the compounded Gross Equity
        df['cumulative_costs'] = df['total_cost_usd'].cumsum()
        df['net_equity'] = df['gross_equity'] - df['cumulative_costs']
        
        # Convert Net Equity back to bar-by-bar Returns for metrics
        df['net_returns'] = df['net_equity'].pct_change(fill_method=None).fillna(0)
        df['cum_market'] = (1 + df['market_returns'].fillna(0)).cumprod()
        
        self.results = df
        return self.results

    def tearsheet(self, title="Alpha Discovery Tearsheet"):
        """Generates Quick Stats and the full QuantStats HTML Report."""
        if self.results is None:
            self.run()
            
        df = self.results
        
        # --- 1. QUICK STATS (Using exact bar resolution) ---
        gross_ret = (df['gross_equity'].iloc[-1] / self.initial_capital - 1) * 100
        net_ret = (df['net_equity'].iloc[-1] / self.initial_capital - 1) * 100
        bm_ret = (df['cum_market'].iloc[-1] - 1) * 100
        
        roll_max = df['net_equity'].cummax()
        drawdown = (df['net_equity'] / roll_max) - 1.0
        max_dd = drawdown.min() * 100
        
        # Restored original Win Rate metric
        win_rate = (df['net_returns'] > 0).mean() * 100
        
        total_costs = df['cumulative_costs'].iloc[-1]
        
        print(f"\n📊 --- QUICK RESULTS: {title} ---")
        print(f"   Frictionless Return: {gross_ret:,.2f}%")
        print(f"   Net Strategy Return: {net_ret:,.2f}% (Market: {bm_ret:,.2f}%)")
        print(f"   Max Drawdown:        {max_dd:,.2f}%")
        print(f"   Win Rate (Bars):     {win_rate:.1f}%")
        print(f"   Total Fees Paid:     ${total_costs:,.2f}")
        print("-" * 35)

        # --- 2. QUANTSTATS HTML REPORT ---
        print("📈 Generating Institutional Tearsheet (quantstats)...")
        try:
            import quantstats as qs
            
            # Resample to Daily for accurate QuantStats math
            # Quantstats assumes 252 periods/year. Minute bars break its volatility models.
            daily_net_equity = df['net_equity'].resample('D').last().dropna()
            daily_returns = daily_net_equity.pct_change(fill_method=None).dropna()
            
            if daily_returns.std() == 0:
                print("⚠️ Strategy volatility is zero. Skipping HTML Tearsheet.")
                return

            tearsheets_dir = ROOT_DIR / "research" / "Tearsheets"
            tearsheets_dir.mkdir(parents=True, exist_ok=True)
            
            # Dynamically name the file based on the title so you don't overwrite older tests
            safe_title = title.replace(' ', '_').replace('/', '-')
            report_path = str(tearsheets_dir / f"{safe_title}_tearsheet.html")

            # Generate the HTML Report
            qs.reports.html(
                daily_returns, 
                benchmark=df['cum_market'].resample('D').last().pct_change(fill_method=None).dropna(),
                output=report_path, 
                title=title
            )
            print(f"✅ Tearsheet saved successfully to: {report_path}")
            print("   (Open this file in your web browser to view Sharpe, Drawdowns, etc.)")
            
        except ImportError:
            print("⚠️ QuantStats not installed. Run 'pip install quantstats'.")
        except Exception as e:
            print(f"⚠️ Could not generate tearsheet: {e}")

        # --- 3. IN-NOTEBOOK PLOTTING ---
        sns.set_theme(style="darkgrid")
        plt.figure(figsize=(14, 6))
        
        # Plot Benchmark normalized to Initial Capital
        bm_equity = self.initial_capital * df['cum_market']
        plt.plot(df.index, bm_equity, label='Benchmark (Buy & Hold)', color='gray', alpha=0.6)
        
        # Plot Gross vs Net Equity
        plt.plot(df.index, df['gross_equity'], label='Frictionless Equity (Gross)', color='green', alpha=0.5, linestyle='--')
        plt.plot(df.index, df['net_equity'], label='Live Equity (Net)', color='blue', linewidth=2)
        
        plt.fill_between(df.index, df['net_equity'], roll_max, color='red', alpha=0.2, label='Drawdown')
        
        plt.title(title)
        plt.ylabel("Portfolio Value ($)")
        plt.legend()
        plt.tight_layout()
        plt.show()