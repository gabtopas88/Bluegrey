import os
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from src.config import ROOT_DIR  # <--- FIX 2: Anchor to absolute project root

# Suppress pandas fragmentation warnings for clean notebook output
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

class VectorEngine:
    """
    Fund-Grade Vectorized Backtester for Jupyter Research.
    Features: Continuous sizing, Turnover-based Transaction Costs (TC), 
    Look-Ahead Bias prevention, and QuantStats Institutional Tearsheets.
    """
    def __init__(self, prices: pd.Series, signals: pd.Series, tc_bps: float = 0.2, execution_delay: int = 1):
        """
        :param prices: pd.Series of asset prices (must have a DatetimeIndex)
        :param signals: pd.Series of target allocations (e.g., 1.0 = 100% Long, -0.5 = 50% Short)
        :param tc_bps: Transaction cost spread/commissions in basis points (1 bps = 0.01%)
        :param execution_delay: Number of bars between signal generation and execution (Default: 1)
        """
        # Align series perfectly by index to prevent mismatched dates
        self.df = pd.DataFrame({'price': prices, 'target_weight': signals}).dropna()
        self.tc = tc_bps / 10000.0 
        self.delay = execution_delay
        self.results = None

    def run(self):
        """Executes the vectorized simulation."""
        df = self.df.copy()
        
        # 1. Calculate underlying asset returns
        df['market_returns'] = df['price'].pct_change(fill_method=None)
        
        # 2. Shift weights to simulate Execution Delay (Zero Look-Ahead Bias)
        df['actual_weight'] = df['target_weight'].shift(self.delay).fillna(0)
        
        # 3. Calculate Turnover (Absolute change in position size)
        # e.g., moving from 0.5 to -0.5 requires trading 1.0 units
        df['turnover'] = df['actual_weight'].diff().abs().fillna(0)
        
        # 4. Calculate Gross Strategy Returns
        df['gross_returns'] = df['actual_weight'].shift(1) * df['market_returns']
        
        # 5. Apply Transaction Costs (Turnover * Cost)
        df['net_returns'] = df['gross_returns'] - (df['turnover'] * self.tc)
        
        # 6. Cumulative Equity (Start at 1.0)
        df['cum_market'] = (1 + df['market_returns'].fillna(0)).cumprod()
        df['cum_strategy'] = (1 + df['net_returns'].fillna(0)).cumprod()
        
        self.results = df
        return self.results

    def tearsheet(self, title="Alpha Discovery Tearsheet"):
        """Generates Quick Stats and the full QuantStats HTML Report."""
        if self.results is None:
            self.run()
            
        df = self.results
        
        # --- 1. QUICK STATS (Using exact bar resolution) ---
        total_ret = (df['cum_strategy'].iloc[-1] - 1) * 100
        bm_ret = (df['cum_market'].iloc[-1] - 1) * 100
        
        roll_max = df['cum_strategy'].cummax()
        drawdown = (df['cum_strategy'] / roll_max) - 1.0
        max_dd = drawdown.min() * 100
        
        win_rate = (df['net_returns'] > 0).mean() * 100
        
        print(f"\n📊 --- QUICK RESULTS: {title} ---")
        print(f"   Strategy Return: {total_ret:,.2f}% (Market: {bm_ret:,.2f}%)")
        print(f"   Max Drawdown:    {max_dd:,.2f}%")
        print(f"   Win Rate (Bars): {win_rate:.1f}%")
        print("-" * 35)

        # --- 2. QUANTSTATS HTML REPORT ---
        print("📈 Generating Institutional Tearsheet (quantstats)...")
        try:
            import quantstats as qs
            
            # Resample to Daily for accurate QuantStats math
            # Quantstats assumes 252 periods/year. Minute bars break its volatility models.
            daily_equity = df['cum_strategy'].resample('D').last().dropna()
            daily_returns = daily_equity.pct_change(fill_method=None).dropna()
            
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
            
        except ImportError:
            print("⚠️ QuantStats not installed. Run 'pip install quantstats'.")
        except Exception as e:
            print(f"⚠️ Could not generate tearsheet: {e}")

        # --- 3. IN-NOTEBOOK PLOTTING ---
        sns.set_theme(style="darkgrid")
        plt.figure(figsize=(14, 6))
        
        plt.plot(df.index, df['cum_market'], label='Benchmark (Buy & Hold)', color='gray', alpha=0.6)
        plt.plot(df.index, df['cum_strategy'], label='Strategy Equity', color='blue', linewidth=2)
        
        plt.fill_between(df.index, df['cum_strategy'], roll_max, color='red', alpha=0.2, label='Drawdown')
        
        plt.title(title)
        plt.ylabel("Cumulative Multiplier")
        plt.legend()
        plt.tight_layout()
        plt.show()