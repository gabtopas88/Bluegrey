import os
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from quantstats.stats import sharpe, sortino
from src.config import ROOT_DIR
from src.fees import IBKRFeeModel

# Suppress pandas fragmentation warnings for clean notebook output
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

class PortfolioVectorEngine:
    """
    Multi-Asset Institutional Vectorized Backtester for Jupyter Research.
    Features: Matrix-based continuous sizing, Turnover-based Transaction Costs (TC) via IBKR 
    exact routing across N-assets, Look-Ahead Bias prevention, and QuantStats Institutional Tearsheets.
    """
    def __init__(self, prices: pd.DataFrame, signals: pd.DataFrame, asset_class: str, initial_capital: float = 1_000_000.0, execution_delay: int = 1):
        """
        :param prices: pd.DataFrame of asset prices (Index=Datetime, Columns=Tickers)
        :param signals: pd.DataFrame of target weights (Index=Datetime, Columns=Tickers)
        :param asset_class: 'STK', 'FX', or 'CRYPTO' to route to correct fee model structure.
        :param initial_capital: Required to calculate exact share sizing for per-share commissions.
        :param execution_delay: Number of bars between signal generation and execution (Default: 1)
        """
        # Align DataFrames perfectly by index to prevent mismatched dates or look-ahead
        self.prices, self.signals = prices.align(signals, join='inner')
        self.asset_class = asset_class
        self.initial_capital = initial_capital
        self.delay = execution_delay
        
        # Initialize the Institutional Cost Model
        self.fee_model = IBKRFeeModel(default_slippage_bps=1.0)
        self.portfolio_stats = None

    def run(self):
        """Executes the vectorized matrix simulation with AUM-aware share sizing."""
        
        # 1. Calculate underlying asset returns (N-Assets Matrix)
        market_returns = self.prices.pct_change(fill_method=None).fillna(0)
        
        # 2. Shift weights to simulate Execution Delay (Zero Look-Ahead Bias)
        actual_weights = self.signals.shift(self.delay).fillna(0)
        
        # 3. Calculate Frictionless (Gross) Returns & Equity
        # This shows what the Alpha is worth before market structure degrades it.
        # (Weight at T-1) * (Return at T) summed across all assets per bar
        temp_gross = actual_weights.shift(1) * market_returns
        portfolio_gross_returns = temp_gross.sum(axis=1) if hasattr(temp_gross, 'shape') and len(temp_gross.shape) > 1 else temp_gross
        gross_equity = self.initial_capital * (1 + portfolio_gross_returns).cumprod()
        
        # 4. Calculate Share Turnover dynamically based on Gross AUM
        # Target Dollars Matrix = Broadcasting 1D gross_equity across the 2D weights matrix
        # Target Shares Matrix = Target Dollars / Prices
        target_allocation_usd = actual_weights.multiply(gross_equity, axis=0)
        target_shares = target_allocation_usd / self.prices
        
        # Calculate Turnover (Absolute change in position size measured in shares per asset)
        share_diff = target_shares.diff().fillna(0)
        
        # 5. Vectorized Matrix Cost Calculation
        # Pass the entire 2D DataFrames to the fee model instantly
        cost_matrix = self.fee_model.calculate_vector_matrix(self.asset_class, share_diff, self.prices)
        
        # Sum costs across the entire portfolio per bar
        total_costs_usd = cost_matrix.sum(axis=1) if hasattr(cost_matrix, 'shape') and len(cost_matrix.shape) > 1 else cost_matrix
        cumulative_costs = total_costs_usd.cumsum()
        
        # 6. Calculate Net Portfolio Equity Curve & Returns
        # Subtract absolute costs directly from the compounded Gross Equity
        net_equity = gross_equity - cumulative_costs
        
        # Convert Net Equity back to bar-by-bar Returns for metrics
        net_returns = net_equity.pct_change(fill_method=None).fillna(0)
        
        # Equal-Weight Benchmark for comparison
        ew_benchmark_returns = market_returns.mean(axis=1) if hasattr(market_returns, 'shape') and len(market_returns.shape) > 1 else market_returns
        cum_benchmark = (1 + ew_benchmark_returns).cumprod()
        
        self.portfolio_stats = pd.DataFrame({
            'gross_returns': portfolio_gross_returns,
            'gross_equity': gross_equity,
            'total_costs_usd': total_costs_usd,
            'cumulative_costs': cumulative_costs,
            'net_equity': net_equity,
            'net_returns': net_returns,
            'cum_benchmark': cum_benchmark
        })
        
        return self.portfolio_stats

    def tearsheet(self, title="Multi-Asset Alpha Discovery Tearsheet"):
        """Generates Quick Stats and the full QuantStats HTML Report."""
        if self.portfolio_stats is None:
            self.run()
            
        df = self.portfolio_stats
        
        # --- 1. QUICK STATS (Using exact bar resolution) ---
        gross_ret = (df['gross_equity'].iloc[-1] / self.initial_capital - 1) * 100
        net_ret = (df['net_equity'].iloc[-1] / self.initial_capital - 1) * 100
        bm_ret = (df['cum_benchmark'].iloc[-1] - 1) * 100
        
        roll_max = df['net_equity'].cummax()
        drawdown = (df['net_equity'] / roll_max) - 1.0
        max_dd = drawdown.min() * 100
        
        # Win Rate metric–– 'gross_returns' is used to avoid returns being negative due to position-entry fees when they should have been exactly 0 (ie no position).
        gross_bar_returns = df['gross_returns'].iloc[1:]      # skip the 0-th row since there is no weight/position yet
        win_rate = (gross_bar_returns > 0).mean() * 100
        bm_returns = df['cum_benchmark'].pct_change(fill_method=None).fillna(0).iloc[1:]
        bm_win_rate = (bm_returns > 0).mean() * 100
        # Note: the first bar is always 0% return because the first weight is reaped in the second bar, so the win rate is slightly underestimated (and so is the loss rate).

        # Loss Rate metric –– 'gross_returns' is used to avoid returns being negative due to position-entry fees when they should have been exactly 0 (ie no position).
        loss_rate = (gross_bar_returns < 0).mean() * 100
        bm_loss_rate = (bm_returns < 0).mean() * 100
        # Note: the loss rate is similarly underestimated too. 
        # Also, the loss rate is not simply (1 - win_rate) since there could be bars with exactly 0% return.
        # Zero Rate metric
        zero_rate = (gross_bar_returns == 0).mean() * 100
        bm_zero_rate = (bm_returns == 0).mean() * 100


        # QuantStats risk-adjusted metrics, using the same bar-level return series as win/loss rates.
        strategy_sharpe = sharpe(df['net_returns'])
        bm_sharpe = sharpe(bm_returns)
        strategy_adjusted_sortino = sortino(df['net_returns']) / np.sqrt(2)
        bm_adjusted_sortino = sortino(bm_returns) / np.sqrt(2)

        # Total Fees Paid (Cumulative Costs)
        total_costs = df['cumulative_costs'].iloc[-1]
        
        print(f"\n📊 --- QUICK PORTFOLIO RESULTS: {title} ---")
        print(f"   Frictionless Return:     {gross_ret:,.2f}%")
        print(f"   Net Strategy Return:     {net_ret:,.2f}%  (EW Benchmark: {bm_ret:,.2f}%)")
        print(f"   Max Drawdown:            {max_dd:,.2f}%")
        print(f"   Win Rate (Gross Bars):   {win_rate:.1f}%   (EW Benchmark: {bm_win_rate:.1f}%)")
        print(f"   Loss Rate (Gross Bars):  {loss_rate:.1f}%   (EW Benchmark: {bm_loss_rate:.1f}%)")
        print(f"   Zero Rate (Gross Bars):  {zero_rate:.1f}%    (EW Benchmark: {bm_zero_rate:.1f}%)")
        print(f"   Sharpe Ratio (Net):      {strategy_sharpe:.2f}    (EW Benchmark: {bm_sharpe:.2f})")
        print(f"   Sortino/√2 (Net):        {strategy_adjusted_sortino:.2f}    (EW Benchmark: {bm_adjusted_sortino:.2f})")
        print(f"   Total Fees Paid:         ${total_costs:,.2f}")
        print("-" * 45)

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
                benchmark=df['cum_benchmark'].resample('D').last().pct_change(fill_method=None).dropna(),
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
        bm_equity = self.initial_capital * df['cum_benchmark']
        plt.plot(df.index, bm_equity, label='EW Benchmark', color='gray', alpha=0.6)
        
        # Plot Gross vs Net Equity
        plt.plot(df.index, df['gross_equity'], label='Frictionless Equity (Gross)', color='green', alpha=0.5, linestyle='--')
        plt.plot(df.index, df['net_equity'], label='Live Equity (Net)', color='blue', linewidth=2)
        
        plt.fill_between(df.index, df['net_equity'], roll_max, color='red', alpha=0.2, label='Drawdown')
        
        plt.title(title)
        plt.ylabel("Portfolio Value ($)")
        plt.legend()
        plt.tight_layout()
        plt.show()
