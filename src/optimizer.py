"""
src/optimizer.py
Institutional Combinatorial Purged Cross-Validation (CPCV) Orchestrator.
Funnels parameterized strategies through alternative historical regimes to destroy overfitting.
"""
import itertools
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from concurrent.futures import ProcessPoolExecutor, as_completed

# Anchor imports to your infrastructure
from src.vector_backtester import VectorEngine
from src.cpcv import PurgedKFold

class CPCVOptimizer:
    def __init__(self, df: pd.DataFrame, target_col: str, strategy_class, param_grid: dict, asset_class: str, max_lookback_bars: int):
        """
        :param df: The raw historical dataframe (OHLCV). MUST have a DatetimeIndex.
        :param target_col: The column to trade (usually 'close').
        :param strategy_class: The uninstantiated class inheriting from BaseStrategy.
        :param param_grid: Dictionary of parameters to test { 'fast_window': [10, 20], 'slow_window': [50, 100] }
        :param asset_class: 'STK', 'FX', or 'CRYPTO' (Required for exact fee modeling).
        :param max_lookback_bars: The maximum memory of your strategy (e.g., 200). Used to purge data leakage.
        """
        self.df = df.copy()
        if not isinstance(self.df.index, pd.DatetimeIndex):
            raise ValueError("Dataframe MUST have a DatetimeIndex for CPCV boundary calculations.")
            
        self.target_col = target_col
        self.strategy_class = strategy_class
        self.param_grid = param_grid
        self.asset_class = asset_class
        self.max_lookback = max_lookback_bars
        self.results = []
        
        # 1. Dynamically Calibrate CPCV Boundaries
        self._calibrate_cpcv()

    def _calibrate_cpcv(self):
        """
        Reads the dataset dimensions and calculates mathematically safe boundaries.
        Prevents running valid math on invalid data sizes.
        """
        total_days = (self.df.index[-1] - self.df.index[0]).days
        
        # CONSTRAINT 1: Absolute Minimum Data Depth
        if total_days < 730: # 2 Years
            raise ValueError(f"❌ Dataset too short ({total_days} days). CPCV requires at least 2 years (730 days) to prevent regime overfitting.")
            
        # CONSTRAINT 2: Minimum 6-Months per Block
        # We want blocks of roughly 180 days to ensure macro-regime exposure.
        self.n_splits = max(4, int(total_days / 180))
        self.n_test_splits = max(2, int(self.n_splits / 3))
        
        # Calculate bars per year dynamically for Sharpe annualization
        self.bars_per_year = len(self.df) / (total_days / 365.25)
        
        # Initialize the Slicer
        self.cpcv = PurgedKFold(
            n_splits=self.n_splits, 
            n_test_splits=self.n_test_splits, 
            purge_window=self.max_lookback, 
            embargo_window=int(self.max_lookback * 0.1) # 10% embargo buffer
        )
        self.paths = list(self.cpcv.split(self.df))
        
        print(f"📐 CPCV Calibrated: {total_days} days of data.")
        print(f"   -> Splits: {self.n_splits} | Test Blocks: {self.n_test_splits}")
        print(f"   -> Combinatorial Paths: {len(self.paths)}")
        print(f"   -> Purge Window: {self.max_lookback} bars")

    def _evaluate_params(self, params: dict):
        """Evaluates a single parameter combination across ALL combinatorial paths."""
        try:
            # 1. Instantiate the Strategy Contract
            strategy = self.strategy_class(instruments={}, params=params)
            
            # 2. Generate signals across the entire continuous dataframe 
            # (Rolling metrics do not leak future data, so this is safe and extremely fast)
            target_weights = strategy.generate_signals(self.df)
            
            # 3. Execute Vector Engine to get exact Net Returns (Including Tiered IBKR Fees)
            engine = VectorEngine(
                prices=self.df[self.target_col], 
                signals=target_weights, 
                asset_class=self.asset_class,
                initial_capital=100000.0,
                execution_delay=1
            )
            res_df = engine.run()
            net_returns = res_df['net_returns']
            
            # 4. Slicer Execution: Extract Out-Of-Sample (Test) returns for each path
            path_sharpes = []
            
            for train_idx, test_idx in self.paths:
                oos_returns = net_returns.iloc[test_idx]
                
                if oos_returns.std() == 0:
                    path_sharpes.append(0.0)
                else:
                    # Annualize Sharpe based on exact bar resolution
                    sharpe = (oos_returns.mean() / oos_returns.std()) * np.sqrt(self.bars_per_year)
                    path_sharpes.append(sharpe)
                    
            path_sharpes = np.array(path_sharpes)
            
            # 5. Extract Distribution Metrics
            return {
                **params,
                'Mean_Sharpe': np.mean(path_sharpes),
                'Median_Sharpe': np.median(path_sharpes),
                'P5_Sharpe': np.percentile(path_sharpes, 5), # The Conservative Robustness Metric
                'Max_Sharpe': np.max(path_sharpes),
                'Win_Rate_Paths': (np.sum(path_sharpes > 0) / len(self.paths)) * 100
            }
            
        except Exception as e:
            print(f"⚠️ Error testing {params}: {e}")
            return None

    def run(self, parallel=True):
        """
        Ignites the Grid Search. 
        Fans out hundreds of thousands of path backtests across all CPU cores.
        """
        keys = self.param_grid.keys()
        combinations = [dict(zip(keys, prod)) for prod in itertools.product(*self.param_grid.values())]
        total_simulations = len(combinations) * len(self.paths)
        
        print(f"🚀 Igniting CPU Cluster: Testing {len(combinations)} Parameter Sets.")
        print(f"   -> Total Path Simulations: {total_simulations}")
        
        self.results = []
        
        if parallel:
            with ProcessPoolExecutor() as executor:
                futures = {executor.submit(self._evaluate_params, p): p for p in combinations}
                for i, future in enumerate(as_completed(futures)):
                    res = future.result()
                    if res: self.results.append(res)
                    if (i + 1) % 50 == 0:
                        print(f"   [{i + 1}/{len(combinations)}] parameter grids processed...")
        else:
            for i, p in enumerate(combinations):
                res = self._evaluate_params(p)
                if res: self.results.append(res)
                if (i + 1) % 50 == 0:
                    print(f"   [{i + 1}/{len(combinations)}] parameter grids processed...")
                    
        print("✅ Optimization Complete.")
        
        # Return sorted by the 5th Percentile Sharpe (The Institutional Standard)
        results_df = pd.DataFrame(self.results)
        return results_df.sort_values(by='P5_Sharpe', ascending=False).reset_index(drop=True)

    def plot_heatmap(self, x_param: str, y_param: str, metric: str = 'P5_Sharpe'):
        """Visualizes the parameter landscape to find robust, flat zones."""
        if not self.results:
            print("⚠️ No results to plot. Run optimize() first.")
            return
            
        df = pd.DataFrame(self.results)
        
        if x_param not in df.columns or y_param not in df.columns:
            print(f"⚠️ Parameters {x_param} or {y_param} not found in results.")
            return
            
        pivot_table = df.pivot(index=y_param, columns=x_param, values=metric)
        
        plt.figure(figsize=(10, 8))
        # We use a distinct colormap to visualize edge boundaries
        sns.heatmap(pivot_table, annot=True, cmap="YlGnBu", fmt=".2f")
        plt.title(f"Alpha Landscape: {metric} Distribution\n({y_param} vs {x_param})")
        plt.gca().invert_yaxis() 
        plt.tight_layout()
        plt.show()