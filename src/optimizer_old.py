import itertools
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from concurrent.futures import ProcessPoolExecutor, as_completed
from src.vector_backtester import VectorEngine

class GridOptimizer:
    """
    Universal Grid Search Optimizer for Vectorized Strategies.
    Tests thousands of parameter combinations to find the most robust edge.
    """
    def __init__(self, prices_df: pd.DataFrame, target_col: str, signal_func, param_grid: dict, tc_bps: float = 0.2):
        """
        :param prices_df: The raw historical dataframe (OHLCV).
        :param target_col: The column to trade (usually 'close').
        :param signal_func: A user-defined function that takes (df, **params) and returns a signal series.
        :param param_grid: Dictionary of parameters to test { 'fast': [10, 20], 'slow': [50, 100] }
        """
        self.df = prices_df.copy()
        self.target_col = target_col
        self.signal_func = signal_func
        self.param_grid = param_grid
        self.tc = tc_bps
        self.results = []

    def _evaluate_params(self, params: dict):
        """Helper method to evaluate a single parameter combination."""
        try:
            # 1. Generate the signal using the custom strategy logic
            signal_series = self.signal_func(self.df.copy(), **params)
            
            # 2. Feed it to the Vector Engine
            engine = VectorEngine(
                prices=self.df[self.target_col], 
                signals=signal_series, 
                tc_bps=self.tc, 
                execution_delay=1
            )
            res_df = engine.run()
            
            # 3. Extract core metrics
            strategy_returns = res_df['net_returns'].fillna(0)
            
            if strategy_returns.std() == 0:
                return {**params, 'Total_Return_%': 0.0, 'Sharpe': 0.0, 'Max_DD_%': 0.0, 'Trades': 0}
                
            total_ret = (res_df['cum_strategy'].iloc[-1] - 1) * 100
            
            # Annualized Sharpe (Minute bars: 252 * 24 * 60)
            sharpe = (strategy_returns.mean() / strategy_returns.std()) * np.sqrt(252 * 24 * 60)
            
            roll_max = res_df['cum_strategy'].cummax()
            max_dd = ((res_df['cum_strategy'] / roll_max) - 1.0).min() * 100
            
            trades = (res_df['turnover'] > 0).sum()
            
            return {**params, 'Total_Return_%': total_ret, 'Sharpe': sharpe, 'Max_DD_%': max_dd, 'Trades': trades}
            
        except Exception as e:
            print(f"Error testing {params}: {e}")
            return None

    def run(self, parallel=False):
        """
        Runs the grid search. 
        parallel=True uses CPU multiprocessing (best for huge datasets).
        """
        # Generate all possible combinations
        keys = self.param_grid.keys()
        combinations = [dict(zip(keys, prod)) for prod in itertools.product(*self.param_grid.values())]
        
        print(f"🚀 Igniting Grid Optimizer: Testing {len(combinations)} combinations...")
        
        self.results = []
        
        if parallel:
            # Parallel execution across CPU cores
            with ProcessPoolExecutor() as executor:
                futures = {executor.submit(self._evaluate_params, p): p for p in combinations}
                for i, future in enumerate(as_completed(futures)):
                    res = future.result()
                    if res: self.results.append(res)
                    if (i + 1) % 10 == 0:
                        print(f"   [{i + 1}/{len(combinations)}] combinations tested...")
        else:
            # Sequential execution (safer for Jupyter Notebooks on Windows)
            for i, p in enumerate(combinations):
                res = self._evaluate_params(p)
                if res: self.results.append(res)
                if (i + 1) % 10 == 0:
                    print(f"   [{i + 1}/{len(combinations)}] combinations tested...")
                    
        print("✅ Optimization Complete.")
        
        # Return sorted by Sharpe Ratio
        results_df = pd.DataFrame(self.results)
        return results_df.sort_values(by='Sharpe', ascending=False).reset_index(drop=True)

    def plot_heatmap(self, x_param: str, y_param: str, metric: str = 'Sharpe'):
        """Visualizes the parameter landscape to find robust zones."""
        if not self.results:
            print("⚠️ No results to plot. Run optimize() first.")
            return
            
        df = pd.DataFrame(self.results)
        
        # Pivot the data for the heatmap
        pivot_table = df.pivot(index=y_param, columns=x_param, values=metric)
        
        plt.figure(figsize=(10, 8))
        sns.heatmap(pivot_table, annot=True, cmap="viridis", fmt=".2f")
        plt.title(f"Optimization Landscape: {metric}\n({y_param} vs {x_param})")
        plt.gca().invert_yaxis() # Keep smaller numbers at the bottom
        plt.show()