"""
src/monte_carlo.py
Institutional Risk of Ruin Engine.
Uses Bootstrap Resampling to stress-test winning algorithms against extreme sequence-of-return risk.
"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class MonteCarloRiskEngine:
    """
    Simulates 10,000 alternative realities of a strategy's return sequence 
    to calculate the definitive probability of hitting a terminal drawdown.
    """
    def __init__(self, historical_returns: pd.Series, initial_capital: float = 100000.0, n_simulations: int = 10000):
        """
        :param historical_returns: A pandas Series of discrete returns (either daily returns or per-trade returns).
        :param initial_capital: Starting AUM.
        :param n_simulations: Number of alternative realities to generate (Standard is 10,000).
        """
        if historical_returns.empty or historical_returns.std() == 0:
            raise ValueError("❌ Invalid return series provided to Monte Carlo Engine.")
            
        self.returns = historical_returns.dropna().values
        self.initial_capital = initial_capital
        self.n_simulations = n_simulations
        self.n_periods = len(self.returns)
        
        # Output Matrices
        self.simulated_equity_curves = None
        self.max_drawdowns = None
        self.final_capitals = None

    def run(self):
        """
        Executes the high-speed vectorized Bootstrap Resampling.
        """
        print(f"🎲 Igniting Monte Carlo Engine: Resampling {self.n_periods} periods across {self.n_simulations:,} alternate realities...")
        
        # 1. Bootstrap Resampling: Randomly pull returns from the historical array with replacement.
        # This creates a massive matrix of shape (n_simulations, n_periods) instantly.
        randomized_returns = np.random.choice(self.returns, size=(self.n_simulations, self.n_periods), replace=True)
        
        # 2. Reconstruct Equity Curves
        # We calculate the cumulative product of (1 + R) for every simulation simultaneously
        growth_factors = 1 + randomized_returns
        cumulative_growth = np.cumprod(growth_factors, axis=1)
        self.simulated_equity_curves = self.initial_capital * cumulative_growth
        
        # 3. Calculate Maximum Drawdowns for every alternate reality
        # roll_max tracks the highest peak seen so far in each simulation
        roll_max = np.maximum.accumulate(self.simulated_equity_curves, axis=1)
        drawdowns = (self.simulated_equity_curves / roll_max) - 1.0
        
        # The worst drawdown experienced in each individual simulation
        self.max_drawdowns = np.min(drawdowns, axis=1) * 100 # Convert to percentage
        
        # 4. Extract Final Capital
        self.final_capitals = self.simulated_equity_curves[:, -1]
        
        print("✅ Simulations Complete.")
        return self._generate_risk_report()

    def _generate_risk_report(self):
        """Calculates statistical probabilities of ruin."""
        
        # Core distribution metrics
        median_dd = np.median(self.max_drawdowns)
        worst_dd = np.min(self.max_drawdowns)
        p5_dd = np.percentile(self.max_drawdowns, 5) # The 5th percentile worst drawdown
        
        median_return = (np.median(self.final_capitals) / self.initial_capital - 1) * 100
        
        # Ruin Probabilities (Risk of hitting specific pain thresholds)
        prob_ruin_10 = np.sum(self.max_drawdowns <= -10.0) / self.n_simulations * 100
        prob_ruin_20 = np.sum(self.max_drawdowns <= -20.0) / self.n_simulations * 100
        prob_ruin_30 = np.sum(self.max_drawdowns <= -30.0) / self.n_simulations * 100
        
        print("\n📊 --- MONTE CARLO RISK OF RUIN REPORT ---")
        print(f"   Simulations Run:     {self.n_simulations:,}")
        print(f"   Median Expected Ret: {median_return:,.2f}%")
        print("-" * 42)
        print("   DRAWDOWN DISTRIBUTIONS:")
        print(f"   Median Max Drawdown: {median_dd:,.2f}%")
        print(f"   95% Confidence DD:   {p5_dd:,.2f}% (Expect this 1 in 20 times)")
        print(f"   Absolute Worst DD:   {worst_dd:,.2f}% (The Black Swan)")
        print("-" * 42)
        print("   PROBABILITY OF RUIN (Hitting threshold at any point):")
        print(f"   > 10% Drawdown:      {prob_ruin_10:,.1f}% probability")
        print(f"   > 20% Drawdown:      {prob_ruin_20:,.1f}% probability")
        print(f"   > 30% Drawdown:      {prob_ruin_30:,.1f}% probability")
        print("-" * 42)

    def plot_stress_test(self):
        """Visualizes the spaghetti plot of equity curves and the drawdown histogram."""
        if self.simulated_equity_curves is None:
            print("⚠️ Run simulations first.")
            return
            
        fig, axes = plt.subplots(1, 2, figsize=(16, 6))
        sns.set_theme(style="darkgrid")
        
        # --- Left Plot: The Spaghetti Chart (First 100 simulations to prevent GPU crash) ---
        ax1 = axes[0]
        ax1.plot(self.simulated_equity_curves[:100].T, color='gray', alpha=0.1)
        
        # Highlight the Median Path and the Worst Path
        median_idx = np.argsort(self.final_capitals)[self.n_simulations // 2]
        worst_idx = np.argmin(self.final_capitals)
        
        ax1.plot(self.simulated_equity_curves[median_idx], color='blue', linewidth=2, label='Median Path')
        ax1.plot(self.simulated_equity_curves[worst_idx], color='red', linewidth=2, label='Worst Case (Black Swan)')
        
        ax1.axhline(self.initial_capital, color='black', linestyle='--')
        ax1.set_title(f"Alternative Realities (Subset of 100/{self.n_simulations})")
        ax1.set_ylabel("Portfolio Value ($)")
        ax1.legend()
        
        # --- Right Plot: Drawdown Risk Histogram ---
        ax2 = axes[1]
        sns.histplot(self.max_drawdowns, bins=50, ax=ax2, color='darkred', kde=True)
        ax2.axvline(np.percentile(self.max_drawdowns, 5), color='black', linestyle='--', label='95% Confidence Limit')
        
        ax2.set_title("Distribution of Maximum Drawdowns")
        ax2.set_xlabel("Max Drawdown (%)")
        ax2.set_ylabel("Frequency (Simulations)")
        ax2.legend()
        
        plt.tight_layout()
        plt.show()