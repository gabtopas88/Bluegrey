"""
src/monte_carlo.py
Institutional Risk of Ruin Engine.
Uses Bootstrap Resampling to stress-test winning algorithms against extreme sequence-of-return risk.

Two resampling modes:

  'iid'   — the original behaviour. Draws individual bars with replacement.
            Destroys ALL serial structure. Appropriate for strategies whose
            bar-to-bar returns are approximately independent.

  'block' — circular moving-block bootstrap. Draws contiguous blocks of bars
            (default 21) with replacement, preserving within-block serial
            correlation. REQUIRED for trend-following / momentum strategies:
            their losses cluster in whipsaw regimes, and their gains cluster
            in sustained trends. IID resampling scatters those clustered bars
            uniformly through time, which systematically UNDERSTATES realistic
            max drawdowns — the one number this engine exists to estimate.
            An IID risk-of-ruin report on a trend strategy is optimistic by
            construction.
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

    # --- Resampling modes ---
    METHOD_IID = 'iid'
    METHOD_BLOCK = 'block'

    def __init__(self, historical_returns: pd.Series, initial_capital: float = 1000000.0,
                 n_simulations: int = 10000, method: str = METHOD_IID, block_size: int = 21):
        """
        :param historical_returns: A pandas Series of discrete returns (either daily returns or per-trade returns).
        :param initial_capital: Starting AUM.
        :param n_simulations: Number of alternative realities to generate (Standard is 10,000).
        :param method: 'iid' (original single-bar bootstrap) or 'block' (circular
                       moving-block bootstrap). Use 'block' for any strategy whose
                       returns carry serial correlation — trend, momentum, or
                       anything with multi-day holding periods.
        :param block_size: Contiguous bars per block in 'block' mode. Default 21
                       (one trading month at daily resolution) matches the S3
                       study's label horizon; it should be at least the horizon
                       over which the strategy's returns are autocorrelated.
        """
        if historical_returns.empty or historical_returns.std() == 0:
            raise ValueError("❌ Invalid return series provided to Monte Carlo Engine.")

        method = (method or self.METHOD_IID).lower()
        if method not in (self.METHOD_IID, self.METHOD_BLOCK):
            raise ValueError(f"❌ Unknown resampling method '{method}'. Use 'iid' or 'block'.")
        if method == self.METHOD_BLOCK and block_size < 2:
            raise ValueError("❌ block_size must be >= 2 in 'block' mode (1 is just 'iid' with extra steps).")

        self.returns = historical_returns.dropna().values
        self.initial_capital = initial_capital
        self.n_simulations = n_simulations
        self.n_periods = len(self.returns)
        self.method = method
        self.block_size = int(block_size)

        if method == self.METHOD_BLOCK and self.block_size >= self.n_periods:
            raise ValueError(
                f"❌ block_size ({self.block_size}) must be smaller than the return "
                f"series ({self.n_periods} periods) — otherwise every 'simulation' "
                f"is just a rotation of the original curve."
            )
        
        # Output Matrices
        self.simulated_equity_curves = None
        self.max_drawdowns = None
        self.final_capitals = None

    def _block_resample(self) -> np.ndarray:
        """
        Circular moving-block bootstrap, fully vectorized.

        Draws random block START indices, expands each into `block_size`
        consecutive indices (wrapping circularly so every bar is an eligible
        start and no edge bars are under-sampled), concatenates blocks until
        the simulated series reaches n_periods, and truncates the tail.

        The circular wrap does splice the last bar onto the first at most once
        per wrapped block — a standard, benign artifact of the method, and a
        far smaller distortion than destroying ALL serial structure would be.
        """
        n_blocks = int(np.ceil(self.n_periods / self.block_size))

        # (n_simulations, n_blocks) random start positions
        starts = np.random.randint(0, self.n_periods, size=(self.n_simulations, n_blocks))

        # Expand every start into a contiguous run of block_size indices (circular)
        offsets = np.arange(self.block_size)
        idx = (starts[:, :, None] + offsets[None, None, :]) % self.n_periods

        # Flatten blocks into one path per simulation and truncate to length
        idx = idx.reshape(self.n_simulations, n_blocks * self.block_size)[:, :self.n_periods]

        return self.returns[idx]

    def run(self):
        """
        Executes the high-speed vectorized Bootstrap Resampling.
        """
        print(f"🎲 Igniting Monte Carlo Engine: Resampling {self.n_periods} periods across {self.n_simulations:,} alternate realities...")
        print(f"   Resampling method: {self.method.upper()}" +
              (f" (block_size={self.block_size} bars)" if self.method == self.METHOD_BLOCK else ""))

        # 1. Resampling: build the (n_simulations, n_periods) return matrix.
        if self.method == self.METHOD_BLOCK:
            randomized_returns = self._block_resample()
        else:
            # Bootstrap Resampling: Randomly pull returns from the historical array with replacement.
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
        print(f"   Resampling Method:   {self.method.upper()}" +
              (f" (block={self.block_size})" if self.method == self.METHOD_BLOCK else ""))
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