import numpy as np
import pandas as pd
from strategies.base import BaseStrategy, StrategySignal

class KalmanPairsStrategy(BaseStrategy):
    """
    Dynamic Cointegration Pairs Trading via 1D Linear Kalman Filter.
    Designed for highly correlated FX pairs (e.g., AUD/USD vs NZD/USD).
    """
    def __init__(self, instruments: dict, params: dict):
        super().__init__(instruments, params)
        
        # Strategy Parameters
        self.leg_y = params.get('leg_y', 'C:AUDUSD') # The Dependent Variable
        self.leg_x = params.get('leg_x', 'C:NZDUSD') # The Independent Variable
        
        self.entry_z = params.get('entry_z', 2.0)
        self.exit_z = params.get('exit_z', 0.0)
        self.z_lookback = params.get('z_lookback', 120) # 2 hours of 1-min bars
        
        # Kalman Filter State Variances
        self.delta = params.get('delta', 1e-5) # How fast beta can adapt
        self.vt = params.get('vt', 1e-3)       # Observation noise variance

    # ==========================================
    # 🔬 RESEARCH INTERFACE (For the Optimizer)
    # ==========================================
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates target weights for both legs across the historical DataFrame.
        Expects df to contain columns for both assets: '{leg_y}_close' and '{leg_x}_close'
        """
        y = df[f'{self.leg_y}_close'].values
        x = df[f'{self.leg_x}_close'].values
        n = len(y)

        # 1. Initialize State Variables for Kalman Recursion
        beta = np.zeros(n)
        beta[0] = y[0] / x[0]  # Initialize with the exact starting ratio
        P = np.zeros(n)      # State covariance
        P[0] = 1.0           # Initial uncertainty
        
        e = np.zeros(n)      # Prediction error (The Spread)
        Q = np.zeros(n)      # Error variance
        
        wt = self.delta / (1 - self.delta)

        # 2. Vectorized Kalman Filter Pass (Fast O(N) loop)
        for t in range(1, n):
            # Prior Update
            beta_hat = beta[t-1]
            P_hat = P[t-1] + wt

            # Measurement Update
            e[t] = y[t] - (beta_hat * x[t])
            Q[t] = P_hat * (x[t]**2) + self.vt
            
            K = P_hat * x[t] / Q[t] # Kalman Gain

            # Posterior Update
            beta[t] = beta_hat + K * e[t]
            P[t] = P_hat * (1 - K * x[t])

        # 3. Dynamic Z-Scoring of the Error (Spread)
        err_series = pd.Series(e)
        rolling_mean = err_series.rolling(window=self.z_lookback).mean()
        rolling_std = err_series.rolling(window=self.z_lookback).std()
        
        # Suppress divide-by-zero warnings during low volatility
        z_scores = np.where(rolling_std > 1e-8, (err_series - rolling_mean) / rolling_std, 0)
        
        # 4. Generate State-Machine Signals
        signal_y = np.zeros(n)
        current_pos = 0

        # Path-dependent execution logic
        for t in range(n):
            z = z_scores[t]
            if pd.isna(z) or t < self.z_lookback:
                continue

            if current_pos == 0:
                if z < -self.entry_z:
                    current_pos = 1   # Long Spread
                elif z > self.entry_z:
                    current_pos = -1  # Short Spread
            elif current_pos == 1:
                if z >= self.exit_z:
                    current_pos = 0   # Close Long
            elif current_pos == -1:
                if z <= -self.exit_z:
                    current_pos = 0   # Close Short
            
            signal_y[t] = current_pos

        # 5. Construct Allocation Weights
        weights = pd.DataFrame(index=df.index)
        
        # Raw weights
        raw_weight_y = signal_y
        raw_weight_x = -signal_y * beta 
        
        # FIX 2: Normalize to 100% Gross Market Value (GMV)
        gross_exposure = np.abs(raw_weight_y) + np.abs(raw_weight_x)
        
        # Avoid division by zero when the strategy is flat (signal_y = 0)
        safe_exposure = np.where(gross_exposure > 0, gross_exposure, 1.0)
        
        weights[self.leg_y] = raw_weight_y / safe_exposure
        weights[self.leg_x] = raw_weight_x / safe_exposure
        
        return weights.fillna(0.0)
    # ==========================================
    # 🏭 PRODUCTION INTERFACE (For Live/Event)
    # ==========================================
    def on_bar(self, latest_bars: pd.DataFrame) -> StrategySignal:
        """
        To be implemented once vector research validates the hypothesis.
        Will require carrying 'beta' and 'P' in self.state to update tick-by-tick.
        """
        raise NotImplementedError("Validate via generate_signals() first.")