import numpy as np
import pandas as pd
from typing import Dict, Any, Optional
from .base import BaseStrategy, StrategySignal

class KalmanPairStrategy(BaseStrategy):
    """
    Adaptive Cointegration Strategy using a Kalman Filter.
    
    The Hypothesis:
    The relationship between Asset A (Y) and Asset B (X) is time-varying:
        Y_t = alpha_t + beta_t * X_t + epsilon_t
        
    We model the coefficients [alpha, beta] as a Random Walk process.
    The 'Innovation' (prediction error) is used as our mean-reverting signal.
    """

    def __init__(self, instruments: Dict[str, Any], params: Dict[str, Any]):
        super().__init__(instruments, params)
        
        # --- Strategy Configuration ---
        self.y_key = params.get('leg_1_key') # e.g., 'AMZN_STK'
        self.x_key = params.get('leg_2_key') # e.g., 'MSFT_STK'
        
        # Thresholds for Z-Score
        self.entry_threshold = params.get('entry_z', 2.0)
        self.exit_threshold = params.get('exit_z', 0.5)
        self.qty = params.get('base_qty', 10)

        # --- Kalman Filter Initialization ---
        # State Vector x = [intercept, slope]^T
        self.state_mean = np.zeros(2) 
        self.state_cov = np.ones((2, 2)) * 1.0  # High initial uncertainty
        
        # Process Noise Covariance (Q): How much the hedge ratio can 'drift' per tick
        # A higher delta makes the filter more adaptive but noisier.
        delta = params.get('delta', 1e-4) 
        self.Q = np.eye(2) * delta
        
        # Measurement Noise Variance (R): Inherent market noise
        self.R = params.get('R', 1e-3)

        # Current Position State
        self.position = 0 # 0: Flat, 1: Long Spread, -1: Short Spread

    def on_tick(self, data: pd.DataFrame) -> Optional[StrategySignal]:
        """
        Executes one step of the Kalman Filter and generates signals.
        """
        # 1. Data Integrity Check
        if self.y_key not in data.columns or self.x_key not in data.columns:
            return None
            
        # Get latest prices
        y_price = data[self.y_key].iloc[-1]
        x_price = data[self.x_key].iloc[-1]
        
        # 2. Kalman Filter: Prediction Step
        # Since we model coefficients as a Random Walk: x(t|t-1) = x(t-1|t-1)
        # The covariance increases by process noise Q
        state_mean_prior = self.state_mean
        state_cov_prior = self.state_cov + self.Q
        
        # 3. Kalman Filter: Observation/Update Step
        # Observation Matrix H = [1, x_price]
        H = np.array([1.0, x_price])
        
        # Expected Price of Y given X
        y_hat = np.dot(H, state_mean_prior)
        
        # Innovation (Prediction Error) -> THIS IS OUR SIGNAL SOURCE
        error = y_price - y_hat
        
        # Variance of the Innovation
        # S = H * P * H.T + R
        S = np.dot(H, np.dot(state_cov_prior, H.T)) + self.R
        
        # Kalman Gain: K = P * H.T * S^-1
        K = np.dot(state_cov_prior, H.T) / S
        
        # Update State Estimates (Posterior)
        self.state_mean = state_mean_prior + K * error
        
        # Update Covariance (Posterior)
        # P = (I - K * H) * P_prior
        self.state_cov = state_cov_prior - np.outer(K, H) @ state_cov_prior

        # 4. Signal Normalization
        # Calculate Z-Score of the error (innovation)
        # We use sqrt(S) as the standard deviation of the error
        if S <= 0: return None # Safety check
        z_score = error / np.sqrt(S)
        
        # Extract dynamic hedge ratio (beta) from state
        # state[0] = intercept, state[1] = slope (beta)
        current_beta = self.state_mean[1]

        # 5. Logic Execution
        signal = None
        
        # --- ENTRY LOGIC ---
        if self.position == 0:
            # Spread is too LOW (Y is cheap relative to X) -> BUY SPREAD
            if z_score < -self.entry_threshold:
                signal = StrategySignal("ENTRY_LONG_SPREAD")
                # Long Y, Short X (weighted by beta)
                hedge_qty = int(self.qty * abs(current_beta))
                
                signal.add_order(self.instruments[self.y_key], "BUY", self.qty)
                signal.add_order(self.instruments[self.x_key], "SELL", hedge_qty)
                
                self.position = 1

            # Spread is too HIGH (Y is expensive relative to X) -> SELL SPREAD
            elif z_score > self.entry_threshold:
                signal = StrategySignal("ENTRY_SHORT_SPREAD")
                # Short Y, Long X (weighted by beta)
                hedge_qty = int(self.qty * abs(current_beta))
                
                signal.add_order(self.instruments[self.y_key], "SELL", self.qty)
                signal.add_order(self.instruments[self.x_key], "BUY", hedge_qty)
                
                self.position = -1
        
        # --- EXIT LOGIC ---
        elif self.position != 0:
            # Mean Reversion: Z-score returns near zero
            if abs(z_score) < self.exit_threshold:
                signal = StrategySignal("EXIT_ALL")
                
                # Close all positions (In a real engine, we'd track specific open orders, 
                # but for this simplified logic we just reverse the "base" quantities 
                # or rely on the Execution Engine to flatten us. 
                # Here we simply emit a FLATTEN signal for the Execution layer.)
                # NOTE: In your current execution.py, you likely need explicit closing orders.
                
                # Close Y
                action_y = "SELL" if self.position == 1 else "BUY"
                signal.add_order(self.instruments[self.y_key], action_y, self.qty)
                
                # Close X
                # Note: This is an approximation. In production, we must track 
                # exactly how many shares of X we opened with, as Beta changes!
                # For Phase 2 Research, we re-calculate hedge_qty.
                hedge_qty = int(self.qty * abs(current_beta))
                action_x = "BUY" if self.position == 1 else "SELL"
                signal.add_order(self.instruments[self.x_key], action_x, hedge_qty)
                
                self.position = 0

        # 6. Metadata for Dashboard
        if signal:
            signal.meta = {
                'z_score': z_score,
                'beta': current_beta,
                'predicted_y': y_hat,
                'error': error
            }
            
        return signal