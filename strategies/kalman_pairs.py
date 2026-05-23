import numpy as np
import pandas as pd
from collections import deque
import logging
from typing import Dict

from strategies.base import BaseStrategy, StrategySignal

logger = logging.getLogger(__name__)

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
        
        # Production Execution Parameters
        self.base_qty = params.get('base_qty', 1000) # Base unit for the Y leg

        # Event-Driven State Memory
        self.state = {
            'beta': None,
            'P': 1.0,
            'errors': deque(maxlen=self.z_lookback), # O(1) rolling window
            'current_pos': 0                         # 0: Flat, 1: Long Spread, -1: Short Spread
        }

    # ==========================================
    # ⚙️ LIFECYCLE & STATE PRIMING (Matrix Dictionary Input)
    # ==========================================
    def get_warmup_lookback(self) -> int:
        """Informs the Engine of historical data requirements."""
        return self.z_lookback

    def prime_state(self, historical_data: Dict[str, pd.DataFrame]):
        """
        Fast-forwards internal Kalman state variables using a dictionary of matrices."""
        
        close_matrix = historical_data.get('close')
        
        if close_matrix is None or close_matrix.empty:
            logger.warning("Priming failed: 'close' matrix missing. Strategy will cold-start.")
            return

        if self.leg_y not in close_matrix.columns or self.leg_x not in close_matrix.columns:
            logger.error("Priming failed: Requested legs not found in historical matrix.")
            return

        logger.info(f"Priming state with {len(close_matrix)} historical bars...")
        
        y = close_matrix[self.leg_y].values
        x = close_matrix[self.leg_x].values
        n = len(y)

        # Vectorized Kalman Pass
        beta = np.zeros(n)
        beta[0] = y[0] / x[0]
        P = np.zeros(n)
        P[0] = 1.0
        e = np.zeros(n)
        Q = np.zeros(n)
        wt = self.delta / (1 - self.delta)

        for t in range(1, n):
            beta_hat = beta[t-1]
            P_hat = P[t-1] + wt
            e[t] = y[t] - (beta_hat * x[t])
            Q[t] = P_hat * (x[t]**2) + self.vt
            K = P_hat * x[t] / Q[t]
            beta[t] = beta_hat + K * e[t]
            P[t] = P_hat * (1 - K * x[t])

        # Overwrite Live State Memory
        self.state['beta'] = beta[-1]
        self.state['P'] = P[-1]
        
        # Populate the ring buffer
        lookback_slice = min(self.z_lookback, n)
        self.state['errors'].extend(e[-lookback_slice:])
        
        logger.info(f"🧠 Warm-Up Complete | Beta: {self.state['beta']:.4f} | P: {self.state['P']:.6f}")

    def reset(self):
        """Clears state if the data feed drops."""
        logger.warning("Resetting Strategy State due to gap.")
        self.state['beta'] = None
        self.state['P'] = 1.0
        self.state['errors'].clear()

    # ==========================================
    # 🔬 RESEARCH & BACKTESTING (Matrix Dictionary Input)
    # ==========================================
    def generate_signals(self, data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Calculates target weights for both legs across the historical DataFrame."""
        close_matrix = data.get('close')
        if close_matrix is None:
            raise ValueError("generate_signals requires a 'close' matrix in the data dictionary.")

        y = close_matrix[self.leg_y].values
        x = close_matrix[self.leg_x].values
        n = len(y)

        # Initialize State Variables
        beta = np.zeros(n)
        beta[0] = y[0] / x[0]  
        P = np.zeros(n)      
        P[0] = 1.0           
        e = np.zeros(n)      
        Q = np.zeros(n)      
        wt = self.delta / (1 - self.delta)

        # Vectorized Kalman Filter Pass
        for t in range(1, n):
            beta_hat = beta[t-1]
            P_hat = P[t-1] + wt
            e[t] = y[t] - (beta_hat * x[t])
            Q[t] = P_hat * (x[t]**2) + self.vt
            K = P_hat * x[t] / Q[t] 
            beta[t] = beta_hat + K * e[t]
            P[t] = P_hat * (1 - K * x[t])

        # Dynamic Z-Scoring
        err_series = pd.Series(e)
        rolling_mean = err_series.rolling(window=self.z_lookback).mean()
        rolling_std = err_series.rolling(window=self.z_lookback).std()
        
        z_scores = np.where(rolling_std > 1e-8, (err_series - rolling_mean) / rolling_std, 0)
        
        # Generate State-Machine Signals
        signal_y = np.zeros(n)
        current_pos = 0

        for t in range(n):
            z = z_scores[t]
            if pd.isna(z) or t < self.z_lookback:
                continue

            if current_pos == 0:
                if z < -self.entry_z:
                    current_pos = 1   
                elif z > self.entry_z:
                    current_pos = -1  
            elif current_pos == 1:
                if z >= self.exit_z:
                    current_pos = 0   
            elif current_pos == -1:
                if z <= -self.exit_z:
                    current_pos = 0   
            
            signal_y[t] = current_pos

        # Construct Allocation Weights
        weights = pd.DataFrame(index=close_matrix.index)
        raw_weight_y = signal_y
        raw_weight_x = -signal_y * beta 
        
        gross_exposure = np.abs(raw_weight_y) + np.abs(raw_weight_x)
        safe_exposure = np.where(gross_exposure > 0, gross_exposure, 1.0)
        
        weights[self.leg_y] = raw_weight_y / safe_exposure
        weights[self.leg_x] = raw_weight_x / safe_exposure
        
        return weights.fillna(0.0)

    # ==========================================
    # 🏭 LIVE EXECUTION / EVENT-DRIVEN (Cross-Sectional DF Input)
    # ==========================================
    def on_bar(self, latest_bars: pd.DataFrame) -> StrategySignal:
        """Executes the recursive 1D Kalman Filter on a single new cross-sectional bar."""
        if self.leg_y not in latest_bars.index or self.leg_x not in latest_bars.index:
            return StrategySignal(signal_type="AWAITING_DATA")

        # Extract closing prices safely
        y_t = latest_bars.loc[self.leg_y, 'close']
        x_t = latest_bars.loc[self.leg_x, 'close']
        
        if pd.isna(y_t) or pd.isna(x_t) or y_t <= 0 or x_t <= 0:
            return StrategySignal(signal_type="INVALID_PRICE")

        # 1. Warm-Up Initialization (If not primed via Engine)
        if self.state['beta'] is None:
            self.state['beta'] = y_t / x_t
            logger.info(f"Cold-Start Initialized Kalman Beta: {self.state['beta']:.4f}")
            return StrategySignal(signal_type="WARMUP_BETA")

        # 2. Kalman Filter Recursion
        beta_hat = self.state['beta']
        wt = self.delta / (1 - self.delta)
        P_hat = self.state['P'] + wt

        e_t = y_t - (beta_hat * x_t)
        Q_t = P_hat * (x_t**2) + self.vt
        K = P_hat * x_t / Q_t

        self.state['beta'] = beta_hat + K * e_t
        self.state['P'] = P_hat * (1 - K * x_t)

        # 3. Dynamic Z-Scoring
        self.state['errors'].append(e_t)
        
        if len(self.state['errors']) < self.z_lookback:
            return StrategySignal(signal_type="WARMUP_ZSCORE")

        errors_array = np.array(self.state['errors'])
        std_e = np.std(errors_array)
        
        if std_e < 1e-8:
            return StrategySignal(signal_type="LOW_VOLATILITY")
            
        mean_e = np.mean(errors_array)
        z = (e_t - mean_e) / std_e

        # 4. State Machine Logic
        current_pos = self.state['current_pos']
        new_pos = current_pos
        signal = StrategySignal(signal_type="FLAT", meta={'z': z, 'beta': self.state['beta']})

        # Logic: Flatten first before reversing to avoid margin spikes
        if current_pos == 0:
            if z < -self.entry_z:
                new_pos = 1   # Long Spread
            elif z > self.entry_z:
                new_pos = -1  # Short Spread
        elif current_pos == 1 and z >= self.exit_z:
            new_pos = 0       # Flatten Long
        elif current_pos == -1 and z <= -self.exit_z:
            new_pos = 0       # Flatten Short

        # 5. Order Generation 
        if new_pos != current_pos:
            signal = self._generate_orders(new_pos, current_pos, signal, y_t, x_t)
            self.state['current_pos'] = new_pos

        return signal

    def _generate_orders(self, new_pos: int, old_pos: int, base_signal: StrategySignal, price_y: float, price_x: float) -> StrategySignal:
        """
        Translates a state transition into explicit IBKR orders, routing current prices 
        for accurate Risk Manager gross exposure calculations.
        """
        base_signal.signal_type = f"TRANSITION_{old_pos}_TO_{new_pos}"
        
        contract_y = self.instruments.get(self.leg_y)
        contract_x = self.instruments.get(self.leg_x)
        
        if not contract_y or not contract_x:
            logger.error("Contracts not found in instruments mapping!")
            return base_signal
        
        # Calculate Hedge Ratio quantities (abs guarantees positive integers for IBKR)
        qty_y = int(abs(self.base_qty))
        qty_x = int(abs(self.base_qty * self.state['beta'])) 
        
        # If flatting out, reverse previous position
        if new_pos == 0:
            if old_pos == 1:
                base_signal.add_order(contract_y, action='SELL', qty=qty_y, estimated_price=price_y)
                base_signal.add_order(contract_x, action='BUY', qty=qty_x, estimated_price=price_x)
            elif old_pos == -1:
                base_signal.add_order(contract_y, action='BUY', qty=qty_y, estimated_price=price_y)
                base_signal.add_order(contract_x, action='SELL', qty=qty_x, estimated_price=price_x)
        
        # If entering new position        
        elif new_pos == 1:
            base_signal.add_order(contract_y, action='BUY', qty=qty_y, estimated_price=price_y)
            base_signal.add_order(contract_x, action='SELL', qty=qty_x, estimated_price=price_x)
            
        elif new_pos == -1:
            base_signal.add_order(contract_y, action='SELL', qty=qty_y, estimated_price=price_y)
            base_signal.add_order(contract_x, action='BUY', qty=qty_x, estimated_price=price_x)

        return base_signal