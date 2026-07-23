"""
src/fees.py
Institutional Transaction Cost & Slippage Models (IBKR Pro)
"""
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger("FeeModel")

class IBKRFeeModel:
    """
    Models exact IBKR Pro Tiered routing, SEC fees, FINRA TAF, and base spread slippage.
    Current Rates As of 2024.
    """
    
    # --- REGULATORY CONSTANTS ---
    # SEC Fee: Only on SELLs. Formula: Trade Value * Rate
    SEC_FEE_RATE = 0.0000278 
    
    # FINRA Trading Activity Fee (TAF): Only on SELLs (Equities). Formula: Shares * Rate
    FINRA_TAF_RATE = 0.000166
    FINRA_TAF_MAX = 8.30
    
    def __init__(self, default_slippage_bps=1.0):
        """
        :param default_slippage_bps: Base spread crossing cost. 1.0 bps = 0.01%.
        Market impact is deferred; this purely models crossing the bid/ask spread.
        """
        self.slippage_bps = default_slippage_bps / 10000.0

    def calculate_event(self, asset_class: str, action: str, qty: float, price: float) -> dict:
        """
        Calculates precise costs for a single discrete order.
        Used by the Event Backtester and Live Engine Risk Manager.
        """
        action = action.upper()
        trade_value = qty * price
        
        commission = 0.0
        regulatory = 0.0
        
        # 1. EQUITIES (US SMART ROUTING - TIERED)
        if asset_class == 'STK':
            # Base IBKR Commission: $0.0035 per share, minimum $0.35, maximum 1% of trade value.
            base_comm = max(0.35, qty * 0.0035)
            commission = min(base_comm, trade_value * 0.01)
            
            # Regulatory Fees (Applied to SELLs only)
            if action == 'SELL':
                sec_fee = trade_value * self.SEC_FEE_RATE
                finra_fee = min(self.FINRA_TAF_MAX, qty * self.FINRA_TAF_RATE)
                regulatory = sec_fee + finra_fee
                
        # 2. FOREX
        elif asset_class == 'FX':
            # Base IBKR Commission: 0.20 basis points * trade value, minimum $2.00
            # Note: For strict reality, IBKR charges in base currency. We assume USD normalization here.
            commission = max(2.00, trade_value * 0.00002)
            
        # 3. CRYPTO (PAXOS via IBKR)
        elif asset_class == 'CRYPTO':
            # Generally 0.12% to 0.18% of trade value. Minimum $1.75.
            commission = max(1.75, trade_value * 0.0012)
            
        else:
            logger.warning(f"Unknown asset class {asset_class}. Defaulting to flat 1 bps cost.")
            commission = trade_value * 0.0001
            
        # 4. SPREAD SLIPPAGE ESTIMATE
        # Assumes we cross the spread (pay half the spread cost)
        slippage = trade_value * self.slippage_bps
        
        total_cost = commission + regulatory + slippage
        
        return {
            'commission': commission,
            'regulatory': regulatory,
            'slippage': slippage,
            'total_cost': total_cost
        }

    def calculate_vector_matrix(self, asset_class: str, share_diff: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
        """
        Matrix-compatible vectorized execution logic for N-assets. 
        Returns a DataFrame of total costs per asset per bar.
        """
        qty = share_diff.abs()
        trade_value = qty * prices
        
        # Initialize output matrix
        if isinstance(share_diff, pd.Series):
            total_costs = pd.Series(0.0, index=share_diff.index)
            commission = pd.Series(0.0, index=share_diff.index)
            regulatory = pd.Series(0.0, index=share_diff.index)
        else:
            total_costs = pd.DataFrame(0.0, index=share_diff.index, columns=share_diff.columns)
            commission = pd.DataFrame(0.0, index=share_diff.index, columns=share_diff.columns)
            regulatory = pd.DataFrame(0.0, index=share_diff.index, columns=share_diff.columns)
        
        # Boolean masks for vectorized matrix logic
        is_trade = qty > 0
        is_sell = share_diff < 0
        
        if asset_class == 'STK':
            # Commission: max(0.35, qty * 0.0035), capped at 1% of trade value
            base_comm = np.maximum(0.35, qty * 0.0035)
            commission[is_trade] = np.minimum(base_comm, trade_value * 0.01)
            
            # Regulatory (Only on Sells)
            sec_fee = trade_value * self.SEC_FEE_RATE
            finra_fee = np.minimum(self.FINRA_TAF_MAX, qty * self.FINRA_TAF_RATE)
            regulatory[is_sell] = sec_fee + finra_fee
            
        elif asset_class == 'FX':
            commission[is_trade] = np.maximum(2.00, trade_value * 0.00002)
            
        elif asset_class == 'CRYPTO':
            commission[is_trade] = np.maximum(1.75, trade_value * 0.0012)
            
        # Slippage is always applied to the gross value of the trade
        slippage = trade_value * self.slippage_bps
        
        total_costs = commission + regulatory + slippage
        
        return total_costs