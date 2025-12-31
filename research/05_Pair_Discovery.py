# research/05_Pair_Discovery.py
import sys
import os
import itertools
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller, coint

# Add project root to path
sys.path.append(os.getcwd())
from src.store import DataStore
from config import INSTRUMENTS

def calculate_half_life(spread):
    """
    Calculates the Ornstein-Uhlenbeck Half-Life of a mean-reverting series.
    Small half-life = Fast reversion (Good for trading).
    """
    spread_lag = spread.shift(1)
    spread_lag.iloc[0] = spread_lag.iloc[1]
    spread_ret = spread - spread_lag
    spread_ret.iloc[0] = spread_ret.iloc[1]
    
    spread_lag2 = sm.add_constant(spread_lag)
    model = sm.OLS(spread_ret, spread_lag2)
    res = model.fit()
    
    mu = res.params[1] # Mean reversion speed
    if mu >= 0: return np.inf # Not mean reverting
    return -np.log(2) / mu

def scan_pairs():
    store = DataStore()
    keys = list(INSTRUMENTS.keys())
    
    # Load all data into memory (these are 1-min bars, so it fits in RAM)
    data_map = {}
    print(f"📥 Loading {len(keys)} assets from Vault...")
    for k in keys:
        try:
            df = store.load(k)
            # Resample to 5-min or 15-min for cointegration stability? 
            # Let's stick to 1-min but grab last 30 days.
            data_map[k] = df['close'].tail(60 * 24 * 30) 
        except Exception:
            print(f"⚠️ Missing data for {k}")

    # Generate all unique pairs
    pairs = list(itertools.combinations(data_map.keys(), 2))
    print(f"🔍 Scanning {len(pairs)} pairs for cointegration...")
    
    results = []
    
    for asset_a, asset_b in pairs:
        s1 = data_map[asset_a]
        s2 = data_map[asset_b]
        
        # Align timestamps
        df = pd.concat([s1, s2], axis=1, join='inner').dropna()
        if len(df) < 1000: continue
        
        # 1. Correlation (Simple check)
        corr = df.iloc[:,0].corr(df.iloc[:,1])
        if corr < 0.8: continue # Skip if not even correlated
        
        # 2. Cointegration Test (Engle-Granger)
        # Null Hypothesis: No cointegration. Low p-value (<0.05) means cointegrated.
        score, pvalue, _ = coint(df.iloc[:,0], df.iloc[:,1])
        
        # 3. Half-Life Calculation
        # We perform a simple OLS to get the spread first
        model = sm.OLS(df.iloc[:,0], sm.add_constant(df.iloc[:,1]))
        res = model.fit()
        hedge_ratio = res.params.iloc[1] 
        spread = df.iloc[:,0] - hedge_ratio * df.iloc[:,1]
        
        hl = calculate_half_life(spread)
        
        results.append({
            'Leg_1': asset_a,
            'Leg_2': asset_b,
            'Correlation': round(corr, 3),
            'Coint_Pvalue': round(pvalue, 5),
            'Half_Life_Bars': round(hl, 1),
            'Hedge_Ratio': round(hedge_ratio, 3)
        })

    # Output Rankings
    results_df = pd.DataFrame(results)
    # Filter for significant cointegration (p < 0.05) and reasonable half-life
    valid_pairs = results_df[
        (results_df['Coint_Pvalue'] < 0.05) & 
        (results_df['Half_Life_Bars'] > 5) &
        (results_df['Half_Life_Bars'] < 1000)
    ].sort_values(by='Coint_Pvalue')
    
    print("\n🏆 TOP COINTEGRATED PAIRS 🏆")
    print(valid_pairs.to_string(index=False))
    
    # Save to CSV for the researcher
    valid_pairs.to_csv("research/pair_candidates.csv", index=False)
    print("\n✅ Results saved to research/pair_candidates.csv")

if __name__ == "__main__":
    scan_pairs()