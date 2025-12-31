from ib_async import Stock, Future, Forex, Contract

# --- CONNECTIVITY ---
IB_PORT = 7497
IB_CLIENT_ID = 202
ACCOUNT_ID = "" 

# --- THE UNIVERSE (Asset Agnostic) ---
# We map a human-readable "Key" to a specific IBKR Contract Object.
# This allows us to trade Stocks, Futures, and FX simultaneously.

INSTRUMENTS = {
    
    # --- TECH GIANTS (The "Mag 7" & Friends) ---
    'MSFT_STK': Stock('MSFT', 'SMART', 'USD'),
    'GOOG_STK': Stock('GOOG', 'SMART', 'USD'),
    'GOOGL_STK': Stock('GOOGL', 'SMART', 'USD'), # Class arb
    'META_STK': Stock('META', 'SMART', 'USD'),
    'NVDA_STK': Stock('NVDA', 'SMART', 'USD'),
    'AMD_STK':  Stock('AMD', 'SMART', 'USD'),    # Semis pair
    
    # --- ENERGY MAJORS (Classic Mean Reversion) ---
    'XOM_STK': Stock('XOM', 'SMART', 'USD'),
    'CVX_STK': Stock('CVX', 'SMART', 'USD'),
    'OXY_STK': Stock('OXY', 'SMART', 'USD'),
    'COP_STK': Stock('COP', 'SMART', 'USD'),
    
    # --- BANKING (Interest Rate correlation) ---
    'JPM_STK': Stock('JPM', 'SMART', 'USD'),
    'BAC_STK': Stock('BAC', 'SMART', 'USD'),
    'GS_STK':  Stock('GS', 'SMART', 'USD'),
    'MS_STK':  Stock('MS', 'SMART', 'USD'),

    # --- FX (The "Aussie-Kiwi" Pair and EUR/GBP) ---
    'AUDUSD_FX': Forex('AUDUSD'),
    'NZDUSD_FX': Forex('NZDUSD'),
    'GBPUSD_FX': Forex('GBPUSD'),
    'EURUSD_FX': Forex('EURUSD'),
}
    # Example of how you WOULD add other assets (commented out for now):
    # 'ES_FUT': Future('ES', '20250320', 'CME'), 
    # 'EUR_USD': Forex('EURUSD')


# --- STRATEGY SETTINGS ---
STRATEGY_CLASS = "MomentumStrategy" # For dynamic loading later
STRATEGY_PARAMS = {
    "window": 20,
    "qty": 10
}
