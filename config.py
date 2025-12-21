from ib_async import Stock, Future, Forex, Contract

# --- CONNECTIVITY ---
IB_PORT = 7497
IB_CLIENT_ID = 202
ACCOUNT_ID = "" 

# --- THE UNIVERSE (Asset Agnostic) ---
# We map a human-readable "Key" to a specific IBKR Contract Object.
# This allows us to trade Stocks, Futures, and FX simultaneously.
INSTRUMENTS = {
    'AMZN_STK': Stock('AMZN', 'ISLAND', 'USD'),
    'TSLA_STK': Stock('TSLA', 'ISLAND', 'USD'),
    # Example of how you WOULD add other assets (commented out for now):
    # 'ES_FUT': Future('ES', '20250320', 'CME'), 
    # 'EUR_USD': Forex('EURUSD')
}

# --- STRATEGY SETTINGS ---
STRATEGY_CLASS = "MomentumStrategy" # For dynamic loading later
STRATEGY_PARAMS = {
    "window": 20,
    "qty": 10
}
