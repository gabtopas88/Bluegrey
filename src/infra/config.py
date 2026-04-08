import os
from pathlib import Path
from ib_async import Stock, Future, Forex, Crypto, Contract

# --- CONNECTIVITY ---
IB_PORT = 7497
IB_CLIENT_ID = 202
ACCOUNT_ID = "" 

# ==========================================
# 🌍 UNIVERSES (Easy to edit, highly scalable)
# ==========================================
# Just type the plain string tickers here. Your team can easily 
# swap these out, or eventually load them from a CSV/Database.

UNIVERSE_FX = ['EURUSD', 'GBPUSD', 'AUDUSD', 'NZDUSD', 'USDJPY']
UNIVERSE_TECH = ['MSFT', 'GOOGL', 'META', 'NVDA', 'AAPL', 'AMZN']
UNIVERSE_ENERGY = ['XOM', 'CVX', 'OXY', 'COP']

# ==========================================
# 🏭 THE CONTRACT FACTORY
# ==========================================
def build_contract(symbol: str, asset_class: str) -> Contract:
    """Dynamically builds IBKR Contract objects based on asset class rules."""
    if asset_class == 'FX':
        return Forex(symbol)
    elif asset_class == 'STK':
        return Stock(symbol, 'SMART', 'USD')
    elif asset_class == 'CRYPTO':
        return Crypto(symbol, 'PAXOS', 'USD')
    else:
        raise ValueError(f"Unknown asset class: {asset_class}")

# --- BUILD THE ACTIVE INSTRUMENT MAP DYNAMICALLY ---
# This automatically generates the dictionary your Engine expects,
# combining whatever universes you want to trade today.

INSTRUMENTS = {}
INSTRUMENTS.update({f"{sym}_FX": build_contract(sym, 'FX') for sym in UNIVERSE_FX})
INSTRUMENTS.update({f"{sym}_STK": build_contract(sym, 'STK') for sym in UNIVERSE_TECH})
# To add energy, just uncomment:
# INSTRUMENTS.update({f"{sym}_STK": build_contract(sym, 'STK') for sym in UNIVERSE_ENERGY})


# --- STRATEGY SETTINGS ---
STRATEGY_CLASS = "KalmanPairStrategy" 
STRATEGY_PARAMS = {} 

# ==========================================
# 🏗️ BLUEGREY INFRASTRUCTURE 
# ==========================================
SRC_DIR = Path(__file__).parent.resolve()
ROOT_DIR = SRC_DIR.parent
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# --- DATABASE (ArcticDB) ---
ARCTIC_PATH = f"lmdb://{DATA_DIR}/arctic_db?map_size=10GB"
LIBS = {
    "equity_min": "equity.min",
    "fx_min": "fx.min",
    "futures_min": "futures.min"
}

# --- DATA VENDORS ---
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "7AFgQiA1pZhVRjYfIup0LlLrZPVeyEJb")