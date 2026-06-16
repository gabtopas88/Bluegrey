import os
from pathlib import Path
from ib_async import Stock, Future, Forex, Crypto, Contract

# ==========================================
# 🏗️ BLUEGREY INFRASTRUCTURE (Static Coordinates)
# ==========================================
SRC_DIR = Path(__file__).parent.resolve()
ROOT_DIR = SRC_DIR.parent
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# --- CONNECTIVITY ---
IB_PORT = 7497
IB_CLIENT_ID = 202
ACCOUNT_ID = "" 

# --- DATABASE (ArcticDB) ---
ARCTIC_PATH = f"lmdb://{DATA_DIR}/arctic_db?map_size=100GB"
LIBS = {
    "equity_min": "equity/min",
    "fx_min": "fx/min",
    "futures_min": "futures/min",
}

# --- DATA VENDORS ---
POLYGON_API_KEY = os.environ["POLYGON_API_KEY"]

# --- STRATEGY SETTINGS ---
STRATEGY_CLASS = "KalmanPairStrategy" 
STRATEGY_PARAMS = {} 

# ==========================================
# 🏭 THE CONTRACT FACTORY
# ==========================================
def build_contract(symbol: str, asset_class: str) -> Contract:
    """Dynamically builds IBKR Contract objects based on asset class rules."""
    # Strip the Polygon "C:" prefix if it exists for FX
    clean_symbol = symbol.replace("C:", "") if asset_class == 'FX' else symbol
    
    if asset_class == 'FX':
        return Forex(clean_symbol)
    elif asset_class == 'STK':
        return Stock(clean_symbol, 'SMART', 'USD')
    elif asset_class == 'CRYPTO':
        return Crypto(clean_symbol, 'PAXOS', 'USD')
    else:
        raise ValueError(f"Unknown asset class: {asset_class}")