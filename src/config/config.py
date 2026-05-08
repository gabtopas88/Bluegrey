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
LIBS = {   # This is the mapping of market/frequency to ArcticDB library names. The download script uses this to know where to save data, and the backtester uses it to know where to load data.
    # --- FX ---
    "fx_sec": "fx.sec",
    "fx_min": "fx.min",
    "fx_hour": "fx.hour",
    "fx_day": "fx.day",
    "fx_week": "fx.week",
    "fx_month": "fx.month",
    "fx_quarter": "fx.quarter",
    "fx_year": "fx.year",

    # --- CRYPTO ---
    "crypto_sec": "crypto.sec",
    "crypto_min": "crypto.min",
    "crypto_hour": "crypto.hour",
    "crypto_day": "crypto.day",
    "crypto_week": "crypto.week",
    "crypto_month": "crypto.month",
    "crypto_quarter": "crypto.quarter",
    "crypto_year": "crypto.year",

    # --- EQUITY (stocks) ---
    "equity_sec": "equity.sec",
    "equity_min": "equity.min",
    "equity_hour": "equity.hour",
    "equity_day": "equity.day",
    "equity_week": "equity.week",
    "equity_month": "equity.month",
    "equity_quarter": "equity.quarter",
    "equity_year": "equity.year",

    # --- OPTIONS ---
    "options_sec": "options.sec",
    "options_min": "options.min",
    "options_hour": "options.hour",
    "options_day": "options.day",
    "options_week": "options.week",
    "options_month": "options.month",
    "options_quarter": "options.quarter",
    "options_year": "options.year",

    # --- INDICES ---
    "indices_sec": "indices.sec",
    "indices_min": "indices.min",
    "indices_hour": "indices.hour",
    "indices_day": "indices.day",
    "indices_week": "indices.week",
    "indices_month": "indices.month",
    "indices_quarter": "indices.quarter",
    "indices_year": "indices.year",
}


# --- DATA VENDORS ---
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "7AFgQiA1pZhVRjYfIup0LlLrZPVeyEJb")

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