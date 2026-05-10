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
IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IB_PORT", 7497))
IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", 202))
ACCOUNT_ID = os.getenv("IB_ACCOUNT", "")
IB_MARKET_DATA_TYPE = int(os.getenv("IB_MARKET_DATA_TYPE", 3))
ENABLE_ORDER_SUBMISSION = os.getenv("ENABLE_ORDER_SUBMISSION", "false").strip().lower() in {"1", "true", "yes", "on"}
STATE_DIR = Path(os.getenv("STATE_DIR", "./state"))

# --- DATABASE (ArcticDB) ---
ARCTIC_PATH = f"lmdb://{DATA_DIR}/arctic_db?map_size=100GB"
LIBS = {
    "equity_min": "equity/min",
    "fx_min": "fx/min",
    "futures_min": "futures/min",
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
BLUEGREY_UNIVERSE = os.getenv("BLUEGREY_UNIVERSE", "SPY:STK")


def parse_universe(raw: str) -> dict:
    """Parse universe strings like 'SPY:STK,QQQ:STK,EURUSD:FX' into IB contracts."""
    instruments = {}
    entries = [entry.strip() for entry in raw.split(',') if entry.strip()]

    for entry in entries:
        parts = entry.split(':')
        if len(parts) != 2:
            raise ValueError(
                f"Malformed BLUEGREY_UNIVERSE entry '{entry}'. Expected SYMBOL:ASSET_CLASS."
            )

        symbol, asset_class = parts[0].strip(), parts[1].strip().upper()
        if not symbol or not asset_class:
            raise ValueError(
                f"Malformed BLUEGREY_UNIVERSE entry '{entry}'. Expected SYMBOL:ASSET_CLASS."
            )

        instruments[symbol] = build_contract(symbol, asset_class)

    return instruments


INSTRUMENTS = parse_universe(BLUEGREY_UNIVERSE)
