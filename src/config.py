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

# --- TELEMETRY (Parquet event log) ---
# Lives under data/ so it's auto-gitignored. Three streams (decisions, orders,
# fills) plus session manifests, partitioned by UTC date.
TELEMETRY_PATH = DATA_DIR / "telemetry"


# --- DATA VENDORS ---
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "7AFgQiA1pZhVRjYfIup0LlLrZPVeyEJb")

# --- STRATEGY SETTINGS ---
STRATEGY_CLASS = "KalmanPairsStrategy"
STRATEGY_PARAMS = {}

# --- BOOT-TIME RECONCILIATION POLICY ---
# What to do if broker positions can't be reconciled with strategy state on boot.
#   'HALT'      : Refuse to start. Require human resolution. (Safest, default)
#   'LIQUIDATE' : Flatten anomalous positions and start clean.
#   'ADOPT'     : Let strategy guess. UNSAFE — controlled testing only.
BOOT_ANOMALY_POLICY = 'HALT'

# ==========================================
# 🏭 THE CONTRACT FACTORY
# ==========================================
def build_contract(symbol: str, asset_class: str) -> Contract:
    """Dynamically builds IBKR Contract objects based on asset class rules."""
    clean_symbol = symbol.replace("C:", "") if asset_class == 'FX' else symbol

    if asset_class == 'FX':
        return Forex(clean_symbol)
    elif asset_class == 'STK':
        return Stock(clean_symbol, 'SMART', 'USD')
    elif asset_class == 'CRYPTO':
        return Crypto(clean_symbol, 'PAXOS', 'USD')
    else:
        raise ValueError(f"Unknown asset class: {asset_class}")
    
# ==========================================
# 🌍 TRADING UNIVERSE
# ==========================================
# The instrument dictionary consumed by every engine component:
#   - DataManager subscribes to these contracts
#   - PortfolioManager qualifies them and uses conIds for reconciliation
#   - The strategy receives this dict via __init__ and uses it for order construction
# Keys must match what the strategy expects (e.g., KalmanPairsStrategy defaults
# to 'C:AUDUSD' / 'C:NZDUSD' for leg_y / leg_x).
INSTRUMENTS = {
    'C:AUDUSD': build_contract('AUDUSD', 'FX'),
    'C:NZDUSD': build_contract('NZDUSD', 'FX'),
}