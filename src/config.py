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
# IB_HOST and IB_PORT are env-driven so the Docker compose files (which set
# IB_HOST=host.docker.internal) actually take effect. Native runs default to
# localhost on the paper-trading port and behave exactly as before.
IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IB_PORT", "7497"))
IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "202"))
ACCOUNT_ID = ""

# --- MARKET DATA MODE (Issue 8) ---
# reqMarketDataType code, was previously hardcoded to 3 in main.py.
#   1 = LIVE, 2 = FROZEN, 3 = DELAYED, 4 = DELAYED-FROZEN
# Defaults to 3 (DELAYED) so paper accounts without live FX entitlements work
# out of the box; set IB_MARKET_DATA_TYPE=1 for LIVE when entitled. The engine
# logs the resolved mode at boot so the parity harness knows what data the
# live session actually traded on.
IB_MARKET_DATA_TYPE = int(os.getenv("IB_MARKET_DATA_TYPE", "3"))

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

# --- STRATEGY PARAMETERS (Issue 12) ---
# Previously {} — which silently fell back to the strategy's in-code defaults
# and could drift from whatever the backtester was run with. These are pinned
# explicitly so the live engine and the backtester read from one source of
# truth. NOTE: parity-pinning only — every value below equals the existing
# KalmanPairsStrategy default, so this is NOT a tuning change.
STRATEGY_PARAMS = {
    # Legs (must exist as keys in INSTRUMENTS below)
    "leg_y": "C:AUDUSD",                # Dependent variable
    "leg_x": "C:NZDUSD",                # Independent variable

    # Signal thresholds
    "entry_z": 2.0,
    "exit_z": 0.0,
    "z_lookback": 120,                  # 2h of 1-min bars; also warmup lookback

    # Kalman filter variances
    "delta": 1e-5,                      # How fast beta adapts
    "vt": 1e-3,                         # Observation noise variance

    # Execution sizing
    "base_qty": 1000,                   # Base unit for the Y leg
    "min_order_qty": 1,                 # Issue 11: reject entry if either leg < this
    "hedge_drift_threshold_pct": 20.0,  # Boot-time hedge-ratio drift warning
}

# --- BOOT-TIME RECONCILIATION POLICY ---
# What to do if broker positions can't be reconciled with strategy state on boot.
#   'HALT'      : Refuse to start. Require human resolution. (Safest, default)
#   'LIQUIDATE' : Flatten anomalous positions and start clean.
#   'ADOPT'     : Let strategy guess. UNSAFE — controlled testing only.
BOOT_ANOMALY_POLICY = 'HALT'

# --- RISK ENFORCEMENT MODE ---
# Controls whether the RiskManager actually vetoes / resizes orders, or merely
# observes.
#   'ENFORCE' (default): orders are gated and vol-target-resized before execution.
#   'SHADOW'           : the RiskManager evaluates and LOGS what it would do, but
#                        does NOT block or resize — orders flow through untouched.
#                        For controlled paper-trading diagnostics only. NEVER run
#                        SHADOW against real capital.
# Env override: RISK_MODE=shadow (case-insensitive). Unknown values fall back to
# ENFORCE inside the RiskManager (fail-safe). The effective mode is recorded in
# the telemetry session manifest so every run_id is self-describing.
RISK_MODE = os.getenv("RISK_MODE", "ENFORCE").upper()

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