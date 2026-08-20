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
IB_PORT = int(os.getenv("IB_PORT", "7496"))
IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "99"))
ACCOUNT_ID = ""

# --- MARKET DATA MODE (Issue 8) ---
# reqMarketDataType code, was previously hardcoded to 3 in main.py.
#   1 = LIVE, 2 = FROZEN, 3 = DELAYED, 4 = DELAYED-FROZEN
# Defaults to 1 (LIVE) - paper accounts without live FX entitlements only work with delayed data (3)
# out of the box; set IB_MARKET_DATA_TYPE=1 for LIVE when entitled. The engine
# logs the resolved mode at boot so the parity harness knows what data the
# live session actually traded on.
IB_MARKET_DATA_TYPE = int(os.getenv("IB_MARKET_DATA_TYPE", "1"))

# --- DATABASE (ArcticDB) ---
ARCTIC_PATH = f"lmdb://{DATA_DIR}/arctic_db?map_size=100GB"
LIBS = {
    "equity_min": "equity/min",
    "fx_min": "fx/min",
    "futures_min": "futures/min",
    "etf_daily_adj": "etf/daily_adj",
}

# --- TELEMETRY (Parquet event log) ---
# Lives under data/ so it's auto-gitignored. Three streams (decisions, orders,
# fills) plus session manifests, partitioned by UTC date.
TELEMETRY_PATH = DATA_DIR / "telemetry"

# --- BACKTEST TELEMETRY (Workstream B) ---
# Backtest runs write the SAME three streams as the live engine, but into an
# isolated tree: data/backtests/{run_id}/{stream}/{YYYY-MM-DD}.parquet
#
# Isolation is not tidiness, it is correctness. TelemetryStore._append() does a
# read-modify-write on the day's Parquet file, so a backtest pointed at
# TELEMETRY_PATH would physically concatenate its rows into the live event log —
# separable only by run_id, but permanently bloating and risking the one artifact
# that has to stay pristine. Backtest trees are disposable; the live tree is not.
BACKTEST_PATH = DATA_DIR / "backtests"


# --- DATA VENDORS ---
# SECURITY: this previously carried a live Polygon key as the os.getenv default,
# which meant the secret was committed in source (and remains in git history —
# the key must be ROTATED at the vendor, not merely deleted here). .env is
# gitignored and is the correct home for it. An empty default fails loudly at
# the first API call rather than silently authenticating as someone else.
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")

# --- STRATEGY SETTINGS ---
STRATEGY_CLASS = "KalmanPairsStrategy"

# --- STRATEGY PARAMETERS ---
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
    "base_qty": 10000,                  # Base unit for the Y leg
    "min_order_qty": 1,                 # reject entry if either leg < this
    "hedge_drift_threshold_pct": 20.0,  # Boot-time hedge-ratio drift warning
}

# --- TRANSACTION COST MODEL PARAMETERS (Workstream B) ---
# Single source of truth for IBKRFeeModel construction.
#
# Previously this value was hardcoded independently in THREE places:
# VirtualBroker (event_backtester), PortfolioVectorEngine (vector_backtester),
# and the IBKRFeeModel default itself. Three copies of a number that must agree
# is three chances for them to silently disagree — and a cost assumption that
# differs between the event backtester and the optimizer invalidates any
# comparison between them.
#
# It is also an input to the run params_hash. A backtest run at 1.0 bps modelled
# slippage must never be hash-comparable to one run at 3.0 bps, or the parity
# harness would treat two materially different cost worlds as the same
# configuration.
#
# ⚠️ CALIBRATION STATUS: UNCALIBRATED. 1.0 bps is a generic flat crossing cost
# applied identically to STK / FX / CRYPTO, with no FX-specific bid/ask term.
# On a high-turnover Kalman pairs strategy this is precisely where alpha dies.
# Workstream B Tier 2 measures the real AUDUSD/NZDUSD figure against live IBKR
# fills; do NOT hand-tune this value before that measurement exists.
FEE_MODEL_PARAMS = {
    "default_slippage_bps": 1.0,
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
#
# NOTE for the params_hash: hash the EFFECTIVE mode read off the constructed
# RiskManager (risk.mode), never this raw config string. An unknown value here is
# silently downgraded to ENFORCE by the RiskManager, so hashing the config would
# record a mode that was never actually in force.
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