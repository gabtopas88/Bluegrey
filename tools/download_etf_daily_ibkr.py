"""
tools/download_etf_daily_ibkr.py  (v2)
S3 TSMOM Study — ETF Daily Data Ingestion Engine (IBKR -> ArcticDB)

WHAT THIS DOES
  Downloads the FULL dividend-adjusted daily price history for the final
  32-fund study universe from IBKR, repairs two known data defects, stores
  everything in ArcticDB, and runs the study charter's data-quality battery
  (charter section 3.4, amended by A1/A2). Nothing gets blessed unless the
  battery passes.

WHAT CHANGED IN V2 (findings from the first ingestion run)
  1. SPTL -> EDV.  The broker's SPTL history is HOLLOW: a handful of 2007
     bars, then a ten-year hole, then data from the fund's Oct-2017 rename
     onward. EDV (Vanguard Extended Duration Treasury) has real, continuous
     history from Dec 2007 and never moved exchanges. Amendment A2.
  2. Split repair.  The broker's "adjusted" data FAILED to adjust for the
     June 9, 2005 iShares share splits (IWM 2-for-1, EFA 3-for-1). Without
     repair the strategy sees a phantom -50%/-67% crash that day. The repair
     is CONDITIONAL: applied only if the data actually shows the unadjusted
     jump, so a future upstream fix cannot cause a double-correction.
  3. Density check.  Bars-per-year for every fund. This is the check that
     would have caught SPTL's hollow history on day one — a start date can
     lie about depth; a bar count cannot.
  4. Gap and spike scans now print WHERE, and carry small whitelists of
     verified events (the 9/11 closure; the big real one-day moves we
     confirmed), so only UNEXPLAINED anomalies shout.

THE ONE POLICY THAT MATTERS: FULL OVERWRITE, NEVER APPEND
  Every new dividend slightly rewrites the ENTIRE PAST of an adjusted price
  series. Appending new days onto stored history would splice stale old data
  onto re-based new data and manufacture a fake price jump at the seam. So
  this tool always re-downloads everything and overwrites the stored copy
  (DataStore.save -> lib.write). At 32 funds of daily bars, a full refresh
  costs seconds — correctness is cheap here.

HOW TO RUN
  1. One-time: add   "etf_daily_adj": "etf/daily_adj",   to LIBS in src/config.py
  2. Start IB Gateway / TWS, check PORT below, then:
         python tools/download_etf_daily_ibkr.py
  Expect a few minutes of runtime.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# --- PATH SETUP (repo convention) ---
ROOT_DIR = Path(__file__).parent.parent.resolve()
sys.path.append(str(ROOT_DIR))

import src.config as config
from src.store import DataStore

try:
    from ib_async import IB, Stock, util
except ImportError:
    print("[X] ib_async is not installed. Run: pip install ib_async")
    sys.exit(1)

# ---------------------------------------------------------------
# Connectivity — host from config; port/client explicit here because data
# downloads often run against a different session than the trading engine.
# ---------------------------------------------------------------
IB_HOST = config.IB_HOST
IB_PORT = 7496          # 7496 TWS live | 7497 TWS paper | 4001/4002 Gateway
CLIENT_ID = 99          # dedicated data-download ID (repo convention)

LIB_NAME = config.LIBS.get("etf_daily_adj")
if LIB_NAME is None:
    LIB_NAME = "etf/daily_adj"
    print('[!] "etf_daily_adj" missing from config.LIBS — using "etf/daily_adj".')
    print('    Add    "etf_daily_adj": "etf/daily_adj",    to LIBS in src/config.py.')

# Provenance record: which source, which universe, which repairs, when.
MANIFEST_PATH = Path(config.DATA_DIR) / "etf_daily_adj_manifest.json"

# ---------------------------------------------------------------
# THE FINAL 32-FUND UNIVERSE (charter section 2, amended by A1 + A2)
# ticker -> (year listed, asset group)
# ---------------------------------------------------------------
UNIVERSE = {
    # US stocks
    "SPY": (1993, "US stocks"), "QQQ": (1999, "US stocks"), "IWM": (2000, "US stocks"),
    "DIA": (1998, "US stocks"), "MDY": (1995, "US stocks"),
    # International stocks
    "EFA": (2001, "Intl stocks"), "EEM": (2003, "Intl stocks"), "EWJ": (1996, "Intl stocks"),
    "VGK": (2005, "Intl stocks"), "EWZ": (2000, "Intl stocks"), "FXI": (2004, "Intl stocks"),
    # Bonds & credit  (A1: SPTI for IEF, BSV for SHY;  A2: EDV for TLT)
    "EDV": (2007, "Bonds"), "SPTI": (2007, "Bonds"), "BSV": (2007, "Bonds"),
    "LQD": (2002, "Bonds"), "HYG": (2007, "Bonds"), "TIP": (2003, "Bonds"),
    "EMB": (2007, "Bonds"),
    # Commodities
    "GLD": (2004, "Commodities"), "SLV": (2006, "Commodities"), "USO": (2006, "Commodities"),
    "UNG": (2007, "Commodities"), "DBC": (2006, "Commodities"), "DBA": (2007, "Commodities"),
    # Currencies
    "UUP": (2007, "Currencies"), "FXE": (2005, "Currencies"), "FXY": (2007, "Currencies"),
    "FXB": (2006, "Currencies"), "FXA": (2006, "Currencies"), "FXC": (2006, "Currencies"),
    # Real assets / cash reference
    "VNQ": (2004, "Real estate"), "BIL": (2007, "Cash ref"),
}

# Substitute-twin validation: (substitute, iShares twin, min daily-return
# correlation over the overlap). EDV's bar is looser than SPTI's because it
# holds much longer-dated bonds than TLT (it swings harder — the strategy's
# vol-scaling absorbs that). BSV's is looser because it is a slightly broader
# fund than SHY. Both disclosed in the amendments.
TWIN_CHECKS = [
    ("EDV",  "TLT", 0.90),
    ("SPTI", "IEF", 0.95),
    ("BSV",  "SHY", 0.80),
]

# Dividend canary (charter 3.4): bond funds whose adjusted price MUST sit
# clearly below the raw price in the deep past, or adjustment silently failed.
CANARY_TICKERS = ["LQD", "SPTI"]

# Known upstream adjustment failures, repaired here.
# ticker -> list of (split date, N-for-1 ratio). Verified against the
# iShares June-2005 split batch (IWM ~124->62, EFA ~158->52).
KNOWN_SPLIT_REPAIRS = {
    "IWM": [("2005-06-09", 2.0)],
    "EFA": [("2005-06-09", 3.0)],
}

# Verified REAL one-day moves > 20% (so the spike scan stops shouting about them).
VERIFIED_REAL_MOVES = {
    ("EEM", "2008-10-13"), ("EEM", "2008-10-28"),
    ("EWZ", "2008-10-13"), ("EWZ", "2020-03-16"),
    ("FXI", "2008-10-13"), ("FXI", "2008-10-28"), ("FXI", "2022-03-16"),
    ("USO", "2020-03-09"), ("USO", "2020-04-21"),
    ("VNQ", "2008-12-01"),
    ("SLV", "2026-01-30"),   # silver's worst day since 1980
    ("UNG", "2026-02-02"),   # natgas futures -25.6%, worst since 1995
}

# Known market closures that legitimately produce multi-day gaps.
KNOWN_CLOSURES = [
    ("2001-09-10", "2001-09-17", "9/11 market closure"),
]

DURATIONS = ["25 Y", "20 Y", "15 Y", "10 Y"]
ATTEMPTS_PER_RUNG = 2
COLUMNS = ["open", "high", "low", "close", "volume"]
MIN_BARS_PER_YEAR = 240      # US market ~252 trading days; below this = holes
GAP_WARN_DAYS = 5            # a normal holiday weekend is <= 4 days
SPIKE_PCT = 0.20

# NOTE on volume: IBKR has historically reported US stock volume in lots of
# 100 shares in some API versions. Stored exactly as delivered; the liquidity
# screen must sanity-check the unit before trusting it.


# ==========================================
# FETCH HELPERS
# ==========================================
def fetch_daily(ib: IB, contract, what_to_show: str, duration: str):
    return ib.reqHistoricalData(
        contract,
        endDateTime="",              # empty end = REQUIRED for ADJUSTED_LAST
        durationStr=duration,
        barSizeSetting="1 day",
        whatToShow=what_to_show,
        useRTH=True,
        formatDate=1,
    )


def bars_to_frame(bars) -> pd.DataFrame:
    df = util.df(bars)
    if df is None or df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    df = df[~df.index.duplicated(keep="last")]
    keep = [c for c in COLUMNS if c in df.columns]
    return df[keep].copy()


def fetch_deepest(ib: IB, contract, what_to_show: str) -> pd.DataFrame:
    for duration in DURATIONS:
        for _ in range(ATTEMPTS_PER_RUNG):
            try:
                df = bars_to_frame(fetch_daily(ib, contract, what_to_show, duration))
                if not df.empty:
                    return df
            except Exception:
                pass
            ib.sleep(2)
    return pd.DataFrame()


def qualified(ib: IB, symbol: str):
    contract = Stock(symbol, "SMART", "USD")
    ib.qualifyContracts(contract)
    return contract


# ==========================================
# 1. REPAIR (conditional, transparent)
# ==========================================
def repair_known_splits(symbol: str, df: pd.DataFrame):
    """
    Fix upstream split-adjustment failures. Applied ONLY when the data
    actually shows the unadjusted jump on the split date; a series that is
    already adjusted is left untouched. Returns (df, notes, ok).
    """
    notes, ok = [], True
    for split_date, ratio in KNOWN_SPLIT_REPAIRS.get(symbol, []):
        sd = pd.Timestamp(split_date)
        if sd not in df.index:
            notes.append(f"{symbol}: split date {split_date} not in data — skipped")
            continue
        pos = df.index.get_loc(sd)
        if pos == 0:
            continue
        jump = df["close"].iloc[pos] / df["close"].iloc[pos - 1]
        expected = 1.0 / ratio
        if abs(jump - expected) / expected < 0.06:
            # Data shows the raw N-for-1 split: scale everything BEFORE it.
            price_cols = [c for c in ["open", "high", "low", "close"] if c in df.columns]
            before = df.index < sd
            df.loc[before, price_cols] = df.loc[before, price_cols] / ratio
            if "volume" in df.columns:
                df.loc[before, "volume"] = df.loc[before, "volume"] * ratio
            notes.append(f"{symbol}: repaired {ratio:g}-for-1 split on {split_date} "
                         f"(pre-split prices / {ratio:g}, volume x {ratio:g})")
        elif abs(jump - 1.0) < 0.10:
            notes.append(f"{symbol}: {split_date} already adjusted upstream "
                         f"(move {(jump - 1) * 100:+.1f}%) — no repair applied")
        else:
            notes.append(f"{symbol}: UNEXPECTED move {(jump - 1) * 100:+.1f}% on {split_date} "
                         f"(expected ~{(expected - 1) * 100:.0f}% or ~0%) — NOT repaired, investigate")
            ok = False
    return df, notes, ok


# ==========================================
# 2. INGESTION (repair, then full overwrite)
# ==========================================
def ingest(ib: IB, store: DataStore):
    frames, repair_notes, repair_ok = {}, [], True
    print(f"\n──────── Ingesting {len(UNIVERSE)} funds into '{LIB_NAME}' ────────")
    for symbol, (listed, group) in UNIVERSE.items():
        try:
            df = fetch_deepest(ib, qualified(ib, symbol), "ADJUSTED_LAST")
        except Exception as e:
            print(f"   {symbol:<5} [X] failed: {e}")
            continue
        if df.empty:
            print(f"   {symbol:<5} [X] no data returned")
            continue

        df, notes, ok = repair_known_splits(symbol, df)
        repair_notes += notes
        repair_ok = repair_ok and ok

        store.save(symbol, df)          # DataStore.save -> lib.write = FULL OVERWRITE
        frames[symbol] = df
        years = (df.index[-1] - df.index[0]).days / 365.25
        density = len(df) / years if years > 0 else 0
        flag = "[OK]" if density >= MIN_BARS_PER_YEAR else "[!!]"
        print(f"   {symbol:<5} {flag} {len(df):>5} bars  {df.index[0].date()} -> {df.index[-1].date()}"
              f"  (~{years:.1f}y, {density:.0f} bars/yr)")
        ib.sleep(1)

    if repair_notes:
        print("\n   Repairs:")
        for n in repair_notes:
            print(f"     - {n}")
    return frames, repair_notes, repair_ok


# ==========================================
# 3. QA BATTERY (charter 3.4 + amendments)
# ==========================================
def qa_battery(ib: IB, frames: dict) -> bool:
    print("\n──────── QA battery ────────")
    ok = True

    # (1) Completeness + depth: every fund present, history reaches its listing.
    for symbol, (listed, group) in UNIVERSE.items():
        if symbol not in frames:
            print(f"   [FAIL] {symbol}: missing from download")
            ok = False
            continue
        df = frames[symbol]
        capped = (df.index[-1] - df.index[0]).days / 365.25 >= 24.5
        if (df.index[0].year - listed) > 1.5 and not capped:
            print(f"   [FAIL] {symbol}: history starts {df.index[0].date()} but fund listed {listed} — truncated")
            ok = False

    # (2) Density: the check that catches HOLLOW histories (the SPTL lesson).
    for symbol, df in frames.items():
        years = (df.index[-1] - df.index[0]).days / 365.25
        density = len(df) / years if years > 0 else 0
        if density < MIN_BARS_PER_YEAR:
            print(f"   [FAIL] {symbol}: only {density:.0f} bars/yr (need >= {MIN_BARS_PER_YEAR}) — history is hollow")
            ok = False

    # (3) Gaps: report WHERE, and excuse known market closures.
    for symbol, df in frames.items():
        idx = df.index.to_series()
        gaps = idx.diff().dt.days
        for end_dt, days in gaps[gaps > GAP_WARN_DAYS].items():
            start_dt = end_dt - pd.Timedelta(days=int(days))
            excuse = next((why for a, b, why in KNOWN_CLOSURES
                           if pd.Timestamp(a) <= start_dt and end_dt <= pd.Timestamp(b)), None)
            if excuse:
                print(f"   [OK]   {symbol}: {int(days)}-day gap {start_dt.date()} -> {end_dt.date()} ({excuse})")
            else:
                print(f"   [WARN] {symbol}: {int(days)}-day gap {start_dt.date()} -> {end_dt.date()} — inspect")

    # (4) Spikes: > 20% one-day moves. Verified real events pass; the rest need eyes.
    for symbol, df in frames.items():
        rets = df["close"].pct_change().dropna()
        for dt, r in rets[rets.abs() > SPIKE_PCT].items():
            key = (symbol, dt.strftime("%Y-%m-%d"))
            if key in VERIFIED_REAL_MOVES:
                print(f"   [OK]   {symbol}: {dt.date()} moved {r * 100:+.1f}% (verified real event)")
            else:
                print(f"   [WARN] {symbol}: {dt.date()} moved {r * 100:+.1f}% — UNEXPLAINED, verify")

    # (5) Price sanity.
    for symbol, df in frames.items():
        if (df["close"] <= 0).any():
            print(f"   [FAIL] {symbol}: zero/negative closes present")
            ok = False

    # (6) Dividend canary.
    for symbol in CANARY_TICKERS:
        if symbol not in frames:
            continue
        raw = fetch_deepest(ib, qualified(ib, symbol), "TRADES")
        both = frames[symbol].join(raw, lsuffix="_adj", rsuffix="_raw", how="inner").dropna()
        if both.empty:
            print(f"   [WARN] canary {symbol}: comparison unavailable")
            continue
        first = both.iloc[0]
        div = (first["close_raw"] - first["close_adj"]) / first["close_raw"] * 100
        if div < 2.0:
            print(f"   [FAIL] canary {symbol}: adjusted ~= raw in {both.index[0].year} ({div:.1f}%) — adjustment NOT applied")
            ok = False
        else:
            print(f"   [OK]   canary {symbol}: {div:.1f}% adjusted-vs-raw divergence at {both.index[0].date()}")
        ib.sleep(1)

    # (7) Twin validation: each substitute must track its iShares twin.
    for sub, twin, min_corr in TWIN_CHECKS:
        if sub not in frames:
            continue
        twin_df = fetch_deepest(ib, qualified(ib, twin), "ADJUSTED_LAST")
        if twin_df.empty:
            print(f"   [WARN] twin {twin} unavailable — cannot validate {sub}")
            continue
        pair = pd.concat(
            [frames[sub]["close"].pct_change(), twin_df["close"].pct_change()],
            axis=1, join="inner", keys=[sub, twin],
        ).dropna()
        corr = pair[sub].corr(pair[twin])
        if corr < min_corr:
            print(f"   [FAIL] {sub} vs {twin}: correlation {corr:.3f} < required {min_corr} — substitute does NOT track its twin")
            ok = False
        else:
            print(f"   [OK]   {sub} vs {twin}: correlation {corr:.3f} over {len(pair)} shared days")
        ib.sleep(1)

    return ok


def write_manifest(frames: dict, repair_notes: list, passed: bool):
    manifest = {
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "source": "IBKR reqHistoricalData whatToShow=ADJUSTED_LAST, 1 day, RTH",
        "library": LIB_NAME,
        "universe": sorted(frames.keys()),
        "data_end": max(df.index[-1] for df in frames.values()).strftime("%Y-%m-%d") if frames else None,
        "repairs_applied": repair_notes,
        "qa_passed": passed,
    }
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"   Provenance manifest written: {MANIFEST_PATH}")


def main():
    ib = IB()
    print(f"Connecting to IBKR at {IB_HOST}:{IB_PORT} (clientId={CLIENT_ID})...")
    try:
        ib.connect(IB_HOST, IB_PORT, clientId=CLIENT_ID, timeout=10)
    except Exception as e:
        print(f"[X] Could not connect: {e}")
        sys.exit(1)
    print("Connected.")

    store = DataStore(library_name=LIB_NAME)
    try:
        frames, repair_notes, repair_ok = ingest(ib, store)
        passed = qa_battery(ib, frames) and repair_ok
    finally:
        ib.disconnect()

    print("\n==================== RESULT ====================")
    print(f"Ingested {len(frames)}/{len(UNIVERSE)} funds into '{LIB_NAME}'.")
    write_manifest(frames, repair_notes, passed and len(frames) == len(UNIVERSE))
    if passed and len(frames) == len(UNIVERSE):
        print("[OK] QA battery passed. The study dataset is ready — Phase 1 can begin.")
    else:
        print("[X] QA battery raised failures — fix before any research uses this data.")
        sys.exit(1)


if __name__ == "__main__":
    main()