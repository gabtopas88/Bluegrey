"""
tools/test_ibkr_adjusted_daily.py  (v3 — the full damage map)

WHY V3
  V2 solved the mystery: TLT and IEF aren't missing dividend records — their
  ENTIRE broker history starts the day they moved to a new stock exchange
  (Feb 2016 and Aug 2017). That exchange move hit ~50 iShares funds, and our
  32-fund basket contains many iShares names we haven't tested yet.

  V3 therefore scans:
    1. The ENTIRE 32-fund study basket — to map every truncated fund.
    2. Five SUBSTITUTE candidates — near-identical bond funds from other
       providers (SPDR, Vanguard, Invesco) that were listed before 2008 and
       never moved exchanges. If they come back with full history, the
       free-data route stays alive by swapping the broken funds.

  It also fixes v2's false alarm: funds that hold gold, oil, or foreign cash
  pay little or no dividends, so "adjusted price = raw price" is CORRECT for
  them, not a failure. Each fund now carries a flag saying whether dividends
  are expected.

HOW TO READ THE VERDICT
  FULL         history reaches (roughly) back to the fund's birth — good.
  CAPPED       we hit our own 25-year request limit, not the data's — good.
  TRUNCATED    broker history starts years after the fund was born — the
               exchange-move problem. These funds need swapping or paid data.

HOW TO RUN
  Same as before: Gateway/TWS running, check HOST/PORT, then
      python tools/test_ibkr_adjusted_daily.py
  ~37 funds x 2 requests each -> expect it to take several minutes.
"""

import sys

import pandas as pd

try:
    from ib_async import IB, Stock, util
except ImportError:
    print("[X] ib_async is not installed in this environment. Run: pip install ib_async")
    sys.exit(1)

# ---------------------------------------------------------------
# Settings — edit these three lines if your setup differs
# ---------------------------------------------------------------
HOST = "127.0.0.1"
PORT = 7496          # 7496 TWS live | 7497 TWS paper | 4001/4002 Gateway
CLIENT_ID = 42

# The full study basket.
# Each entry: ticker -> (year listed, asset group, dividends expected?)
# "Dividends expected" = False for funds holding gold/oil/commodities/foreign
# cash, where adjusted == raw prices is the CORRECT outcome, not a failure.
UNIVERSE = {
    # --- US stocks ---
    "SPY": (1993, "US stocks", True),
    "QQQ": (1999, "US stocks", True),
    "IWM": (2000, "US stocks", True),
    "DIA": (1998, "US stocks", True),
    "MDY": (1995, "US stocks", True),
    # --- International stocks ---
    "EFA": (2001, "Intl stocks", True),
    "EEM": (2003, "Intl stocks", True),
    "EWJ": (1996, "Intl stocks", True),
    "VGK": (2005, "Intl stocks", True),
    "EWZ": (2000, "Intl stocks", True),
    "FXI": (2004, "Intl stocks", True),
    # --- Bonds & credit ---
    "TLT": (2002, "Bonds", True),
    "IEF": (2002, "Bonds", True),
    "SHY": (2002, "Bonds", True),
    "LQD": (2002, "Bonds", True),
    "HYG": (2007, "Bonds", True),
    "TIP": (2003, "Bonds", True),
    "EMB": (2007, "Bonds", True),
    # --- Commodities (no meaningful dividends) ---
    "GLD": (2004, "Commodities", False),
    "SLV": (2006, "Commodities", False),
    "USO": (2006, "Commodities", False),
    "UNG": (2007, "Commodities", False),
    "DBC": (2006, "Commodities", False),
    "DBA": (2007, "Commodities", False),
    # --- Currencies (little/no distributions in many years) ---
    "UUP": (2007, "Currencies", False),
    "FXE": (2005, "Currencies", False),
    "FXY": (2007, "Currencies", False),
    "FXB": (2006, "Currencies", False),
    "FXA": (2006, "Currencies", False),
    "FXC": (2006, "Currencies", False),
    # --- Real assets / cash reference ---
    "VNQ": (2004, "Real estate", True),
    "BIL": (2007, "Cash ref", True),
}

# Substitute candidates for the funds broken by the iShares exchange moves.
# All listed before 2008, all from providers that did NOT move exchanges.
SUBSTITUTES = {
    "SPTL": (2007, "Bonds (sub for TLT)", True),   # SPDR Long-Term Treasury
    "SPTI": (2007, "Bonds (sub for IEF)", True),   # SPDR Intermediate Treasury (was 'ITE')
    "EDV":  (2007, "Bonds (sub for TLT)", True),   # Vanguard Extended Duration
    "BSV":  (2007, "Bonds (sub for SHY)", True),   # Vanguard Short-Term Bond
    "PCY":  (2007, "Bonds (sub for EMB)", True),   # Invesco EM Sovereign Debt
}

DURATIONS = ["25 Y", "20 Y", "15 Y", "10 Y", "5 Y"]  # tried longest-first
ATTEMPTS_PER_RUNG = 2
MIN_DIVERGENCE_PCT = 2.0    # for dividend payers only
REQUEST_CEILING_YEARS = 24.5  # >= this means WE capped it at "25 Y", not the broker
TRUNCATION_SLACK_YEARS = 1.5  # allow listing-date fuzziness before crying wolf


def fetch_daily(ib: IB, contract, what_to_show: str, duration: str):
    """Ask IBKR for one long block of daily bars ending now."""
    return ib.reqHistoricalData(
        contract,
        endDateTime="",              # empty = "up to now" (REQUIRED for ADJUSTED_LAST)
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
    df = df[["date", "close"]].copy()
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date")


def fetch_deepest(ib: IB, contract, what_to_show: str):
    """Walk the duration ladder longest-first, retrying each rung once."""
    notes = []
    for duration in DURATIONS:
        for attempt in range(1, ATTEMPTS_PER_RUNG + 1):
            try:
                df = bars_to_frame(fetch_daily(ib, contract, what_to_show, duration))
                if not df.empty:
                    return df, notes
                notes.append(f"{what_to_show} {duration} try {attempt}: empty")
            except Exception as e:
                notes.append(f"{what_to_show} {duration} try {attempt}: {e}")
            ib.sleep(2)
    return pd.DataFrame(), notes


def years_of(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    return (df.index[-1] - df.index[0]).days / 365.25


def test_ticker(ib: IB, symbol: str, listed_year: int, group: str, pays_divs: bool) -> dict:
    r = {"symbol": symbol, "listed": listed_year, "group": group, "pays_divs": pays_divs,
         "raw_years": 0.0, "adj_years": 0.0, "raw_start": None,
         "divergence_pct": None, "diagnosis": "NO DATA", "notes": []}

    contract = Stock(symbol, "SMART", "USD")
    try:
        ib.qualifyContracts(contract)
    except Exception as e:
        r["notes"].append(f"qualify failed: {e}")
        print(f"   {symbol:<5} [X] could not identify contract")
        return r

    raw, notes = fetch_deepest(ib, contract, "TRADES")
    r["notes"] += notes
    r["raw_years"] = years_of(raw)
    if raw.empty:
        print(f"   {symbol:<5} [X] no raw data at all")
        return r
    r["raw_start"] = raw.index[0].date()

    ib.sleep(1)
    adj, notes = fetch_deepest(ib, contract, "ADJUSTED_LAST")
    r["notes"] += notes
    r["adj_years"] = years_of(adj)

    # Adjustment sanity (dividend payers only): adjusted price in the deep past
    # should sit clearly below the raw traded price.
    if not adj.empty:
        both = adj.join(raw, lsuffix="_adj", rsuffix="_raw", how="inner").dropna()
        if not both.empty:
            first = both.iloc[0]
            r["divergence_pct"] = (first["close_raw"] - first["close_adj"]) / first["close_raw"] * 100

    # Diagnosis
    if r["raw_years"] >= REQUEST_CEILING_YEARS:
        r["diagnosis"] = "CAPPED"      # we hit our own 25Y request limit — data may go deeper
    elif (raw.index[0].year - listed_year) > TRUNCATION_SLACK_YEARS:
        r["diagnosis"] = "TRUNCATED"   # broker history starts years after the fund was born
    else:
        r["diagnosis"] = "FULL"

    flag = {"CAPPED": "[OK]", "FULL": "[OK]", "TRUNCATED": "[!!]"}.get(r["diagnosis"], "[X]")
    print(f"   {symbol:<5} {flag} raw from {r['raw_start']} (~{r['raw_years']:.1f}y)  -> {r['diagnosis']}")
    return r


def scan(ib: IB, funds: dict, title: str):
    print(f"\n──────── Scanning: {title} ({len(funds)} funds) ────────")
    results = []
    for symbol, (listed, group, pays) in funds.items():
        results.append(test_ticker(ib, symbol, listed, group, pays))
        ib.sleep(1)
    return results


def main():
    ib = IB()
    print(f"Connecting to IBKR at {HOST}:{PORT} (clientId={CLIENT_ID})...")
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=10)
    except Exception as e:
        print(f"[X] Could not connect: {e}")
        sys.exit(1)
    print("Connected. This scan takes several minutes — go make a coffee.")

    try:
        basket = scan(ib, UNIVERSE, "study basket")
        subs = scan(ib, SUBSTITUTES, "substitute candidates")
    finally:
        ib.disconnect()

    # --------------------------- DAMAGE MAP ---------------------------
    print("\n==================== DAMAGE MAP: STUDY BASKET ====================")
    print(f"{'Ticker':<7}{'Group':<14}{'Listed':<8}{'Raw start':<12}{'Depth':>7}{'Adj OK?':>9}   Diagnosis")
    truncated, missing, adj_problems = [], [], []
    for r in basket:
        adj_ok = "n/a"
        if r["pays_divs"] and r["divergence_pct"] is not None:
            adj_ok = "yes" if r["divergence_pct"] >= MIN_DIVERGENCE_PCT else "CHECK"
            if adj_ok == "CHECK":
                adj_problems.append(r["symbol"])
        start = str(r["raw_start"]) if r["raw_start"] else "-"
        print(f"{r['symbol']:<7}{r['group']:<14}{r['listed']:<8}{start:<12}{r['raw_years']:>6.1f}y{adj_ok:>9}   {r['diagnosis']}")
        if r["diagnosis"] == "TRUNCATED":
            truncated.append(r["symbol"])
        if r["diagnosis"] == "NO DATA":
            missing.append(r["symbol"])

    print("\n==================== SUBSTITUTE CANDIDATES ====================")
    print(f"{'Ticker':<7}{'Role':<22}{'Listed':<8}{'Raw start':<12}{'Depth':>7}   Diagnosis")
    good_subs = []
    for r in subs:
        start = str(r["raw_start"]) if r["raw_start"] else "-"
        print(f"{r['symbol']:<7}{r['group']:<22}{r['listed']:<8}{start:<12}{r['raw_years']:>6.1f}y   {r['diagnosis']}")
        if r["diagnosis"] in ("FULL", "CAPPED") and r["raw_start"] and r["raw_start"].year <= 2008:
            good_subs.append(r["symbol"])

    print("\n==================== BOTTOM LINE ====================")
    if truncated:
        print(f"[!!] Truncated in the basket: {', '.join(truncated)}")
    else:
        print("[OK] No truncated funds found in the basket.")
    if missing:
        print(f"[X]  No data at all: {', '.join(missing)}")
    if adj_problems:
        print(f"[?]  Dividend adjustment looks suspicious for: {', '.join(adj_problems)}")
    if good_subs:
        print(f"[OK] Substitutes with pre-2008 history available: {', '.join(good_subs)}")
    print("\nDecision guide:")
    print("  - Every truncated fund has a good substitute above  -> free route viable (swap them in).")
    print("  - Truncated funds WITHOUT good substitutes          -> one month of a paid feed is the clean fix.")
    print("  - Either way, paste this output back to Claude for the final call.")


if __name__ == "__main__":
    main()