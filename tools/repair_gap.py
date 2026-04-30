"""
tools/repair_gap.py
Surgical Repair Tool for ArcticDB Gaps
"""
import sys
import pandas as pd
from pathlib import Path
from polygon import RESTClient
from arcticdb import Arctic

# --- PATH SETUP ---
ROOT_DIR = Path(__file__).parent.parent.resolve()
sys.path.append(str(ROOT_DIR))
from src.config import config

# --- CONFIG ---
TARGET_TICKER = "C:EURUSD"
REPAIR_START = "2020-10-24" # Go a bit earlier the gap to ensure overlap
REPAIR_END = "2020-11-14" # Go a bit past the gap to ensure overlap

def run_repair():
    print(f"🔧 Starting Surgical Repair for {TARGET_TICKER}...")
    print(f"📅 Target Window: {REPAIR_START} to {REPAIR_END}")

    # 1. Connect
    client = RESTClient(config.POLYGON_API_KEY)
    store = Arctic(config.ARCTIC_PATH)
    lib = store[config.LIBS["fx_min"]]

    # 2. Fetch Specific Range
    aggs = []
    try:
        for a in client.list_aggs(
            ticker=TARGET_TICKER,
            multiplier=1,
            timespan="minute",
            from_=REPAIR_START,
            to=REPAIR_END,
            limit=50000
        ):
            aggs.append(a)
    except Exception as e:
        print(f"❌ API Error: {e}")
        return

    if not aggs:
        print("❌ CRITICAL: Polygon returned NO DATA for this period. The data might actually be missing from the vendor.")
        return

    # 3. Process
    df = pd.DataFrame(aggs)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    
    # Normalize Columns
    cols_map = {'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume', 'vw': 'vwap'}
    df.rename(columns=cols_map, inplace=True)
    
    # Ensure all columns exist
    for c in ['open', 'high', 'low', 'close', 'volume', 'vwap']:
        if c not in df.columns: df[c] = 0.0
    
    df = df[['open', 'high', 'low', 'close', 'volume', 'vwap']]
    df = df[~df.index.duplicated(keep='last')]

    print(f"📥 Downloaded {len(df)} bars.")

    # 4. Surgical Update (ArcticDB handles the merge/sort automatically)
    lib.update(TARGET_TICKER, df)
    print(f"✅ Patch Applied to {TARGET_TICKER}.")

if __name__ == "__main__":
    run_repair()
