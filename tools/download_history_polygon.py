"""
tools/download_history_polygon.py
Institutional-Grade Multi-Asset Data Ingestion Engine (Polygon.io -> ArcticDB)
Wired with UniverseManager for Point-In-Time Liquidity Filtering
"""
import sys
import os  # -- this isn't being used
import argparse
from pathlib import Path

# --- IMPORT FIX ---
# Add the project root to sys.path so we can import 'src.config'
# We assume this script is in /Bluegrey/tools/
ROOT_DIR = Path(__file__).parent.parent.resolve()
sys.path.append(str(ROOT_DIR))

"""
Mateo's way to ensure src.config is found without assuming this script is Bluegrey/tools/ 
"""
"""
# --- import ArcticDB module for client setup ---
from arcticdb import Arctic

# --- Modules to manage directories and paths ---
from pathlib import Path # -- for handling filesystem paths
import sys # -- for managing the Python path to ensure project modules can be imported

# --- Ensure repo root is on sys.path so that src/config.py can be imported ---
ROOT_DIR = Path.cwd()

while not (ROOT_DIR / "src").exists(): # -- traverse up the directory tree until we find a directory containing 'src/'
    if ROOT_DIR.parent == ROOT_DIR: # -- if we reach the root of the filesystem without finding 'src/', raise an error and exit to avoid infinite loop
        raise RuntimeError("Could not find repo root containing 'src/'")
    ROOT_DIR = ROOT_DIR.parent

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR)) # -- add repo root to sys.path at the start (to be read first) for module imports

# --- Now safe to import project config ---
import src.config as config
"""

from datetime import date, timedelta
import logging
import pandas as pd
from polygon import RESTClient
from arcticdb import Arctic

# --- IMPORT FIX ---
ROOT_DIR = Path(__file__).parent.parent.resolve()
sys.path.append(str(ROOT_DIR))

# Import the Unified Config & The Universe Manager
from src import config
from src.universe import UniverseManager

# --- SETUP LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("ingestion.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("DataFactory")

class PolygonIngestor:
    def __init__(self, market="fx", freq="min", multiplier=1, initial_date="2020-01-01", final_date=str(date.today())):
        self.client = RESTClient(config.POLYGON_API_KEY)
        self.store = Arctic(config.ARCTIC_PATH)
        self.universe_manager = UniverseManager()  # <--- INTEGRATION POINT
        
    def _get_library(self, lib_name: str):
        """Safely retrieves or creates an ArcticDB library."""
        if lib_name not in self.store.list_libraries():
            self.store.create_library(lib_name)
            logger.info(f"Created new ArcticDB library: {lib_name}")
        return self.store[lib_name]

    def _get_smart_start_date(self, library, ticker: str, default_start: str) -> str:
        """
        Queries ArcticDB to find the last known timestamp for a ticker.
        Prevents downloading years of history we already have.
        """
        if not library.has_symbol(ticker):
            return default_start
            
        try:
            df_existing = library.read(ticker).data
            if df_existing.empty:
                return default_start
                
            last_timestamp = df_existing.index.max()
            
            # Overlap by 1 day to ensure no gaps, deduplication handles the overlap
            smart_start = (last_timestamp - timedelta(days=1)).strftime("%Y-%m-%d")
            return smart_start
            
        except Exception as e:
            logger.warning(f"Failed to resolve smart start date for {ticker}, defaulting to {default_start}. Error: {e}")
            return default_start

    def download_ticker(self, ticker: str, lib_name: str, timespan: str = "minute", 
                        multiplier: int = 1, start_date: str = "2020-01-01", end_date: str = None):
        """
        Core ingestion logic. Fetches, formats, and smartly updates ArcticDB.
        """
        if end_date is None:
            end_date = str(date.today())

        lib = self._get_library(lib_name)
        
        # SMART UPDATE
        actual_start_date = self._get_smart_start_date(lib, ticker, start_date)
        is_update = actual_start_date != start_date
        action_type = "UPDATE" if is_update else "FULL PULL"
        
        if is_update and actual_start_date >= end_date:
             print(f"⏩ {ticker} is already up to date ({end_date}). Skipping.")
             return

        try:
            aggs = []
            for a in self.client.list_aggs(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                from_=actual_start_date,
                to=end_date,
                limit=50000
            ):
                aggs.append(a)

            if not aggs:
                print(f"⚠️ No new data found for {ticker} between {actual_start_date} and {end_date}.")
                return

            df = pd.DataFrame(aggs)
            
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            cols_map = {'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume', 'vw': 'vwap', 'n': 'transactions'}
            df.rename(columns=cols_map, inplace=True)
            
            expected_cols = ['open', 'high', 'low', 'close', 'volume', 'vwap']
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = 0.0 
            
            df = df[expected_cols]
            df = df[~df.index.duplicated(keep='last')]

            if lib.has_symbol(ticker):
                lib.update(ticker, df)
                action = f"Appended {len(df)} rows"
            else:
                lib.write(ticker, df)
                action = f"Created {len(df)} rows"
            
            print(f"✅ [{action_type}] {ticker}: {action} (From: {actual_start_date}).")

        except Exception as e:
            print(f"❌ FAILED {ticker}: {e}")

    def run_batch_job(self, universe_name: str, timespan: str, multiplier: int, start_date: str, end_date: str, lib_name: str, specific_tickers: list = None):
        """
        Orchestrator for processing lists of assets.
        Now queries the UniverseManager for Point-In-Time validated tickers.
        """
        logger.info(f"--- STARTING BATCH JOB: Universe '{universe_name}' | {multiplier}-{timespan} ---")
        
        # 1. DELEGATE TICKER SELECTION TO THE UNIVERSE MANAGER
        if specific_tickers:
            logger.info("Override active: Using manually provided specific_tickers.")
            tickers = specific_tickers
        else:
            # Pass the end_date to the UniverseManager so it fetches tickers that were 
            # active and liquid specifically on that date (Point-In-Time)
            tickers = self.universe_manager.get_universe(universe_name, as_of_date=end_date)
            
        if not tickers:
            logger.error("No tickers to process. Exiting.")
            return

        logger.info(f"Data Factory received {len(tickers)} validated assets. Commencing download...")

        # 2. EXECUTE THE DOWNLOAD LOOP
        for i, ticker in enumerate(tickers):
            print(f"[{i+1}/{len(tickers)}] ", end="")
            self.download_ticker(
                ticker=ticker,
                lib_name=lib_name,
                timespan=timespan,
                multiplier=multiplier,
                start_date=start_date,
                end_date=end_date
            )

def parse_args():
    parser = argparse.ArgumentParser(description="Bluegrey Institutional Data Factory")
    
    # We replaced --market with --universe because the YAML config defines the market
    parser.add_argument("--universe", type=str, required=True, 
                        help="Name of the Universe YAML file (e.g., 'equities_liquid', 'fx_g10').")
    parser.add_argument("--lib-name", type=str, required=True, 
                        help="Target ArcticDB Library name (e.g., equities_daily, crypto_min).")
    parser.add_argument("--timespan", type=str, default="minute", choices=['minute', 'hour', 'day', 'week', 'month'], 
                        help="Bar size unit.")
    parser.add_argument("--multiplier", type=int, default=1, 
                        help="Bar size multiplier (e.g., 5 for 5-minute bars).")
    parser.add_argument("--start-date", type=str, default="2020-01-01", 
                        help="Fallback start date (YYYY-MM-DD) if data does not exist in DB.")
    parser.add_argument("--end-date", type=str, default=str(date.today()), 
                        help="End date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--tickers", type=str, nargs="+", default=None, 
                        help="Manual override: Specific tickers to download (Space separated).")
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    ingestor = PolygonIngestor()
    # ingestor = PolygonIngestor(market="equity", freq="day", multiplier=1, initial_date="2020-01-01", final_date=str(date.today()))

    logger.info(f"Ignition: Bluegrey Data Factory. Target: {config.ARCTIC_PATH}")

    ingestor.run_batch_job(
        universe_name=args.universe,
        timespan=args.timespan,
        multiplier=args.multiplier,
        start_date=args.start_date,
        end_date=args.end_date,
        lib_name=args.lib_name,
        specific_tickers=args.tickers
    )

    logger.info("Batch Job Complete.")


"""
Run this script from the command line like this:

python tools/download_history_polygon.py \
  --universe manual_equity \
  --lib-name equity.day \
  --timespan day \
  --multiplier 1 \
  --start-date 2009-01-01 \
  --end-date 2019-12-31 \
  --tickers ECH SPY QQQ DIA IWM EFA EEM

"""