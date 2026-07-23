"""
tools/download_history_polygon.py
Institutional-Grade Data Ingestion Engine (Polygon.io -> ArcticDB)
"""
import sys
# import os  # -- this isn't being used
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

import logging
import pandas as pd
from datetime import date
from polygon import RESTClient
from arcticdb import Arctic

# Import the Unified Config
from src import config

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
    def __init__(
        self, 
        market="fx", 
        freq="min", 
        multiplier=1, 
        initial_date="2020-01-01", 
        final_date=str(date.today())
    ):
        self.freq = freq
        self.multiplier = multiplier
        self.initial_date = initial_date
        self.final_date = final_date
        self.client = RESTClient(config.POLYGON_API_KEY)
        self.store = Arctic(config.ARCTIC_PATH)

        FREQ_MAP = {
            "sec": "second",
            "min": "minute",
            "hour": "hour",
            "day": "day",
            "week": "week",
            "month": "month",
            "quarter": "quarter",
            "year": "year"
        }
        if freq not in FREQ_MAP:
            raise ValueError(f"Unsupported freq: {freq}. Supported: {list(FREQ_MAP.keys())}")
        
        MARKET_MAP = {
            "fx": "fx",
            "crypto": "crypto",
            "equity": "stocks",
            "options": "otc",
            "indices": "indices"
        }
        if market not in MARKET_MAP:
            raise ValueError(f"Unsupported market: {market}. Supported: {list(MARKET_MAP.keys())}")

        self.timespan = FREQ_MAP[freq]
        self.market = MARKET_MAP[market]
        
        self.lib = self._get_library(config.LIBS[f"{market}_{freq}"])



        
    def _get_library(self, lib_name):
        if lib_name not in self.store.list_libraries():
            self.store.create_library(lib_name)
            logger.info(f"Created ArcticDB library: {lib_name}")
        return self.store[lib_name]

    def fetch_all_fx_tickers(self):
        """
        Dynamically discovers ALL active forex pairs from Polygon.
        (Use this if you want everything including exotics).
        """
        logger.info("Querying Polygon for ALL active Currency Pairs...")
        tickers = []
        try:
            # Iterate through all tickers where market is 'fx' and active is True
            for t in self.client.list_tickers(market="fx", active=True, limit=1000):
                tickers.append(t.ticker)
            logger.info(f"Discovery Complete: Found {len(tickers)} active FX pairs.")
            return tickers
        except Exception as e:
            logger.critical(f"Failed to fetch ticker list: {e}")
            return []
        
    def fetch_all_tickers(self):
        logger.info(f"Querying Polygon for all active {self.market} tickers...")
        tickers = []
        try:
            # Iterate through all tickers where market is 'self.market' and active is True
            for t in self.client.list_tickers(market=self.market, active=True, limit=1000):
                tickers.append(t.ticker)
            logger.info(f"Discovery Complete: Found {len(tickers)} active {self.market} tickers.")
            return tickers
        except Exception as e:
            logger.critical(f"Failed to fetch ticker list: {e}")
            return []

    def fetch_liquid_fx_tickers(self):
        """
        Discovers FX pairs, but ONLY keeps those composed of Liquid Currencies.
        Filters out exotic noise (e.g., AED, BHD, etc.)
        """
        # The 'Liquid Club' - Top traded currencies by volume
        LIQUID_CURRENCIES = {
            'USD', 'EUR', 'JPY', 'GBP', 'AUD', 'CAD', 'CHF', 'NZD', 
            'SGD', 'HKD', 'SEK', 'NOK', 'MXN', 'ZAR'
        }
        
        logger.info("Querying Polygon for FX pairs (Liquid Filter Active)...")
        valid_tickers = []
        
        try:
            # Fetch EVERYTHING first
            all_tickers = self.client.list_tickers(market="fx", active=True, limit=1000)
            
            for t in all_tickers:
                # Format is usually 'C:EURUSD'
                symbol = t.ticker
                
                # Safety check on length (must be C:XXXYYY -> 8 chars)
                if len(symbol) != 8 or not symbol.startswith("C:"):
                    continue
                
                base = symbol[2:5]
                quote = symbol[5:8]
                
                # THE FILTER: Both sides must be liquid
                if base in LIQUID_CURRENCIES and quote in LIQUID_CURRENCIES:
                    valid_tickers.append(symbol)
            
            logger.info(f"Filter Complete: Reduced universe from 1000+ to {len(valid_tickers)} high-quality pairs.")
            return valid_tickers

        except Exception as e:
            logger.critical(f"Failed to fetch ticker list: {e}")
            return []

    def download_ticker(self, ticker):
        """
        Downloads history. Handles missing Volume/VWAP gracefully.
        Smart-Switch between WRITE (for new) and UPDATE (for existing).
        """
        
        start_date=self.initial_date
        end_date=self.final_date
        
        try:
            # 1. Fetch Data
            aggs = []
            for a in self.client.list_aggs(
                ticker=ticker,
                multiplier=self.multiplier,
                timespan=self.timespan,
                from_=start_date,
                to=end_date,
                limit=50000
            ):
                aggs.append(a)

            if not aggs:
                # logger.warning(f"    No data found for {ticker}")
                return

            df = pd.DataFrame(aggs)
            
            # 2. Format Data
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            cols_map = {'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume', 'vw': 'vwap'}
            df.rename(columns=cols_map, inplace=True)
            
            # Fill missing columns with 0.0 to prevent crashes on exotic pairs
            expected_cols = ['open', 'high', 'low', 'close', 'volume', 'vwap']
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = 0.0 
            
            df = df[expected_cols]
            df = df[~df.index.duplicated(keep='last')]

            # 3. Write to Vault
            if self.lib.has_symbol(ticker):
                self.lib.update(ticker, df)
                action = "Updated"
            else:
                self.lib.write(ticker, df)
                action = "Created"
            
            print(f"✅ {action} {ticker}: {len(df)} bars.")

        except Exception as e:
            # Catch-all to keep the loop running even if one pair fails
            print(f"❌ FAILED {ticker}: {e}")

    def run_bulk_fx(self):
        """
        Main execution flow for Bulk FX Download.
        """
        # --- SELECT UNIVERSE MODE HERE ---
        all_tickers = self.fetch_liquid_fx_tickers()   # OPTION A: Institutional Filter (Current)
        # all_tickers = self.fetch_all_fx_tickers()    # OPTION B: Download Everything (Uncomment to use)
        
        if not all_tickers:
            logger.error("No tickers to process. Exiting.")
            return

        logger.info(f"Starting Batch Job for {len(all_tickers)} pairs.")
        
        for i, ticker in enumerate(all_tickers):
            print(f"[{i+1}/{len(all_tickers)}] ", end="")
            self.download_ticker(ticker)

    def run_bulk(self):
        """
        Main execution flow for Bulk market data Download.
        """
        if self.market == "fx":
            all_tickers = self.fetch_liquid_fx_tickers()   # OPTION A: Institutional Filter (Current)
            # all_tickers = self.fetch_all_fx_tickers()    # OPTION B: Download Everything (Uncomment to use)
        # elif self.market == "crypto":
        else:
            all_tickers = self.fetch_all_tickers()
        # else:
        #     raise ValueError(f"Unsupported market: {self.market}")

        if not all_tickers:
            logger.error("No tickers to process. Exiting.")
            return

        logger.info(f"Starting Batch Job for {len(all_tickers)} {self.market} tickers.")

        for i, ticker in enumerate(all_tickers):
            print(f"[{i+1}/{len(all_tickers)}] ", end="")
            self.download_ticker(ticker)

if __name__ == "__main__":
    
    # ingestor = PolygonIngestor()  # -- default is FX with liquid filter
    ingestor = PolygonIngestor(market="indices", freq="day", initial_date="2026-04-01", final_date=str(date.today()))  # -- example for crypto
    
    logger.info(f"\nIgnition: Bluegrey Data Factory. Target: {config.ARCTIC_PATH}")
    
    # ingestor.run_bulk_fx()  # -- use this for FX with the liquid filter
    ingestor.run_bulk()
    
    logger.info("Job Complete.")