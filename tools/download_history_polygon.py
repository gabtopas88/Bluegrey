"""
tools/download_history_polygon.py
Institutional-Grade Data Ingestion Engine (Polygon.io -> ArcticDB)
"""
import sys
import os
from pathlib import Path

# --- IMPORT FIX ---
# Add the project root to sys.path so we can import 'src.config'
# We assume this script is in /Bluegrey/tools/
ROOT_DIR = Path(__file__).parent.parent.resolve()
sys.path.append(str(ROOT_DIR))

import logging
import pandas as pd
from datetime import date
from polygon import RESTClient
from arcticdb import Arctic

# Import the Unified Config
import src.config as config

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
    def __init__(self):
        self.client = RESTClient(config.POLYGON_API_KEY)
        self.store = Arctic(config.ARCTIC_PATH)
        self.fx_lib = self._get_library(config.LIBS["fx_min"])
        
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

    def download_ticker(self, ticker, start_year=2020):
        """
        Downloads history. Handles missing Volume/VWAP gracefully.
        Smart-Switch between WRITE (for new) and UPDATE (for existing).
        """
        try:
            # 1. Fetch Data
            aggs = []
            for a in self.client.list_aggs(
                ticker=ticker,
                multiplier=1,
                timespan="minute",
                from_=f"{start_year}-01-01",
                to=str(date.today()),
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
            if self.fx_lib.has_symbol(ticker):
                self.fx_lib.update(ticker, df)
                action = "Updated"
            else:
                self.fx_lib.write(ticker, df)
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

if __name__ == "__main__":
    ingestor = PolygonIngestor()
    logger.info(f"Ignition: Bluegrey Data Factory. Target: {config.ARCTIC_PATH}")
    ingestor.run_bulk_fx()
    logger.info("Job Complete.")