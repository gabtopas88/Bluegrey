"""
It connects to ArcticDB (or any DB) to save history and retrieve it. It creates the "Single Source of Truth" for your research.
"""

import pandas as pd
import arcticdb as adb
from datetime import datetime
import logging

logger = logging.getLogger()

class DataStore:
    """
    The Librarian.
    Handles saving historical data and retrieving it for backtesting.
    Abstracts away the specific database (ArcticDB) so you can swap it later.
    """
    def __init__(self, library_name="quant_data_v1"):
        # Connect to Local LMDB (fast, file-based)
        self.arctic = adb.Arctic("lmdb://./data/db") 
        
        # Ensure Library Exists
        if library_name not in self.arctic.list_libraries():
            self.arctic.create_library(library_name)
        
        self.lib = self.arctic[library_name]
        logger.info(f"🗄️ Connected to DataStore: {library_name}")

    def save(self, key: str, data: pd.DataFrame):
        """
        Saves a DataFrame to the DB under a specific Key (e.g., 'AMZN_STK').
        """
        if data.empty:
            logger.warning(f"⚠️ Attempted to save empty data for {key}")
            return
            
        # Ensure index is datetime for time-series operations
        if not isinstance(data.index, pd.DatetimeIndex):
            # Try to find a date column or complain
            if 'date' in data.columns:
                data.set_index('date', inplace=True)
            elif 'timestamp' in data.columns:
                data.set_index('timestamp', inplace=True)
                
        self.lib.write(key, data)
        logger.info(f"💾 Saved {len(data)} rows for {key}")

    def load(self, key: str, start_date=None, end_date=None):
        """
        Loads data for a specific instrument.
        Returns a Pandas DataFrame.
        """
        if key not in self.lib.list_symbols():
            logger.error(f"❌ Key '{key}' not found in database.")
            return None
            
        # Read from Arctic (supports date range filtering)
        # Note: ArcticDB generic read; specific range filtering depends on version
        # For simplicity, we load all and slice (LMDB is fast enough for this scale)
        item = self.lib.read(key)
        df = item.data
        
        if start_date:
            df = df[df.index >= start_date]
        if end_date:
            df = df[df.index <= end_date]
            
        return df