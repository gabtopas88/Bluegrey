import pandas as pd
import arcticdb as adb
import logging
from src.config import ARCTIC_PATH, LIBS  # <--- CRITICAL: Import the shared path

logger = logging.getLogger(__name__)

class DataStore:
    """
    The Librarian.
    Handles saving historical data and retrieving it for backtesting.
    Now strictly follows src/config.py for paths.
    """
    def __init__(self, library_name: str):
        # 1. Connect using the Central Config
        try:
            self.arctic = adb.Arctic(ARCTIC_PATH)
            logger.info(f"🗄️ Connected to ArcticDB at: {ARCTIC_PATH}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to ArcticDB: {e}")
            raise

        # 2. Ensure Library Exists
        # If the user asks for a lib that doesn't exist, we create it.
        if library_name not in self.arctic.list_libraries():
            self.arctic.create_library(library_name)
            logger.info(f"🆕 Created new library: {library_name}")
        
        self.lib = self.arctic[library_name]

    def save(self, key: str, data: pd.DataFrame):
        """
        Saves a DataFrame to the DB under a specific Key.
        """
        if data.empty:
            logger.warning(f"⚠️ Attempted to save empty data for {key}")
            return
            
        # Ensure index is datetime
        if not isinstance(data.index, pd.DatetimeIndex):
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
            
        # Read from Arctic
        item = self.lib.read(key)
        df = item.data
        
        # Filter Dates (ArcticDB slicing is faster, but this is safe for now)
        if start_date:
            df = df[df.index >= start_date]
        if end_date:
            df = df[df.index <= end_date]
            
        return df