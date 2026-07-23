import pandas as pd
import arcticdb as adb
import logging
from typing import List, Dict, Union
from src.config import ARCTIC_PATH

logger = logging.getLogger(__name__)

class DataStore:
    """
    The Librarian.
    Handles saving historical data and retrieving perfectly aligned matrices.
    """
    def __init__(self, library_name: str):
        try:
            self.arctic = adb.Arctic(ARCTIC_PATH)
            logger.info(f"🗄️ Connected to ArcticDB at: {ARCTIC_PATH}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to ArcticDB: {e}")
            raise

        if library_name not in self.arctic.list_libraries():
            self.arctic.create_library(library_name)
            logger.info(f"🆕 Created new library: {library_name}")
        
        self.lib = self.arctic[library_name]

    def save(self, key: str, data: pd.DataFrame):
        """Saves a DataFrame to the DB under a specific Key."""
        if data.empty:
            logger.warning(f"⚠️ Attempted to save empty data for {key}")
            return
            
        if not isinstance(data.index, pd.DatetimeIndex):
            if 'date' in data.columns:
                data.set_index('date', inplace=True)
            elif 'timestamp' in data.columns:
                data.set_index('timestamp', inplace=True)
                
        self.lib.write(key, data)
        logger.info(f"💾 Saved {len(data)} rows for {key}")

    def _fetch_raw(self, key: str, start_date=None, end_date=None) -> pd.DataFrame:
        """
        PRIVATE API: Fetches unaligned, raw data for a single instrument directly from DB.
        """
        if key not in self.lib.list_symbols():
            logger.error(f"❌ Key '{key}' not found in database.")
            return pd.DataFrame()
            
        item = self.lib.read(key)
        df = item.data
        
        if start_date:
            df = df[df.index >= start_date]
        if end_date:
            df = df[df.index <= end_date]
            
        return df

    def load(self, symbols: Union[str, List[str]], start_date=None, end_date=None) -> Dict[str, pd.DataFrame]:
        """
        PUBLIC API: Loads and perfectly aligns historical data.
        Accepts a single string or a list of strings.
        ALWAYS returns a Dictionary of N-dimensional matrices (Time x Assets),
        matching the exact contract expected by BaseStrategy and PortfolioVectorEngine.
        """
        # 1. Standardize Input
        if isinstance(symbols, str):
            symbols = [symbols]
            
        logger.info(f"🔄 Assembling universe matrices for {len(symbols)} asset(s)...")
        raw_data = {}
        
        # 2. Fetch Raw Data using the private helper
        for sym in symbols:
            df = self._fetch_raw(sym, start_date, end_date)
            if not df.empty:
                df = df.sort_index()
                df = df[~df.index.duplicated(keep='last')]
                raw_data[sym] = df
        
        if not raw_data:
            logger.error("❌ No data loaded for the requested symbols.")
            return {}

        # 3. Align into N-Dimensional Matrices
        metrics = ['open', 'high', 'low', 'close', 'volume']
        aligned_matrices = {}
        
        for metric in metrics:
            metric_df = pd.DataFrame({
                sym: df[metric] for sym, df in raw_data.items() if metric in df.columns
            })
            
            metric_df.sort_index(inplace=True)
            
            # Forward Fill prices, zero-fill volume
            if metric != 'volume':
                metric_df.ffill(inplace=True)
            else:
                metric_df.fillna(0, inplace=True)
                
            aligned_matrices[metric] = metric_df

        # 4. Clean up weekends/holidays where entire universe is NaN
        master_close = aligned_matrices['close']
        valid_idx = master_close.dropna(how='all').index
        
        for metric in metrics:
            aligned_matrices[metric] = aligned_matrices[metric].loc[valid_idx]

        logger.info(f"✅ Data aligned. Master Matrix shape: {aligned_matrices['close'].shape}")
        return aligned_matrices