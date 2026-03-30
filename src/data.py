import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from ib_async import *
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Constants
GAP_THRESHOLD = timedelta(hours=1)  # If data stops for 1h, we consider it a 'Gap'
BAR_SIZE = timedelta(minutes=1)     # We are building 1-minute bars

@dataclass
class DataEvent:
    """
    Standardized response from the DataManager to the Engine.
    Tells the Engine what just happened.
    """
    has_new_tick: bool = False
    has_new_bar: bool = False
    gap_detected: bool = False
    gap_duration: timedelta = timedelta(0)

class DataManager:
    """
    The Gatekeeper.
    Responsibilities:
    1. Ingest raw ticks from IBKR.
    2. Aggregate ticks into 1-minute OHLC bars (for the Strategy).
    3. Detect Data Gaps (The 'Heartbeat').
    """
    def __init__(self, ib_instance, instruments_dict):
        self.ib = ib_instance
        self.instruments = instruments_dict # {Key: Contract}
        self.conid_to_key = {} 
        
        # State: The 'Last Known' Valid Prices
        self.last_prices = pd.Series(index=instruments_dict.keys(), dtype=float)
        self.last_times = pd.Series(index=instruments_dict.keys(), dtype='datetime64[ns]')
        
        # State: Bar Building (The 'Current' Candle being formed)
        # Structure: { 'ASSET_A': {'open': 100, 'high': 101, ... 'start_time': ...} }
        self.pending_bars = {} 
        
        # Output: The latest finalized bars (ready for Strategy.on_bar)
        # DataFrame Index: Instrument Keys, Columns: open, high, low, close, volume, time
        self.latest_bars = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume', 'time'])
        
        # Global Heartbeat (Tracks the very last time we heard from *any* asset)
        self.system_last_time = None
        self.current_minute = None

    def subscribe(self):
        logger.info("📡 Subscribing to Market Data...")
        for key, contract in self.instruments.items():
            self.ib.qualifyContracts(contract)
            self.conid_to_key[contract.conId] = key
            
            # Request Live Ticks
            self.ib.reqMktData(contract, '', False, False)
            
            # Initialize buffers
            self.pending_bars[key] = None
            logger.info(f"   -> Watching {key} (ID: {contract.conId})")

    def on_tick(self, tickers) -> DataEvent:
        """
        Called by Main Engine when IBKR sends new data.
        Returns a DataEvent summary.
        """
        event = DataEvent()
        # Identify the latest timestamp in this batch of ticks
        batch_latest_time = None

        for t in tickers:
            key = self.conid_to_key.get(t.contract.conId)
            if not key: continue

            price = t.marketPrice()
            tick_time = t.time  # datetime object from IB
            
            # Validations
            if not tick_time or price is None or np.isnan(price) or price <= 0:
                continue
            if not batch_latest_time or tick_time > batch_latest_time:
                batch_latest_time = tick_time
                
            # 1. GAP DETECTION (Heartbeat)
            # We check if this new tick is significantly later than the last system activity
            if self.system_last_time:
                delta = tick_time - self.system_last_time
                if delta > GAP_THRESHOLD:
                    logger.warning(f"⚠️ DATA GAP DETECTED: {delta} elapsed since last tick.")
                    event.gap_detected = True
                    event.gap_duration = delta
                    # We accept the new time as the new baseline
                    
            self.system_last_time = tick_time
            self.last_times[key] = tick_time
            self.last_prices[key] = price
            event.has_new_tick = True

            # Update the pending bar for this specific asset
            self._update_pending_bar(key, price, tick_time)
        
        # --- THE SYSTEM CLOCK ---
        # Guarantees the strategy never receives misaligned timestamps
        if batch_latest_time:
            tick_minute = batch_latest_time.replace(second=0, microsecond=0)
            
            if self.current_minute is None:
                self.current_minute = tick_minute
                
            # If the global clock rolled over to a new minute...
            elif tick_minute > self.current_minute:
                self._finalize_all_bars()
                self.current_minute = tick_minute
                event.has_new_bar = True

        return event

    def _update_pending_bar(self, key, price, time):
        """Updates the running candle. Does NOT finalize it."""
        current_bar = self.pending_bars.get(key)

        if current_bar is None:
            self.pending_bars[key] = {
                'start_time': self.current_minute if self.current_minute else time.replace(second=0, microsecond=0),
                'open': price, 'high': price, 'low': price, 'close': price, 'volume': 1
            }
        else:
            current_bar['high'] = max(current_bar['high'], price)
            current_bar['low'] = min(current_bar['low'], price)
            current_bar['close'] = price
            current_bar['volume'] += 1

    def _finalize_all_bars(self):
        """
        FIX: Sweeps all pending bars across ALL assets simultaneously 
        and commits them to the latest_bars dataframe.
        """
        for key, bar_dict in self.pending_bars.items():
            if bar_dict is not None:
                self.latest_bars.loc[key] = [
                    bar_dict['open'], bar_dict['high'], bar_dict['low'], 
                    bar_dict['close'], bar_dict['volume'], bar_dict['start_time']
                ]
        # Reset pending bars for the next minute
        for key in self.pending_bars.keys():
            self.pending_bars[key] = None

    def get_latest_prices(self):
        """Used for On_Tick execution logic"""
        return self.last_prices.copy()

    def get_latest_bars(self):
        """Used for On_Bar math logic"""
        return self.latest_bars.copy()
    
    def is_ready(self):
        # Ready if we have at least one price for every asset
        return not self.last_prices.isnull().any()