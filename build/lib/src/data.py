import pandas as pd
import numpy as np
from ib_async import *
import logging

logger = logging.getLogger()

class DataManager:
    """
    Asset-Agnostic Data Handler.
    It takes a dictionary of {Key: Contract} and maintains a price table.
    Responsibilities:
    1. Connect to Data Streams.
    2. Clean/Normalize incoming ticks (handle NaN, delayed data).
    3. Provide a 'single source of truth' price table.
    """
    def __init__(self, ib_instance, instruments_dict):
        self.ib = ib_instance
        self.instruments = instruments_dict # Dict: {e.g. 'ASSET_A': ContractObject}
        
        # Map: IBKR Contract ID -> Config Key (e.g., 3691937 -> 'ASSET_A')
        # We map Contract ID (conId) back to our readable Key
        # This helps us know that conId 3691937 is "ASSET_A"
        self.conid_to_key = {} 
        
        # The Price Board
        self.data = pd.DataFrame(columns=list(self.instruments.keys()))

    def subscribe(self):
        logger.info("📡 Subscribing to Instruments...")
        
        for key, contract in self.instruments.items():
            # 1. Qualify the contract (Resolve conId, exchange, etc.)
            self.ib.qualifyContracts(contract)
            
            # 2. Map conId to Key for reverse lookup
            self.conid_to_key[contract.conId] = key
            
            # 3. Request Data
            self.ib.reqMktData(contract, '', False, False)
            logger.info(f"   -> Linked {key} (ID: {contract.conId})")

    def on_tick(self, tickers):
        """
        Ingests ticks for ANY asset class.
        """
        has_updates = False
        
        for t in tickers:
            # We use the ConID to find which instrument this is
            key = self.conid_to_key.get(t.contract.conId)
            if not key: continue

            price = t.marketPrice()
            
            # robust check
            if price is not None and not np.isnan(price) and price > 0:
                self.data.loc['last', key] = price
                has_updates = True
        
        return has_updates

    def get_latest_prices(self):
        return self.data.loc['last']
    
    def is_ready(self):
        # We are ready when we have at least one price for every instrument
        # (Or you can relax this logic depending on strategy needs)
        if self.data.empty: return False
        missing = self.data.loc['last'].isnull().any()
        return not missing