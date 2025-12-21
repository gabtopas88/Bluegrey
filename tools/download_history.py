import sys
import os
import datetime
import pandas as pd
from ib_async import *

# Add root to path to import config & store
sys.path.append(os.getcwd())
import config
from src.store import DataStore

class HistoryLoader:
    def __init__(self):
        self.ib = IB()
        self.store = DataStore()
        
    def run(self, days=30):
        print("🔌 Connecting to IBKR for Download...")
        try:
            self.ib.connect('127.0.0.1', config.IB_PORT, clientId=999) # ID 999 for tools
        except Exception as e:
            print(f"❌ Failed to connect: {e}")
            return

        # Loop through Universe
        for key, contract in config.INSTRUMENTS.items():
            print(f"📉 Downloading {key} ({contract.symbol})...")
            self.ib.qualifyContracts(contract)
            
            # Request History
            # '1 D' bar size is fast; use '1 min' for real strategies
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr=f'{days} D',
                barSizeSetting='1 min',
                whatToShow='TRADES',
                useRTH=True
            )
            
            if not bars:
                print(f"⚠️ No data found for {key}")
                continue
                
            # Convert to DataFrame
            df = util.df(bars)
            df.set_index('date', inplace=True)
            
            # Save to Vault
            self.store.save(key, df)
            print(f"✅ Saved {len(df)} rows to ArcticDB.")

        self.ib.disconnect()
        print("🏁 Download Complete.")

if __name__ == "__main__":
    loader = HistoryLoader()
    loader.run(days=5) # Download last 5 days