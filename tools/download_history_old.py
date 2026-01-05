import sys
import os
import datetime
import pandas as pd
from ib_async import *

# Add root to path to import config & store
sys.path.append(os.getcwd())
import src.config as config
from src.store import DataStore

class HistoryLoader:
    def __init__(self):
        self.ib = IB()
        self.store = DataStore()
        
    def run(self, days=60):
        print("🔌 Connecting to IBKR for Download...")
        try:
            self.ib.connect('127.0.0.1', config.IB_PORT, clientId=999) # ID 999 for tools

            # CRITICAL FIX 1: Request Delayed Data
            # If you don't have paid subscriptions, this is required.
            self.ib.reqMarketDataType(3) 
            print("✅ Connected. Switched to Delayed Data mode.")

        except Exception as e:
            print(f"❌ Failed to connect: {e}")
            return

        # Loop through Universe
        for key, contract in config.INSTRUMENTS.items():
            print(f"📉 Downloading {key} ({contract.symbol})...")

            # RETRY LOOP: Try up to 3 times per stock
            success = False
            for attempt in range(1, 4):
                try:
                    self.ib.qualifyContracts(contract)
                # PACING: Wait 15 seconds BEFORE asking (Critical)
                # IBKR requires a small pause between historical requests to avoid "Pacing Violations" (Error 162).
                    print(f"⏳ Pacing... (Attempt {attempt}/3)")
                    self.ib.sleep(15)
                    
                    # 1. Determine Data Type based on Asset Class
                    data_type = 'TRADES'
                    if contract.secType == 'CASH': # 'CASH' is IBKR code for Forex
                        data_type = 'MIDPOINT'
                    
                    # Request Data
                    bars = self.ib.reqHistoricalData(
                        contract,
                        endDateTime='',
                        durationStr=f'{days} D',
                        barSizeSetting='1 min',
                        whatToShow=data_type,
                        useRTH=True,
                        timeout=120 # give it ample time to respond / arrive
                    )
                
                    if not bars:
                        print(f"⚠️ Attempt {attempt} failed: No data received for {key}")
                        # If failed, wait longer (30s) before retry
                        self.ib.sleep(30)
                        continue
                
                    # Save Data
                    # Convert to dataframe
                    df = util.df(bars)
                
                    if df is not None and not df.empty:
                        df.set_index('date', inplace=True)
                        # Save to Vault
                        self.store.save(key, df)
                        print(f"✅ SUCCESS: Saved {len(df)} rows for {key} to ArcticDB.")
                        success = True 
                        break           # Exit retry loop
                    else:
                        print(f"⚠️ Dataframe empty for {key}")

                except Exception as e:
                    print(f"❌ Error downloading {key}: {e}")
                    self.ib.sleep(30) # Cool down on error
            if not success:
                print(f"💀 GAVE UP on {key} after 3 attempts.")        
        self.ib.disconnect()
        print("🏁 Download Complete.")

if __name__ == "__main__":
    loader = HistoryLoader()
    loader.run(days=30) # Download last 60 days. This overrides days in def_run() at the top, which only acts as a default in case loader.run() does not define days.