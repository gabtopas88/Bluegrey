import arcticdb as adb
from src.config import ARCTIC_PATH

def audit():
    print(f"🔌 Connecting to Vault: {ARCTIC_PATH}")
    
    try:
        store = adb.Arctic(ARCTIC_PATH)
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    # 1. List All Libraries (The "Categories")
    libs = store.list_libraries()
    
    if not libs:
        print("⚠️ The Database is EMPTY.")
        return

    print(f"📚 Found {len(libs)} Libraries: {libs}")
    print("-" * 50)

    # 2. Inspect Each Library
    for lib_name in libs:
        lib = store[lib_name]
        keys = lib.list_symbols()
        
        print(f"\n📂 Library: '{lib_name}'")
        print(f"   Count: {len(keys)} items")
        
        if keys:
            # Show the first 5 to give you a hint of what they are
            print(f"   Samples: {keys[:5]} ...")
            
            # Heuristic Check: Polygon Script usually puts data in 'fx.min'
            # If this is NOT 'fx.min', it might be legacy data.
            if lib_name != 'fx.min':
                print("   ⚠️  SUSPECT: This library was likely NOT created by your new Polygon script.")
        else:
            print("   (Empty Library)")

if __name__ == "__main__":
    audit()
