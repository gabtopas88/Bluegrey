from event_backtester import BacktestEngine

# 🧠 1. The Strategy
# (Once your plumbing is confirmed, you will import your KalmanPairStrategy here)
class TestStrategy:
    def __init__(self, instruments, params):
        self.instruments = instruments
        
    def on_tick(self, market_snapshot):
        # We are just observing the market. No trades yet.
        # This proves the Event Loop can process millions of ticks without crashing.
        return None

if __name__ == "__main__":
    print("🚀 Igniting Backtest Engine...")

    # 🗺️ 2. The Universe
    # CRITICAL: These keys must exactly match how Polygon saved them in ArcticDB
    my_universe = {
        'C:EURUSD': None,  
        'C:AUDUSD': None
    }

    # 🎛️ 3. The Parameters
    my_params = {}

    # 🏗️ 4. Initialize Engine
    engine = BacktestEngine(
        strategy_class=TestStrategy,
        instruments=my_universe,
        params=my_params,
        start_cap=100000,
        data_library="fx.min"
    )

    # ⏳ 5. The Timeframe
    # Let's run a short 7-day test first to ensure it is lightning fast
    start = "2024-01-01"
    end = "2024-01-07"

    # 🔥 6. Execute
    engine.run(start_date=start, end_date=end)