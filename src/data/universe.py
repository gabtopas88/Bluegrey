"""
src/universe.py
Institutional Point-In-Time Universe Manager & Liquidity Filter
"""
import os
import yaml
import logging
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
from polygon import RESTClient

from src.config import config

logger = logging.getLogger("UniverseManager")

class UniverseManager:
    def __init__(self):
        self.client = RESTClient(config.POLYGON_API_KEY)
        self.universes_dir = Path(config.ROOT_DIR) / "universes"

    def load_config(self, universe_name: str) -> dict:
        """Loads the YAML configuration for a given universe."""
        filepath = self.universes_dir / f"{universe_name}.yaml"
        if not filepath.exists():
            raise FileNotFoundError(f"Universe configuration {filepath} not found.")
            
        with open(filepath, 'r') as file:
            return yaml.safe_load(file)

    def get_universe(self, universe_name: str, as_of_date: str = None) -> list:
        """
        Master method to resolve a universe. 
        as_of_date allows for Point-In-Time (PIT) ticker discovery.
        """
        if as_of_date is None:
            as_of_date = str(date.today())

        cfg = self.load_config(universe_name)
        logger.info(f"Resolving Universe: {cfg['name']} (As of: {as_of_date})")

        # 1. Static Universes (e.g., G10 FX, predefined ETF baskets)
        if 'static_tickers' in cfg:
            logger.info("Loaded static ticker list.")
            return cfg['static_tickers']

        # 2. Dynamic Universes (e.g., Equities requiring Liquidity Filtering)
        market = cfg.get('market', 'stocks')
        rules = cfg.get('rules', {})

        return self._build_dynamic_universe(market, as_of_date, rules)

    def _build_dynamic_universe(self, market: str, as_of_date: str, rules: dict) -> list:
        """
        Queries Polygon for active tickers ON A SPECIFIC DATE, preventing survivorship bias.
        Then applies cross-sectional liquidity filters.
        """
        logger.info(f"Fetching point-in-time active {market} for {as_of_date}...")
        
        # 1. Get ALL active tickers on the specific date (Survivorship Bias Prevention)
        raw_tickers = []
        try:
            for t in self.client.list_tickers(market=market, date=as_of_date, active=True, limit=1000):
                raw_tickers.append(t.ticker)
        except Exception as e:
            logger.error(f"Failed to fetch PIT tickers: {e}")
            return []

        logger.info(f"Found {len(raw_tickers)} active tickers. Applying liquidity filters...")

        # If no rules are defined, return the raw list
        if not rules:
            return raw_tickers

        # 2. Cross-Sectional Liquidity Filtering
        # Instead of pinging Polygon 10,000 times, we get a single cross-sectional snapshot
        # of the entire market for the given date (or the closest prior trading day).
        return self._apply_liquidity_snapshot(raw_tickers, as_of_date, rules)

    def _apply_liquidity_snapshot(self, valid_tickers: list, as_of_date: str, rules: dict) -> list:
        """
        Uses Polygon's grouped daily aggregates to filter thousands of stocks in < 2 seconds.
        """
        min_dollar_volume = rules.get('min_dollar_volume', 0)
        min_price = rules.get('min_price', 0)
        max_price = rules.get('max_price', float('inf'))

        try:
            # Note: If as_of_date is a weekend/holiday, this might return empty. 
            # In a live fund, you'd implement a calendar offset here to find the last trading day.
            aggs = self.client.get_grouped_daily_aggs(date=as_of_date)
            
            if not aggs:
                logger.warning(f"No market data found for {as_of_date}. Returning unfiltered list.")
                return valid_tickers

            # Convert to DataFrame for vectorized filtering
            df = pd.DataFrame([{
                'ticker': a.ticker,
                'close': a.close,
                'volume': a.volume,
                'dollar_volume': a.close * a.volume
            } for a in aggs])

            # Apply Rules
            filtered_df = df[
                (df['ticker'].isin(valid_tickers)) &
                (df['dollar_volume'] >= min_dollar_volume) &
                (df['close'] >= min_price) &
                (df['close'] <= max_price)
            ]
            
            final_tickers = filtered_df['ticker'].tolist()
            logger.info(f"Liquidity Filter Complete: Reduced from {len(valid_tickers)} to {len(final_tickers)} tradable assets.")
            return final_tickers

        except Exception as e:
            logger.error(f"Liquidity snapshot failed: {e}. Falling back to unfiltered list.")
            return valid_tickers

if __name__ == "__main__":
    # Quick Test
    import sys
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    
    manager = UniverseManager()
    
    # Test Static
    print("\n--- Testing FX G10 ---")
    g10 = manager.get_universe("fx_g10")
    print(g10)
    
    # Test Dynamic (Requires PyYAML installed: pip install pyyaml)
    # Using a recent weekday to ensure the snapshot data exists
    test_date = (date.today() - timedelta(days=3)).strftime("%Y-%m-%d") 
    print(f"\n--- Testing Liquid Equities (As of {test_date}) ---")
    liquid_equities = manager.get_universe("equities_liquid", as_of_date=test_date)
    print(f"Sample of top liquid assets: {liquid_equities[:10]}")
