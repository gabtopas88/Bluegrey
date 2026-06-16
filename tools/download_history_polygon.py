"""
tools/download_history_polygon.py
Institutional-Grade Multi-Asset Data Ingestion Engine (Polygon.io -> ArcticDB)
Wired with UniverseManager for Point-In-Time Liquidity Filtering
"""
import sys
import os
import argparse
import time
from pathlib import Path
from datetime import date, timedelta, time as dt_time
import logging
import pandas as pd
from polygon import RESTClient
from arcticdb import Arctic

# --- IMPORT FIX ---
ROOT_DIR = Path(__file__).parent.parent.resolve()
sys.path.append(str(ROOT_DIR))

# Import the Unified Config & The Universe Manager
from src.config import config
from src.data.universe import UniverseManager

# --- SETUP LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("ingestion.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("DataFactory")

# Hardcoded NYSE regular-session calendar for this research period.
# Sources checked: NYSE Holidays & Trading Hours page for 2026 and standard
# NYSE/Nasdaq holiday schedules for 2022-2025. Times are Eastern Time.
NYSE_FULL_CLOSE_DATES = {
    "2022-01-17",  # Martin Luther King Jr. Day
    "2022-02-21",  # Washington's Birthday
    "2022-04-15",  # Good Friday
    "2022-05-30",  # Memorial Day
    "2022-06-20",  # Juneteenth observed
    "2022-07-04",  # Independence Day
    "2022-09-05",  # Labor Day
    "2022-11-24",  # Thanksgiving Day
    "2022-12-26",  # Christmas Day observed
    "2023-01-02",  # New Year's Day observed
    "2023-01-16",  # Martin Luther King Jr. Day
    "2023-02-20",  # Washington's Birthday
    "2023-04-07",  # Good Friday
    "2023-05-29",  # Memorial Day
    "2023-06-19",  # Juneteenth National Independence Day
    "2023-07-04",  # Independence Day
    "2023-09-04",  # Labor Day
    "2023-11-23",  # Thanksgiving Day
    "2023-12-25",  # Christmas Day
    "2024-01-01",  # New Year's Day
    "2024-01-15",  # Martin Luther King Jr. Day
    "2024-02-19",  # Washington's Birthday
    "2024-03-29",  # Good Friday
    "2024-05-27",  # Memorial Day
    "2024-06-19",  # Juneteenth National Independence Day
    "2024-07-04",  # Independence Day
    "2024-09-02",  # Labor Day
    "2024-11-28",  # Thanksgiving Day
    "2024-12-25",  # Christmas Day
    "2025-01-01",  # New Year's Day
    "2025-01-09",  # National Day of Mourning for President Jimmy Carter
    "2025-01-20",  # Martin Luther King Jr. Day
    "2025-02-17",  # Washington's Birthday
    "2025-04-18",  # Good Friday
    "2025-05-26",  # Memorial Day
    "2025-06-19",  # Juneteenth National Independence Day
    "2025-07-04",  # Independence Day
    "2025-09-01",  # Labor Day
    "2025-11-27",  # Thanksgiving Day
    "2025-12-25",  # Christmas Day
    "2026-01-01",  # New Year's Day
    "2026-01-19",  # Martin Luther King Jr. Day
    "2026-02-16",  # Washington's Birthday
    "2026-04-03",  # Good Friday
    "2026-05-25",  # Memorial Day
    "2026-06-19",  # Juneteenth National Independence Day
    "2026-07-03",  # Independence Day observed
    "2026-09-07",  # Labor Day
    "2026-11-26",  # Thanksgiving Day
    "2026-12-25",  # Christmas Day
}

NYSE_EARLY_CLOSE_TIMES = {
    "2022-11-25": "13:00",  # Day after Thanksgiving
    "2023-07-03": "13:00",  # Day before Independence Day
    "2023-11-24": "13:00",  # Day after Thanksgiving
    "2024-07-03": "13:00",  # Day before Independence Day
    "2024-11-29": "13:00",  # Day after Thanksgiving
    "2024-12-24": "13:00",  # Christmas Eve
    "2025-07-03": "13:00",  # Day before Independence Day
    "2025-11-28": "13:00",  # Day after Thanksgiving
    "2025-12-24": "13:00",  # Christmas Eve
    "2026-07-02": "13:00",  # Day before Independence Day observed
    "2026-11-27": "13:00",  # Day after Thanksgiving
    "2026-12-24": "13:00",  # Christmas Eve
}

class PolygonIngestor:
    def __init__(self):
        self.client = RESTClient(config.POLYGON_API_KEY)
        self.store = Arctic(config.ARCTIC_PATH)
        self.universe_manager = UniverseManager()  # <--- INTEGRATION POINT
        
    def _get_library(self, lib_name: str):
        """Safely retrieves or creates an ArcticDB library."""
        if lib_name not in self.store.list_libraries():
            self.store.create_library(lib_name)
            logger.info(f"Created new ArcticDB library: {lib_name}")
        return self.store[lib_name]

    def _get_smart_start_date(self, library, ticker: str, default_start: str) -> str:
        """
        Queries ArcticDB to find the last known timestamp for a ticker.
        Prevents downloading years of history we already have.
        """
        if not library.has_symbol(ticker):
            return default_start
            
        try:
            df_existing = library.read(ticker).data
            if df_existing.empty:
                return default_start
                
            last_timestamp = df_existing.index.max()
            
            # Overlap by 1 day to ensure no gaps, deduplication handles the overlap
            smart_start = (last_timestamp - timedelta(days=1)).strftime("%Y-%m-%d")
            return smart_start
            
        except Exception as e:
            logger.warning(f"Failed to resolve smart start date for {ticker}, defaulting to {default_start}. Error: {e}")
            return default_start

    def _fetch_aggs_with_retries(
        self,
        ticker: str,
        timespan: str,
        multiplier: int,
        start_date: str,
        end_date: str,
        max_retries: int,
        retry_sleep_seconds: float,
    ) -> list:
        for attempt in range(max_retries + 1):
            try:
                aggs = []
                for agg in self.client.list_aggs(
                    ticker=ticker,
                    multiplier=multiplier,
                    timespan=timespan,
                    from_=start_date,
                    to=end_date,
                    limit=50000
                ):
                    aggs.append(agg)
                return aggs
            except Exception as e:
                message = str(e).lower()
                rate_limited = "429" in message or "too many" in message or "max retries" in message
                if rate_limited and attempt < max_retries:
                    wait = retry_sleep_seconds * (attempt + 1)
                    print(f"⏳ RATE LIMITED {ticker}: retry {attempt + 1}/{max_retries} after {wait:.0f}s.")
                    time.sleep(wait)
                    continue
                raise

    @staticmethod
    def _normalize_aggs(aggs: list) -> pd.DataFrame:
        df = pd.DataFrame(aggs)
        if df.empty:
            return df

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)

        cols_map = {'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume', 'vw': 'vwap', 'n': 'transactions'}
        df.rename(columns=cols_map, inplace=True)

        expected_cols = ['open', 'high', 'low', 'close', 'volume', 'vwap']
        for col in expected_cols:
            if col not in df.columns:
                df[col] = 0.0

        df = df[expected_cols]
        df = df.astype({col: "float64" for col in expected_cols})
        df = df[~df.index.duplicated(keep='last')]
        return df.sort_index()

    @staticmethod
    def _write_or_update(library, ticker: str, df: pd.DataFrame) -> str:
        if library.has_symbol(ticker):
            library.update(ticker, df)
            return f"Appended {len(df)} rows"
        library.write(ticker, df)
        return f"Created {len(df)} rows"

    def download_ticker_window(
        self,
        ticker: str,
        lib_name: str,
        timespan: str = "minute",
        multiplier: int = 1,
        start_date: str = "2020-01-01",
        end_date: str = None,
        max_retries: int = 3,
        retry_sleep_seconds: float = 65.0,
        use_smart_start: bool = False,
    ) -> bool:
        """
        Downloads exactly the requested window unless use_smart_start=True.
        Use this for gap repair or force-window backfills.
        """
        if end_date is None:
            end_date = str(date.today())

        lib = self._get_library(lib_name)
        actual_start_date = start_date
        if use_smart_start:
            actual_start_date = self._get_smart_start_date(lib, ticker, start_date)
            if actual_start_date >= end_date:
                print(f"⏩ {ticker} is already up to date ({end_date}). Skipping.")
                return True

        try:
            aggs = self._fetch_aggs_with_retries(
                ticker=ticker,
                timespan=timespan,
                multiplier=multiplier,
                start_date=actual_start_date,
                end_date=end_date,
                max_retries=max_retries,
                retry_sleep_seconds=retry_sleep_seconds,
            )

            if not aggs:
                print(f"⚠️ No new data found for {ticker} between {actual_start_date} and {end_date}.")
                return False

            df = self._normalize_aggs(aggs)
            action = self._write_or_update(lib, ticker, df)
            print(f"✅ {ticker}: {action} ({actual_start_date} -> {end_date}).")
            return True

        except Exception as e:
            print(f"❌ FAILED {ticker}: {e}")
            return False

    def download_ticker(
        self,
        ticker: str,
        lib_name: str,
        timespan: str = "minute",
        multiplier: int = 1,
        start_date: str = "2020-01-01",
        end_date: str = None,
        max_retries: int = 3,
        retry_sleep_seconds: float = 65.0,
    ):
        """
        Core ingestion logic. Fetches, formats, and smartly updates ArcticDB.
        """
        return self.download_ticker_window(
            ticker=ticker,
            lib_name=lib_name,
            timespan=timespan,
            multiplier=multiplier,
            start_date=start_date,
            end_date=end_date,
            max_retries=max_retries,
            retry_sleep_seconds=retry_sleep_seconds,
            use_smart_start=True,
        )

    @staticmethod
    def _expected_regular_session_index(
        start_date: str,
        end_date: str,
        timezone: str = "America/New_York",
        session_start: str = "09:30",
        session_end: str = "16:00",
    ) -> pd.DatetimeIndex:
        start = pd.Timestamp(start_date).date()
        end = pd.Timestamp(end_date).date()
        days = pd.bdate_range(start, end)
        ranges = []
        default_session_end = dt_time.fromisoformat(session_end)

        for day in days:
            day_str = day.strftime("%Y-%m-%d")
            if day_str in NYSE_FULL_CLOSE_DATES:
                continue

            session_end_time = default_session_end
            if day_str in NYSE_EARLY_CLOSE_TIMES:
                early_close_time = dt_time.fromisoformat(NYSE_EARLY_CLOSE_TIMES[day_str])
                session_end_time = min(default_session_end, early_close_time)

            start_ts = pd.Timestamp.combine(day.date(), dt_time.fromisoformat(session_start)).tz_localize(timezone)
            end_ts = pd.Timestamp.combine(day.date(), session_end_time).tz_localize(timezone)
            if end_ts <= start_ts:
                continue

            ranges.append(pd.date_range(start_ts, end_ts, freq="1min", inclusive="left"))

        if not ranges:
            return pd.DatetimeIndex([], name="timestamp")

        expected = ranges[0]
        for idx in ranges[1:]:
            expected = expected.append(idx)

        # Polygon data is stored as naive UTC timestamps in this project.
        expected = expected.tz_convert("UTC").tz_localize(None)
        expected.name = "timestamp"
        return expected

    @staticmethod
    def _date_windows_from_missing_minutes(
        missing_minutes: pd.DatetimeIndex,
        max_window_days: int = 5,
    ) -> list:
        if missing_minutes.empty:
            return []

        missing_dates = pd.Series(missing_minutes.date).drop_duplicates()
        windows = []
        window_start = pd.Timestamp(missing_dates.iloc[0])
        window_end = window_start

        for raw_day in missing_dates.iloc[1:]:
            day = pd.Timestamp(raw_day)
            contiguous = day <= window_end + pd.Timedelta(days=1)
            within_size = (day - window_start).days < max_window_days

            if contiguous and within_size:
                window_end = day
                continue

            windows.append((window_start.strftime("%Y-%m-%d"), window_end.strftime("%Y-%m-%d")))
            window_start = day
            window_end = day

        windows.append((window_start.strftime("%Y-%m-%d"), window_end.strftime("%Y-%m-%d")))
        return windows

    def audit_ticker_coverage(
        self,
        ticker: str,
        lib_name: str,
        start_date: str,
        end_date: str,
        timezone: str = "America/New_York",
        session_start: str = "09:30",
        session_end: str = "16:00",
        max_window_days: int = 5,
    ) -> dict:
        lib = self._get_library(lib_name)
        expected = self._expected_regular_session_index(
            start_date=start_date,
            end_date=end_date,
            timezone=timezone,
            session_start=session_start,
            session_end=session_end,
        )

        if not lib.has_symbol(ticker):
            existing = pd.DatetimeIndex([], name="timestamp")
        else:
            df = lib.read(ticker).data
            existing = pd.DatetimeIndex(df.index).sort_values().drop_duplicates()
            existing = existing[(existing >= expected.min()) & (existing <= expected.max())] if len(expected) else existing

        existing_expected = expected.intersection(existing)
        missing = expected.difference(existing_expected)
        windows = self._date_windows_from_missing_minutes(missing, max_window_days=max_window_days)
        coverage = 1.0 if len(expected) == 0 else 1.0 - (len(missing) / len(expected))

        return {
            "ticker": ticker,
            "start_date": start_date,
            "end_date": end_date,
            "expected_bars": len(expected),
            "existing_bars": len(existing_expected),
            "missing_bars": len(missing),
            "coverage": coverage,
            "missing_windows": windows,
            "first_existing": existing.min() if len(existing) else pd.NaT,
            "last_existing": existing.max() if len(existing) else pd.NaT,
        }

    def repair_ticker_gaps(
        self,
        ticker: str,
        lib_name: str,
        timespan: str,
        multiplier: int,
        start_date: str,
        end_date: str,
        timezone: str = "America/New_York",
        session_start: str = "09:30",
        session_end: str = "16:00",
        max_window_days: int = 5,
        max_windows: int = None,
        sleep_seconds: float = 15.0,
        max_retries: int = 3,
        retry_sleep_seconds: float = 65.0,
    ) -> dict:
        before = self.audit_ticker_coverage(
            ticker=ticker,
            lib_name=lib_name,
            start_date=start_date,
            end_date=end_date,
            timezone=timezone,
            session_start=session_start,
            session_end=session_end,
            max_window_days=max_window_days,
        )

        windows = before["missing_windows"]
        if max_windows:
            windows = windows[:max_windows]

        print(
            f"🔎 {ticker}: {before['missing_bars']} missing of {before['expected_bars']} "
            f"expected bars ({before['coverage']:.2%} coverage)."
        )
        print(f"🧩 Repair windows selected: {len(windows)}")

        repaired_windows = 0
        for i, (window_start, window_end) in enumerate(windows):
            print(f"[{i + 1}/{len(windows)}] Repairing {ticker}: {window_start} -> {window_end}")
            ok = self.download_ticker_window(
                ticker=ticker,
                lib_name=lib_name,
                timespan=timespan,
                multiplier=multiplier,
                start_date=window_start,
                end_date=window_end,
                max_retries=max_retries,
                retry_sleep_seconds=retry_sleep_seconds,
                use_smart_start=False,
            )
            if ok:
                repaired_windows += 1
            if sleep_seconds and i < len(windows) - 1:
                time.sleep(sleep_seconds)

        after = self.audit_ticker_coverage(
            ticker=ticker,
            lib_name=lib_name,
            start_date=start_date,
            end_date=end_date,
            timezone=timezone,
            session_start=session_start,
            session_end=session_end,
            max_window_days=max_window_days,
        )
        return {
            "ticker": ticker,
            "windows_attempted": len(windows),
            "windows_repaired": repaired_windows,
            "missing_before": before["missing_bars"],
            "missing_after": after["missing_bars"],
            "coverage_before": before["coverage"],
            "coverage_after": after["coverage"],
            "first_existing": after["first_existing"],
            "last_existing": after["last_existing"],
        }

    def run_batch_job(
        self,
        universe_name: str,
        timespan: str,
        multiplier: int,
        start_date: str,
        end_date: str,
        lib_name: str,
        specific_tickers: list = None,
        sleep_seconds: float = 15.0,
        max_retries: int = 3,
        retry_sleep_seconds: float = 65.0,
        mode: str = "update_forward",
    ):
        """
        Orchestrator for processing lists of assets.
        Now queries the UniverseManager for Point-In-Time validated tickers.
        """
        logger.info(f"--- STARTING BATCH JOB: Universe '{universe_name}' | {multiplier}-{timespan} ---")
        
        # 1. DELEGATE TICKER SELECTION TO THE UNIVERSE MANAGER
        if specific_tickers:
            logger.info("Override active: Using manually provided specific_tickers.")
            tickers = specific_tickers
        else:
            # Pass the end_date to the UniverseManager so it fetches tickers that were 
            # active and liquid specifically on that date (Point-In-Time)
            tickers = self.universe_manager.get_universe(universe_name, as_of_date=end_date)
            
        if not tickers:
            logger.error("No tickers to process. Exiting.")
            return

        logger.info(f"Data Factory received {len(tickers)} validated assets. Commencing download...")

        # 2. EXECUTE THE DOWNLOAD LOOP
        for i, ticker in enumerate(tickers):
            print(f"[{i+1}/{len(tickers)}] ", end="")
            if mode == "force_window":
                self.download_ticker_window(
                    ticker=ticker,
                    lib_name=lib_name,
                    timespan=timespan,
                    multiplier=multiplier,
                    start_date=start_date,
                    end_date=end_date,
                    max_retries=max_retries,
                    retry_sleep_seconds=retry_sleep_seconds,
                    use_smart_start=False,
                )
            else:
                self.download_ticker(
                    ticker=ticker,
                    lib_name=lib_name,
                    timespan=timespan,
                    multiplier=multiplier,
                    start_date=start_date,
                    end_date=end_date,
                    max_retries=max_retries,
                    retry_sleep_seconds=retry_sleep_seconds,
                )
            if sleep_seconds and i < len(tickers) - 1:
                time.sleep(sleep_seconds)

    def run_audit_job(
        self,
        universe_name: str,
        start_date: str,
        end_date: str,
        lib_name: str,
        specific_tickers: list = None,
        timezone: str = "America/New_York",
        session_start: str = "09:30",
        session_end: str = "16:00",
        max_window_days: int = 5,
    ) -> pd.DataFrame:
        if specific_tickers:
            tickers = specific_tickers
        else:
            tickers = self.universe_manager.get_universe(universe_name, as_of_date=end_date)

        rows = []
        for ticker in tickers:
            report = self.audit_ticker_coverage(
                ticker=ticker,
                lib_name=lib_name,
                start_date=start_date,
                end_date=end_date,
                timezone=timezone,
                session_start=session_start,
                session_end=session_end,
                max_window_days=max_window_days,
            )
            row = report.copy()
            row["missing_windows"] = len(report["missing_windows"])
            rows.append(row)

        out = pd.DataFrame(rows)
        if not out.empty:
            print(out[[
                "ticker",
                "expected_bars",
                "existing_bars",
                "missing_bars",
                "coverage",
                "missing_windows",
                "first_existing",
                "last_existing",
            ]])
        return out

    def run_gap_repair_job(
        self,
        universe_name: str,
        timespan: str,
        multiplier: int,
        start_date: str,
        end_date: str,
        lib_name: str,
        specific_tickers: list = None,
        timezone: str = "America/New_York",
        session_start: str = "09:30",
        session_end: str = "16:00",
        max_window_days: int = 5,
        max_windows_per_ticker: int = None,
        sleep_seconds: float = 15.0,
        max_retries: int = 3,
        retry_sleep_seconds: float = 65.0,
    ) -> pd.DataFrame:
        if specific_tickers:
            tickers = specific_tickers
        else:
            tickers = self.universe_manager.get_universe(universe_name, as_of_date=end_date)

        rows = []
        for i, ticker in enumerate(tickers):
            print(f"[{i + 1}/{len(tickers)}] Gap repair for {ticker}")
            rows.append(self.repair_ticker_gaps(
                ticker=ticker,
                lib_name=lib_name,
                timespan=timespan,
                multiplier=multiplier,
                start_date=start_date,
                end_date=end_date,
                timezone=timezone,
                session_start=session_start,
                session_end=session_end,
                max_window_days=max_window_days,
                max_windows=max_windows_per_ticker,
                sleep_seconds=sleep_seconds,
                max_retries=max_retries,
                retry_sleep_seconds=retry_sleep_seconds,
            ))
            if sleep_seconds and i < len(tickers) - 1:
                time.sleep(sleep_seconds)

        out = pd.DataFrame(rows)
        if not out.empty:
            print(out)
        return out

def parse_args():
    parser = argparse.ArgumentParser(description="Bluegrey Institutional Data Factory")
    parser.add_argument("--mode", type=str, default="update_forward",
                        choices=["update_forward", "force_window", "audit_gaps", "repair_gaps"],
                        help="Download mode: update from last timestamp, force requested window, audit gaps, or repair gaps.")
    
    # We replaced --market with --universe because the YAML config defines the market
    parser.add_argument("--universe", type=str, required=True, 
                        help="Name of the Universe YAML file (e.g., 'equities_liquid', 'fx_g10').")
    parser.add_argument("--lib-name", type=str, required=True, 
                        help="Target ArcticDB Library name (e.g., equities_daily, crypto_min).")
    parser.add_argument("--timespan", type=str, default="minute", choices=['minute', 'hour', 'day', 'week', 'month'], 
                        help="Bar size unit.")
    parser.add_argument("--multiplier", type=int, default=1, 
                        help="Bar size multiplier (e.g., 5 for 5-minute bars).")
    parser.add_argument("--start-date", type=str, default="2020-01-01", 
                        help="Fallback start date (YYYY-MM-DD) if data does not exist in DB.")
    parser.add_argument("--end-date", type=str, default=str(date.today()), 
                        help="End date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--tickers", type=str, nargs="+", default=None, 
                        help="Manual override: Specific tickers to download (Space separated).")
    parser.add_argument("--sleep-seconds", type=float, default=15.0,
                        help="Pause between tickers. Use a larger value for rate-limited plans.")
    parser.add_argument("--max-retries", type=int, default=3,
                        help="Retries per ticker when Polygon returns a transient/rate-limit error.")
    parser.add_argument("--retry-sleep-seconds", type=float, default=65.0,
                        help="Base wait before retrying a rate-limited ticker.")
    parser.add_argument("--timezone", type=str, default="America/New_York",
                        help="Timezone used to build expected regular-session minute bars.")
    parser.add_argument("--session-start", type=str, default="09:30",
                        help="Regular session start used by gap audit.")
    parser.add_argument("--session-end", type=str, default="16:00",
                        help="Regular session end used by gap audit.")
    parser.add_argument("--max-window-days", type=int, default=5,
                        help="Maximum number of calendar days per repair window.")
    parser.add_argument("--max-windows-per-ticker", type=int, default=None,
                        help="Optional cap on repair windows per ticker for incremental runs.")
    
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    ingestor = PolygonIngestor()
    logger.info(f"Ignition: Bluegrey Data Factory. Target: {config.ARCTIC_PATH}")

    if args.mode == "audit_gaps":
        ingestor.run_audit_job(
            universe_name=args.universe,
            start_date=args.start_date,
            end_date=args.end_date,
            lib_name=args.lib_name,
            specific_tickers=args.tickers,
            timezone=args.timezone,
            session_start=args.session_start,
            session_end=args.session_end,
            max_window_days=args.max_window_days,
        )
    elif args.mode == "repair_gaps":
        ingestor.run_gap_repair_job(
            universe_name=args.universe,
            timespan=args.timespan,
            multiplier=args.multiplier,
            start_date=args.start_date,
            end_date=args.end_date,
            lib_name=args.lib_name,
            specific_tickers=args.tickers,
            timezone=args.timezone,
            session_start=args.session_start,
            session_end=args.session_end,
            max_window_days=args.max_window_days,
            max_windows_per_ticker=args.max_windows_per_ticker,
            sleep_seconds=args.sleep_seconds,
            max_retries=args.max_retries,
            retry_sleep_seconds=args.retry_sleep_seconds,
        )
    else:
        ingestor.run_batch_job(
            universe_name=args.universe,
            timespan=args.timespan,
            multiplier=args.multiplier,
            start_date=args.start_date,
            end_date=args.end_date,
            lib_name=args.lib_name,
            specific_tickers=args.tickers,
            sleep_seconds=args.sleep_seconds,
            max_retries=args.max_retries,
            retry_sleep_seconds=args.retry_sleep_seconds,
            mode=args.mode,
        )

    logger.info("Batch Job Complete.")
