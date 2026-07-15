"""
tools/download_history_yahoo_Mateo.py
Yahoo Finance historical data downloader (Yahoo Finance -> CSV).

This script intentionally mirrors the simple ingestor shape used in
download_history_polygon_Mateo.py, but keeps the storage layer lightweight:
one clean CSV per ticker for easy alpha-research reads in notebooks.
"""

import argparse # not in polygon ingestor
from html import parser
import logging
import re # not in polygon ingestor
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import yfinance as yf

# --- IMPORT FIX ---
# Add the project root to sys.path so we can import repo modules later if needed.
# We assume this script is in /Bluegrey/tools/
ROOT_DIR = Path(__file__).parent.parent.resolve()
sys.path.append(str(ROOT_DIR))

# --- SETUP LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("ingestion_yahoo.log"),  # writes error/info to the log file
        logging.StreamHandler(sys.stdout),           # prints error/info in the terminal
    ],
)
logger = logging.getLogger("YahooDataFactory")


class YahooFinanceIngestor:
    def __init__(
        self,
        market="equity",
        interval="1d",
        initial_date="1980-01-01",
        final_date=str(date.today()),
        output_dir=None,
        auto_adjust=False,
        actions=True,
        repair=False,
        show_progress=False,
        overwrite=False,
    ):
        self.market = market
        self.interval = interval
        self.initial_date = initial_date
        self.final_date = final_date
        self.auto_adjust = auto_adjust
        self.actions = actions
        self.repair = repair
        self.show_progress = show_progress
        self.overwrite = overwrite
        SUPPORTED_INTERVALS = {
            "1m",
            "2m",
            "5m",
            "15m",
            "30m",
            "60m",
            "90m",
            "1h",
            "1d",
            "5d",
            "1wk",
            "1mo",
            "3mo",
        }
        if interval not in SUPPORTED_INTERVALS:
            raise ValueError(f"Unsupported interval: {interval}. Supported: {list(SUPPORTED_INTERVALS)}")

        SUPPORTED_MARKETS = {
            "fx",
            "crypto",
            "equity",
            "options",
            "indices",
        }
        if market not in SUPPORTED_MARKETS:
            raise ValueError(f"Unsupported market: {market}. Supported: {list(SUPPORTED_MARKETS)}")

        self.output_dir = Path(output_dir) if output_dir else ROOT_DIR / "src" / "data" / "yahoo" / f"{self.market}" / f"{self.interval}"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        yf.set_tz_cache_location(str(self.output_dir / ".yfinance_cache"))



    def _safe_filename(self, ticker):
        clean = re.sub(r"[^A-Za-z0-9._-]+", "_", ticker).strip("_")
        return f"{clean}.csv"

    def _csv_path(self, ticker):
        return self.output_dir / self._safe_filename(ticker)

    def _to_yahoo_ticker(self, ticker):
        """
        Converts common internal/Polygon ticker formats into Yahoo symbols.

        Examples:
            C:EURUSD -> EURUSD=X
            BTCUSD   -> BTC-USD for market="crypto"
            SPY      -> SPY
        """
        if ticker.startswith("C:") and len(ticker) == 8:
            return f"{ticker[2:]}=X"

        if self.market == "fx" and len(ticker) == 6 and "=" not in ticker:
            return f"{ticker}=X"

        if self.market == "crypto" and len(ticker) == 6 and "-" not in ticker:
            return f"{ticker[:3]}-{ticker[3:]}"

        return ticker

    def _read_tickers_from_file(self, path):
        """
        Reads a simple ticker universe from .txt, .csv, or this repo's YAML files.
        YAML parsing is intentionally lightweight so the script has only one
        non-core dependency: yfinance.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Ticker file not found: {path}")

        tickers = []
        for raw_line in path.read_text().splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            if line.startswith("-"):
                line = line[1:].strip()

            line = line.strip("\"'")
            if ":" in line and not line.startswith("C:"):
                key, value = line.split(":", 1)
                if key.strip() not in {"ticker", "symbol"}:
                    continue
                line = value.strip()

            for ticker in line.split(","):
                ticker = ticker.strip().strip("\"'")
                if ticker:
                    tickers.append(ticker)

        return tickers

    def fetch_default_tickers(self):
        """
        Small starter universes. For serious research, pass explicit symbols or
        --ticker-file so the universe is versioned and reproducible.
        """
        DEFAULT_UNIVERSES = {
            "equity": ["SPY", "ECH", "EZW", "EWW", "QQQ", "IWM", "DIA", "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META"],
            "indices": ["^GSPC", "^IXIC", "^DJI", "^RUT", "^VIX"],
            "crypto": ["BTC-USD", "ETH-USD", "SOL-USD"],
            "fx": ["EURUSD=X", "USDJPY=X", "GBPUSD=X", "AUDUSD=X", "USDCAD=X", "USDCHF=X", "NZDUSD=X"],
        }
        if self.market == "options":
            raise ValueError("Options contracts have expiration- and strike-specific tickers. Pass Yahoo option symbols explicitly with --tickers or --ticker-file.")

        if self.market not in DEFAULT_UNIVERSES:
            raise ValueError(f"No default universe for market={self.market}. Pass tickers explicitly.")

        tickers = DEFAULT_UNIVERSES[self.market]
        logger.info(f"Using default {self.market} universe: {len(tickers)} tickers.")
        return tickers

    def _format_history(self, df, source_ticker, yahoo_ticker):
        if df.empty:
            return df

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.reset_index()
        first_col = df.columns[0]
        df.rename(columns={first_col: "Timestamp"}, inplace=True)

        cols_map = {
            "Adj Close": "Adj_Close",
            "Stock Splits": "Stock_Splits",
            "Capital Gains": "Capital_Gains",
            "Repaired?": "Repaired",
        }
        df.rename(columns=cols_map, inplace=True)

        expected_cols = ["Timestamp", "Open", "High", "Low", "Close", "Adj_Close", "Volume"]
        if self.actions:
            expected_cols += ["Dividends", "Stock_Splits", "Capital_Gains"]
        if self.repair:
            expected_cols += ["Repaired"]

        if self.auto_adjust and "Adj_Close" not in df.columns and "Close" in df.columns:
            df["Adj_Close"] = df["Close"]

        for col in expected_cols:
            if col not in df.columns:
                if col in {"Dividends", "Stock_Splits", "Capital_Gains"}:
                    df[col] = 0.0
                elif col == "Repaired":
                    df[col] = False
                else:
                    df[col] = pd.NA

        if self.actions:
            df[["Dividends", "Stock_Splits", "Capital_Gains"]] = df[
                ["Dividends", "Stock_Splits", "Capital_Gains"]
            ].fillna(0.0)
        if self.repair:
            df["Repaired"] = df["Repaired"].fillna(False).astype(bool)

        df["Timestamp"] = pd.to_datetime(df["Timestamp"], utc=True).dt.tz_localize(None)
        df["Source_Ticker"] = source_ticker  # the ticker passed to the script
        df["Yahoo_Ticker"] = yahoo_ticker    # what the ticker was converted to and then passed to yfinance

        df = df[expected_cols + ["Source_Ticker", "Yahoo_Ticker"]]
        df = df.dropna(subset=["Timestamp"])
        df = df.drop_duplicates(subset=["Timestamp"], keep="last")
        df = df.sort_values("Timestamp")
        return df

    def download_ticker(self, ticker):
        """
        Downloads one ticker from Yahoo and writes/updates a CSV.
        """
        yahoo_ticker = self._to_yahoo_ticker(ticker)
        csv_path = self._csv_path(yahoo_ticker)

        try:
            df = yf.download(
                yahoo_ticker,
                start=self.initial_date,
                end=self.final_date,
                interval=self.interval,
                actions=self.actions,
                auto_adjust=self.auto_adjust,
                repair=self.repair,
                progress=self.show_progress,
                threads=True,
                multi_level_index=False,
            )

            df = self._format_history(df, source_ticker=ticker, yahoo_ticker=yahoo_ticker)
            if df.empty:
                print(f"⚠️ WARNING No data found for {ticker} ({yahoo_ticker}).")
                return

            if csv_path.exists():
                old_df = pd.read_csv(csv_path, parse_dates=["Timestamp"])
                output_cols = list(df.columns)
                for col in output_cols:
                    if col not in old_df.columns:
                            old_df[col] = pd.NA
                old_df = old_df[output_cols]
                df = pd.concat([old_df, df], ignore_index=True)
                if self.overwrite:
                    df = df.drop_duplicates(subset=["Timestamp"], keep="last")  # overwrite old data with new data
                elif self.overwrite is False:
                    df = df.drop_duplicates(subset=["Timestamp"], keep="first")  # keep first to preserve the original data if there are duplicates
                df = df.sort_values("Timestamp")
                df = df[output_cols]
                action = "Updated"
            else:
                action = "Created"

            df.to_csv(csv_path, index=False)
            if action == "Updated":
                print(f"🔄 Updated {yahoo_ticker}: added {len(df) - len(old_df)} new bars to existing {len(old_df)} bars (no old data overwritten) -> {csv_path}")
            elif action == "Created":
                print(f"✅ Created {yahoo_ticker}: {len(df)} new bars -> {csv_path} (no data existed previously)")

        except Exception as e:
            print(f"❌ FAILED {ticker} ({yahoo_ticker}): {e}")

    def run_bulk(self, tickers=None, ticker_file=None):
        """
        Main execution flow for bulk Yahoo CSV download.
        """
        logger.info(f"\n🔥 Ignition: Bluegrey YahooFinance Data Factory. Target: {ingestor.output_dir}")

        if ticker_file:
            all_tickers = self._read_tickers_from_file(ticker_file)
            if tickers is None:
                logger.info(f"Loaded {len(all_tickers)} tickers from {ticker_file}.")
            else:
                logger.info(f"Loaded {len(all_tickers)} tickers from {ticker_file}. \"tickers\" argument will be ignored.")
        elif tickers:
            all_tickers = tickers
            logger.info(f"Using explicit ticker list: {len(all_tickers)} tickers.")
        else:
            all_tickers = self.fetch_default_tickers()

        if not all_tickers:
            logger.error("No tickers to process. Exiting.")
            return

        logger.info(
            f"Starting Yahoo batch job for {len(all_tickers)} {self.market} tickers "
            f"[{self.interval}, {self.initial_date} -> {self.final_date}]."
        )

        for i, ticker in enumerate(all_tickers):
            print(f"[{i + 1}/{len(all_tickers)}] ", end="")
            self.download_ticker(ticker)

        logger.info("👍 Job Complete.")



def parse_args(command_line_args=None):
    parser = argparse.ArgumentParser(description="Download Yahoo Finance history into research-ready CSV files.")
    parser.add_argument("--market", default="equity", choices=["fx", "crypto", "equity", "options", "indices"], help="Market type for Yahoo symbols, e.g. fx, crypto, equity, options, indices. Options require --tickers or --ticker-file.")
    parser.add_argument("--interval", default="1d", help="Yahoo interval, e.g. 1d, 1h, 15m.")
    parser.add_argument("--initial-date", default="1980-01-01")
    parser.add_argument("--final-date", default=str(date.today()))
    parser.add_argument("--output-dir", default=None, help=f"Defaults to {ROOT_DIR}/src/data/yahoo/<market>/<interval>/")
    parser.add_argument("--ticker-file", default=None, help="Optional .txt/.csv/.yaml universe file.")
    parser.add_argument("--tickers", nargs="*", default=None, help="Optional explicit ticker list.")
    parser.add_argument("--auto-adjust", action="store_true", help="Use Yahoo adjusted OHLC prices.")
    parser.add_argument("--no-actions", dest="actions", action="store_false", help="Exclude Yahoo corporate actions.")
    parser.set_defaults(actions=True)
    parser.add_argument("--repair", action="store_true", help="Repair data issues.")
    parser.add_argument("--show-progress", action="store_true", help="Show download progress.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing data with fresh downloaded data.")
    return parser.parse_args(command_line_args)


if __name__ == "__main__":
    # =========================================================================
    # HOW TO RUN THIS INGESTOR
    # =========================================================================
    #
    # 1) TERMINAL / SHELL
    # -------------------------------------------------------------------------
    # Run these commands from the project root: /Users/mateo/Bluegrey
    #
    # Simple example: default equity universe, daily bars, default dates.
    #     python tools/download_history_yahoo_Mateo.py
    #
    # Simple example: explicit equity tickers.
    #     python tools/download_history_yahoo_Mateo.py --market equity --tickers SPY ECH AAPL
    #
    # Bigger example: many arguments.
    #     python tools/download_history_yahoo_Mateo.py \
    #         --market equity \
    #         --interval 1d \
    #         --initial-date 2005-01-01 \
    #         --final-date 2024-12-31 \
    #         --tickers SPY ECH QQQ EWW AAPL MSFT NVDA \
    #         --repair \
    #         --overwrite
    #
    # Actions are included by default. Add --no-actions only if you want to
    # exclude dividends, stock splits, and capital gains.
    #
    #
    # 2) VS CODE "RUN PYTHON FILE"
    # -------------------------------------------------------------------------
    # Clicking Run in VS Code does not give you a terminal command to edit, so
    # use VS_CODE_ARGS below when you want to simulate terminal arguments.
    #
    # Leave as None to use the parser defaults:
    VS_CODE_ARGS = None
    #
    # Simple example: uncomment this line, then click Run.
    # VS_CODE_ARGS = ["--market", "equity", "--tickers", "SPY", "QQQ"]
    #
    # Simple example: FX tickers from an existing universe file.
    # VS_CODE_ARGS = ["--market", "fx", "--ticker-file", "universes/fx_liquid.yaml"]
    #
    # Bigger example: many arguments.
    # VS_CODE_ARGS = [
    #     "--market", "crypto",
    #     "--interval", "1d",
    #     "--initial-date", "2018-01-01",
    #     "--final-date", "2024-12-31",
    #     "--tickers", "BTC-USD", "ETH-USD", "SOL-USD",
    #     "--repair",
    #     "--overwrite",
    # ]
    #
    #
    # 3) NOTEBOOK
    # -------------------------------------------------------------------------
    # In a notebook, import the class and call it directly. Do not run this file
    # through argparse unless you specifically want command-line behavior.
    #
    # Simple example:
    #     from tools.download_history_yahoo_Mateo import YahooFinanceIngestor
    #     ingestor = YahooFinanceIngestor(market="equity", interval="1d")
    #     ingestor.run_bulk(tickers=["SPY", "QQQ"])
    #
    # Simple example: use default tickers for a market.
    #     ingestor = YahooFinanceIngestor(market="crypto", interval="1d")
    #     ingestor.run_bulk()
    #
    # Bigger example: many arguments.
    #     ingestor = YahooFinanceIngestor(
    #         market="equity",
    #         interval="1d",
    #         initial_date="2005-01-01",
    #         final_date="2024-12-31",
    #         auto_adjust=False,
    #         actions=True,
    #         repair=True,
    #         overwrite=True,
    #     )
    #     ingestor.run_bulk(tickers=["SPY", "QQQ", "ECH", "EWW", "AAPL", "MSFT", "NVDA"])
    #
    # =========================================================================

    args = parse_args(VS_CODE_ARGS)

    ingestor = YahooFinanceIngestor(
        market=args.market,
        interval=args.interval,
        initial_date=args.initial_date,
        final_date=args.final_date,
        output_dir=args.output_dir,
        auto_adjust=args.auto_adjust,
        actions=args.actions,
        repair=args.repair,
        show_progress=args.show_progress,
        overwrite=args.overwrite,
    )

    ingestor.run_bulk(tickers=args.tickers, ticker_file=args.ticker_file)
