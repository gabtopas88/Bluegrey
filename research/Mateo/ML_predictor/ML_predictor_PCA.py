# ======================================
# Initialisation -- import libraries
# ======================================

# Core
import numpy as np
import pandas as pd
import time
from IPython.display import display
import re
from numbers import Integral, Real
from pathlib import Path

# Plotting PCA diagnostics.
import matplotlib.pyplot as plt

# Data download
import yfinance as yf

# Technical indicators
# !pip install pandas-ta-classic --quiet   # May need to install this
# !pip install TA-Lib --quiet     # May need to install this
import pandas_ta_classic as ta

# Machine learning
from sklearn.neural_network import MLPClassifier
# from sklearn.preprocessing import MinMaxScaler     # this was used for Pearson correlation, but not PCA
from sklearn.preprocessing import StandardScaler
# On StandardScaler vs MinMaxScaler: I used StandardScaler because PCA is variance-based. 
# PCA finds directions of maximum variance, so features should usually be centered and scaled to comparable variance first. 
# StandardScaler gives each feature mean 0 and standard deviation 1. MinMaxScaler puts features into a range, usually [0, 1],
# but does not equalize variance and can be distorted by extreme min/max values.
from sklearn.pipeline import Pipeline
from sklearn.feature_selection import VarianceThreshold
from sklearn.decomposition import PCA

# Silence convergence warnings from MLPClassifier when max_iter is too low to converge
import warnings
from sklearn.exceptions import ConvergenceWarning

# Inherit BaseStrategy
from strategies.base import BaseStrategy


# ==================================================
# Class definition for the MLP Predictor Strategy
# ==================================================

class MLPredictorStrategy(BaseStrategy):

    def __init__(self, instruments: dict = None, params: dict = None):
        super().__init__(instruments or {}, params or {})    # initialises self.instruments and self.params 
                                                             # in the BaseStrategy parent class

        try:
            self.symbol = self.params["symbol"]
        except KeyError as exc:
            raise ValueError("Missing required strategy parameter: symbol") from exc
        
        self.signal_threshold = self.params.get("signal_threshold", 0.002)  # delimiter of '0' signal from above and below – log-return units
        # sklearn.PCA(n_components=...) already accepts either:
        # int, e.g. 6: keep exactly 6 principal components
        # float, e.g. 0.95: keep however many components explain 95% of variance
        self.pca_n_components = self.params.get("pca_n_components", 0.95)
        self.position_size = self.params.get("position_size", 1.0)

        self.start_date = self.params.get("start_date", "2025-01-01")
        self.end_date = self.params.get("end_date", "2026-01-01")
        self.training_years = self.params.get("training_years", 10)
        self.indicator_warmup_years = self.params.get("indicator_warmup_years", 1)  # extra data to download before training window to account for indicators that require more history (e.g. DPO with length 20 would need at least 20 days of data before the training window starts)

        self.download_data = self.params.get("download_data", False)
        self.adj_method = self.params.get("adj_method", "corporate_actions")
        valid_adj_methods = {"corporate_actions", "adj_close_ratio"}
        if self.adj_method not in valid_adj_methods:
            raise ValueError(f"adj_method must be one of {valid_adj_methods}")

        # Rolling Sortino filter parameters, expressed in bars/periods.
        # self.sortino_filter_enabled = self.params.get("sortino_filter_enabled", True)

        # Rolling Sharpe filter parameters, expressed in bars/periods.
        self.sharpe_filter_enabled = self.params.get("sharpe_filter_enabled", True)
        self.rolling_sharpe_window = int(self.params.get("rolling_sharpe_window", 63))  # 21 days in typical stock trading month
        self.rolling_sharpe_min_periods = int(self.params.get("rolling_sharpe_min_periods", self.rolling_sharpe_window))
        self.rolling_sharpe_annualisation = self.params.get("rolling_sharpe_annualization", 252)

        # Hysteresis thresholds: enter position only above enter-threshold, exit only below exit-threshold.
        self.sharpe_enter_threshold = self.params.get("sharpe_enter_threshold", 1.05)
        self.sharpe_exit_threshold = self.params.get("sharpe_exit_threshold", 0.95)

        # Prediction at bar t is realised over Open t+1 to Open t+2, so performance is known with lag 2.
        self.sharpe_performance_lag = int(self.params.get("sharpe_performance_lag", 2))

        # Stores prediction, shadow returns, rolling Sharpe, trust state, and final weights.
        self.weight_diagnostics_ = None

        # Validate Sharpe filter settings early.
        if self.rolling_sharpe_window <= 0:
            raise ValueError("rolling_sharpe_window must be positive integer.")
        if self.rolling_sharpe_min_periods <= 0 or self.rolling_sharpe_min_periods > self.rolling_sharpe_window:
            raise ValueError("rolling_sharpe_min_periods must be between 1 and rolling_sharpe_window.")
        if self.sharpe_exit_threshold > self.sharpe_enter_threshold:
            raise ValueError("sharpe_exit_threshold should be <= sharpe_enter_threshold.")
        
        self.validate_pca_n_components()

        # PCA diagnostics: stores component variance and feature loadings each day.
        self.feature_diagnostics_enabled = self.params.get("feature_diagnostics_enabled", True)
        self.feature_diagnostics_top_n = self.params.get("feature_diagnostics_top_n", 10)

        # These are populated during predict() / walk_forward_predictions().
        self.pca_components_by_date_ = {}
        self.pca_components_list_ = []
        self.feature_selection_records_ = []
        self.feature_selection_diagnostics_ = pd.DataFrame()

        # Validate feature-diagnostics settings early.
        if self.feature_diagnostics_top_n is not None and int(self.feature_diagnostics_top_n) <= 0:
            raise ValueError("feature_diagnostics_top_n must be positive or None.")

        # Other paramaters, rarely varied
        self.nan_threshold = self.params.get("nan_threshold", 0.05)
        self.random_state = self.params.get("random_state", 42)
        self.max_iter = self.params.get("max_iter", 5000)
        self.verbose = self.params.get("verbose", True)
        self.max_abs_feature_value = self.params.get("max_abs_feature_value", 1e100)

        if self.max_abs_feature_value is not None and self.max_abs_feature_value <= 0:
            raise ValueError("max_abs_feature_value must be positive or None.")



    def validate_pca_n_components(self) -> None:

        # Validate PCA parameter before the first rolling fit.
        if isinstance(self.pca_n_components, bool):
            raise ValueError("pca_n_components must be a positive integer or a float between 0 and 1.")

        if isinstance(self.pca_n_components, Integral):
            if self.pca_n_components <= 0:
                raise ValueError("Integer pca_n_components must be positive.")
            return

        if isinstance(self.pca_n_components, Real):
            if not 0.0 < self.pca_n_components < 1.0:
                raise ValueError("Float pca_n_components must be between 0 and 1, e.g. 0.95.")
            return

        raise ValueError("pca_n_components must be a positive integer or a float between 0 and 1.")



    def validate_pca_available_components(self, X_train: pd.DataFrame) -> None:

        # PCA cannot keep more components than the fitted matrix rank dimensions allow.
        max_components = min(X_train.shape[0], X_train.shape[1])

        if isinstance(self.pca_n_components, Integral) and self.pca_n_components > max_components:
            raise ValueError(
                f"pca_n_components ({self.pca_n_components}) cannot be larger than "
                f"min(n_training_rows, n_available_features) ({max_components})."
            )



    def get_prediction_start_date(self, df: pd.DataFrame) -> pd.Timestamp:

        # If the Sharpe filter is off, predictions start at the normal backtest start date.
        backtest_start = pd.to_datetime(self.start_date)

        if not self.sharpe_filter_enabled:
            return backtest_start

        # Move back just enough bars so rolling Sharpe can be valid on start_date.
        warmup_bars = self.rolling_sharpe_min_periods + self.sharpe_performance_lag - 1

        # Find the first dataset row at or after start_date, then step backward by warmup bars.
        start_loc = df.index.searchsorted(backtest_start, side="left")

        if start_loc >= len(df.index):
            return backtest_start

        warmup_loc = max(0, start_loc - warmup_bars)

        return df.index[warmup_loc]



    def download_dataset(self, symbol: str = None, start_date: str = None, end_date: str = None, actions: bool = None, training_years: float = None, indicator_warmup_years: float = None) -> pd.DataFrame:

        symbol = symbol or self.symbol
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date

        # Download corporate actions only if they're needed or else if explicitly asked for
        if self.adj_method == "corporate_actions":
            actions = True
        elif self.adj_method == "adj_close_ratio":
            actions = actions or False
        
        training_years = self.training_years if training_years is None else training_years
        indicator_warmup_years = self.indicator_warmup_years if indicator_warmup_years is None else indicator_warmup_years

        # Download extra data for warmup window
        start_date_download = pd.to_datetime(start_date) - pd.DateOffset(years=training_years + indicator_warmup_years)
        start_date_download = start_date_download.strftime("%Y-%m-%d")

        # =========================
        # Download data
        # =========================
        if self.verbose:
            print(f"⬇️ Downloading data for {symbol} from {start_date_download} to {end_date}...")

        # Download data from Yahoo Finance
        df = yf.download(
            tickers=symbol,
            start=start_date_download,
            end=end_date,                   # Note: 'end' is exclusive
            actions=actions, 
            auto_adjust=False,
            progress=self.verbose,
            threads=True,
            timeout=20,
            multi_level_index=False,
            )

        # Since we only download one ticker, remove unnecessary Ticker MultiIndex
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df.rename(columns={
            "Adj Close": "Adj_Close",
            "Stock Splits": "Stock_Splits",
            "Capital Gains": "Capital_Gains",
            },
            inplace=True,
        )

        df.columns.name = symbol

        return df
    


    def load_stored_dataset(self, symbol: str = None, start_date: str = None, end_date: str = None, training_years: float = None, indicator_warmup_years: float = None) -> pd.DataFrame:

        symbol = symbol or self.symbol
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date
        training_years = self.training_years if training_years is None else training_years
        indicator_warmup_years = self.indicator_warmup_years if indicator_warmup_years is None else indicator_warmup_years

        # Load extra data for warmup window
        start_date_load = pd.to_datetime(start_date) - pd.DateOffset(years=training_years + indicator_warmup_years)
        start_date_load = start_date_load.strftime("%Y-%m-%d")

        # =========================
        # Load stored data
        # =========================
        if self.verbose:
            print(f"⬇️ Loading stored data for {symbol} from {start_date_load} to {end_date}...")

        # Locate the Yahoo data directory relative to the repo root
        repo_root = Path(__file__).resolve().parents[3]
        yahoo_root = repo_root / "src" / "data" / "yahoo"

        # Use regex to match the symbol to the filename: subsitute any 
        # non-letter-number-dot-underscore-hyphen with an underscore, and strip leading/trailing underscores
        safe_symbol = re.sub(r"[^A-Za-z0-9._-]+", "_", symbol).strip("_")
        # Find all matching CSV for the symbol in the Yahoo directory
        csv_matches = list(yahoo_root.glob(f"*/1d/{safe_symbol}.csv"))

        # Raise an error if none or multiple matching CSV files are found
        if not csv_matches:
            raise FileNotFoundError(f"No stored Yahoo CSV found for {symbol} under {yahoo_root}")
        if len(csv_matches) > 1:
            raise ValueError(f"Multiple stored Yahoo CSVs found for {symbol}: {csv_matches}")

        # Load the CSV into a DataFrame and keep the selected date range
        df = pd.read_csv(csv_matches[0], parse_dates=["Timestamp"])
        df = df[(df["Timestamp"] >= start_date_load) & (df["Timestamp"] < end_date)].copy()        # Note: 'end' is exclusive like in download_dataset()
        df.set_index("Timestamp", inplace=True)

        df.columns.name = symbol

        return df
    


    def adjust_dataset(self, df: pd.DataFrame, adjustment_method: str = "corporate_actions") -> pd.DataFrame:

        # Check that the raw dataset has the OHLCV columns needed for and after adjustment
        required_cols = ["Open", "High", "Low", "Close", "Volume"]
        if adjustment_method == "corporate_actions":
            required_cols += ["Dividends"]
        elif adjustment_method == "adj_close_ratio":
            required_cols += ["Adj_Close"]
        else:
            raise ValueError("adjustment_method must be 'corporate_actions' or 'adj_close_ratio'")

        # Error if some required column is missing
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Cannot adjust dataset. Missing columns: {missing_cols}")

        # Work on a copy so the original DataFrame is not changed in place
        adjusted_df = df.copy()

        # Choose how to calculate the price adjustment factor
        if adjustment_method == "corporate_actions":
            adjustment_factor = self.calculate_adjustment_factor(adjusted_df)
        elif adjustment_method == "adj_close_ratio":
            adjustment_factor = self.calculate_adj_close_ratio_factor(adjusted_df)

        # Apply the same adjustment factor to all OHLC prices
        for col in ["Open", "High", "Low", "Close"]:
            adjusted_df[col] = adjusted_df[col] * adjustment_factor

        # When using Yahoo's own ratio, force Close to Adj_Close exactly
        if adjustment_method == "adj_close_ratio":
            adjusted_df["Close"] = adjusted_df["Adj_Close"]

        # Keep only (adjusted) OHLCV columns and make a copy
        adjusted_df = adjusted_df[["Open", "High", "Low", "Close", "Volume"]].copy()
        
        return adjusted_df
    


    def calculate_adj_close_ratio_factor(self, df: pd.DataFrame) -> pd.Series:

        # Check that the raw dataset has the inputs needed for the ratio
        required_cols = ["Close", "Adj_Close"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Cannot calculate Adj_Close ratio factor. Missing columns: {missing_cols}")

        # Calculate Yahoo's implied adjustment factor from adjusted close versus raw close
        adjustment_factor = df["Adj_Close"] / df["Close"]

        # Replace divide-by-zero infinities with NaN so bad rows do not become infinite prices
        adjustment_factor = adjustment_factor.replace([np.inf, -np.inf], np.nan)

        return adjustment_factor



    def calculate_adjustment_factor(self, df: pd.DataFrame) -> pd.Series:

        # NOTE: Yahoo's OHLC prices are ALREADY SPLIT-ADJUSTED even when auto_adjust=False, 
        # so we do not reapply Stock_Splits to adjustment factor

        # Check that the raw dataset has the inputs needed to calculate adjustments
        required_cols = ["Close", "Dividends"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Cannot calculate adjustment factor. Missing columns: {missing_cols}")

        # Keep the original index so the returned factor lines up with the input data
        original_index = df.index

        # Include capital gains when Yahoo provides them, mostly for funds/ETFs
        optional_cols = [col for col in ["Capital_Gains"] if col in df.columns]

        # Work on a date-sorted copy because cumulative adjustments depend on time order
        actions_df = df[required_cols + optional_cols].copy().sort_index()

        # Convert inputs to numeric values and treat missing actions as no action
        close = pd.to_numeric(actions_df["Close"], errors="coerce")
        dividends = pd.to_numeric(actions_df["Dividends"], errors="coerce").fillna(0.0)
        if "Capital_Gains" in actions_df.columns:
            capital_gains = pd.to_numeric(actions_df["Capital_Gains"], errors="coerce").fillna(0.0)
        else:
            capital_gains = pd.Series(0.0, index=actions_df.index)
        cash_distributions = dividends + capital_gains

        # Start with no adjustment on every date
        event_factor = pd.Series(1.0, index=actions_df.index)

        # Cash distribution adjustment uses the close from the day before the ex-date
        previous_close = close.shift(1)
        distribution_factor = (previous_close - cash_distributions) / previous_close
        valid_distribution = cash_distributions.ne(0.0) & previous_close.notna() & previous_close.ne(0.0)
        event_factor.loc[valid_distribution] *= distribution_factor.loc[valid_distribution]

        # Apply each event only to dates before the event date, not to the event date itself
        adjustment_factor = event_factor.iloc[::-1].cumprod().iloc[::-1] / event_factor

        # Return the factor in the same row order as the input DataFrame
        adjustment_factor = adjustment_factor.reindex(original_index)

        return adjustment_factor



    def assign_signal(self, df: pd.DataFrame) -> pd.DataFrame:

        # =========================
        # Assign signal
        # =========================

        if self.verbose:
            print("🧪 Assigning signal to historic dataset...")

        # Compute next days' Open
        df["Open_t+1"] = df["Open"].shift(-1)
        df["Open_t+2"] = df["Open"].shift(-2)

        # Assign signal from the next open-to-open move
        open_return = np.log( df["Open_t+2"] / df["Open_t+1"] )

        df["Signal"] = np.select(
            [open_return > self.signal_threshold, open_return < -self.signal_threshold],
            [1, -1],
            default=0
        )
        df.loc[open_return.isna(), "Signal"] = np.nan

        return df



    def compute_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:

        # =============================================================================================
        # Compute all technical indicators with Pandas-TA (and TA-Lib internally if installed)
        # =============================================================================================

        if self.verbose:
            print("🧪 Computing technical indicators...")

        # Run "strategy" to compute and append all available technical indicators.
        # Disable pandas-ta's inner multiprocessing when this strategy runs inside joblib/loky.
        df.ta.cores = 0
        df.ta.strategy(lookahead=False)

        # Drop DPO created by df.ta.strategy(), because default DPO may be centered/lookahead-biased
        df = df.drop(columns=["DPO_20"], errors="ignore")

        # Recompute DPO without lookahead/centering
        df.ta.dpo(close="Close", length=20, lookahead=False, append=True)

        # Drop all Ichimoku indicators, then recompute – ICS_26 looks 26 days ahead
        df = df.drop(columns=["ISA_9", "ISB_26", "ITS_9", "IKS_26", "ICS_26"], errors="ignore")

        # Recompute Ichimoku indicators wihtout lookahead (ie. without ICS_26)
        df.ta.ichimoku(lookahead=False, append=True)

        # Drop all TOS_STDEVALL indicators, then recompute for finite length
        df = df.drop(columns=["TOS_STDEVALL_LR", "TOS_STDEVALL_L_1", "TOS_STDEVALL_U_1",
                               "TOS_STDEVALL_L_2", "TOS_STDEVALL_U_2", "TOS_STDEVALL_L_3", 
                               "TOS_STDEVALL_U_3"], errors="ignore")

        # Recompute TOS_STDEVALL indicator for finite lengths
        for length in [21, 63, 126, 252, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000]: 
            if length < len(df):
                # linear regression
                lr = df.ta.linreg(close="Close", length=length, append=False)
                # standard deviation
                sd = df.ta.stdev(close="Close", length=length, append=False)

                df[f"TOS_STDEVALL_{length}_LR"] = lr

                for n in [1, 2, 3]:
                    df[f"TOS_STDEVALL_{length}_L_{n}"] = lr - n * sd
                    df[f"TOS_STDEVALL_{length}_U_{n}"] = lr + n * sd

        # Include more increasing and decreasing pattern indicators (INC_n and DEC_n) for n=2 to 10 days back 
        # (n=1 is already included by df.ta.strategy() )
        # These track if the  Close price has been increasing or decreasing for n consecutive days, which may be useful for trend detection.
        for n in range(2, 11):
            df[f"INC_{n}"] = df.ta.increasing(close="Close", length=n, strict=True, asint=True, append=True)

            df[f"DEC_{n}"] = df.ta.decreasing(close="Close", length=n, strict=True, asint=True, append=True)


        return df



    def list_technical_indicators(self, df: pd.DataFrame) -> list[str]:

        # ==================================================================
        # List all technical indicators in the dataset
        # ==================================================================

        cols = df.columns.tolist()

        # Find total number of columns (technical indicators) and how many were added by Pandas-TA
        n_total = len(cols)
        n_added = n_total - 8  # there were already 8 to start with: OHLCV, Open_t+1, Open_t+2, Signal

        # Display all column names
        print(cols)
        # display(pd.DataFrame({"Column": cols}))

        print("\n", f"Total number of technical indicators: {n_total}")
        print("\n", f"Added TA columns: {n_added}", "\n")

        return cols
    


    def nan_pct_per_column(self, df: pd.DataFrame) -> pd.Series:

        # ==================================================================
        # Calculate percentage of NaNs per column
        # ==================================================================

        # Percentage of NaNs per column
        nan_pct = df.isna().mean().copy() * 100

        nan_pct.index.name = 'Tech Indicators NaN % (sorted, first 15)'
        display(nan_pct.sort_values(ascending=False).head(15))

        return nan_pct
    


    def scan_nan_thresholds(self, df: pd.DataFrame, threshold_list: list[int] = None) -> None:
        
        # ==================================================================
        # Scan thresholds from 0% to 60%
        # ==================================================================

        if threshold_list is None:
            threshold_list = [0, 5, 10, 15, 20, 25, 30, 40, 50, 60]

        nan_pct = self.nan_pct_per_column(df)

        # Scan thresholds from 0% to 60%
        for x in threshold_list:
            cols = nan_pct[nan_pct > x].index.tolist()

            print(f"\n Number of columns with > {x}% NaNs: {len(cols)}")
            # print(cols)       # Uncomment if you want to see names



    def clean_dataset(self, df: pd.DataFrame) -> pd.DataFrame:

        # ====================================================================================================
        # Data cleaning – drop columns and rows with too many NaNs, non-numeric and auxiliary columns
        # ====================================================================================================

        if self.verbose:
            print("🧹 Cleaning dataset...")

        # convert any infinite values to NaN (which will be dropped later)
        df = df.replace([np.inf, -np.inf], np.nan)

        # store row and column info before cleaning
        rows_before_cleaning = [df.shape[0], df.index.strftime("%Y-%m-%d").tolist()]
        cols_before_cleaning = [df.shape[1], df.columns.tolist()]

        # Calculate NaN integer threshold - i.e. minimum number of non-NaNs to keep the column
        threshold = int((1 - self.nan_threshold) * df.shape[0])

        # Drop columns with too many NaNs (e.g. >5%)
        df = df.dropna(thresh=threshold, axis=1).copy()

        # Drop auxiliary "Open_t+1" and "Open_t+2" columns
        df = df.drop(columns=['Open_t+1', 'Open_t+2'], errors='ignore')

        # Drop non-numeric columns (if any)
        df = df.select_dtypes(include=['number'])

        # store column info after cleaning
        cols_after_cleaning = df.shape[1]
        cols_removed_num = cols_before_cleaning[0] - cols_after_cleaning
        cols_removed_list = [x for x in cols_before_cleaning[1] if x not in df.columns.tolist()]

        # # Drop rows with any remaining NaNs except for the last two rows
        df = pd.concat([df.iloc[:-2].dropna(), df.iloc[-2:]]).copy()

        # store row info after cleaning
        rows_after_cleaning = df.shape[0]
        rows_cleaned_num = rows_before_cleaning[0] - rows_after_cleaning
        rows_cleaned_list = [x for x in rows_before_cleaning[1] if x not in df.index.strftime("%Y-%m-%d").tolist()]


        # Visualise
        if self.verbose:
            print(f"☑️ {cols_removed_num} NaN, non-numeric and auxiliary columns have been removed:")
            print(cols_removed_list)
            print(f"☑️ {rows_cleaned_num} rows with NaN have been cleaned:")
            print(rows_cleaned_list)
            print(f"➡️ We have {rows_after_cleaning} rows and {df.shape[1]} columns (features = OHLCV + indicators + signal) in the dataset.")


        # ==================================================
        # Diagnose and remove globally constant features
        # ==================================================

        # Identify columns with only one unique value
        global_constant_cols = df.columns[df.nunique(dropna=False) <= 1].copy()

        # Report globally constant features to delete
        if self.verbose:
            print(f"☑️ {len(global_constant_cols)} features are globally constant:")
            print(global_constant_cols.tolist())
            print("➡️ They will be removed as they provide no information for ML model.")

        # Drop constant features
        df = df.drop(columns=global_constant_cols).copy()

        # ==================================================
        # Diagnose and remove numerically unsafe features
        # ==================================================

        # Some pandas-ta indicators such as EXP, SINH, and COSH can create finite but enormous
        # values. StandardScaler estimates variance, so squaring those values can overflow and
        # create NaNs before PCA. Drop only feature columns whose magnitude is unsafe.
        if self.max_abs_feature_value is not None:
            feature_cols = df.columns.drop("Signal", errors="ignore")
            feature_abs_max = df[feature_cols].abs().max(axis=0)
            unsafe_feature_cols = feature_abs_max[feature_abs_max > self.max_abs_feature_value].index.copy()

            if self.verbose:
                print(f"☑️ {len(unsafe_feature_cols)} numerically unsafe features exceed max_abs_feature_value={self.max_abs_feature_value:g}:")
                print(unsafe_feature_cols.tolist())
                print("➡️ They will be removed to prevent scaler/PCA overflow.")

            df = df.drop(columns=unsafe_feature_cols).copy()

        # Visualise
        if self.verbose:
            print("✅ We now have", rows_after_cleaning, "rows and", df.shape[1], "columns (features = OHLCV + indicators + signal) in the dataset.", "\n")

        return df
    


    def prepare_dataset(self) -> pd.DataFrame:
    
        if self.download_data:
            df = self.download_dataset()
        else:
            df = self.load_stored_dataset()

        df = self.adjust_dataset(df, adjustment_method=self.adj_method)

        df = self.assign_signal(df)

        df = self.compute_technical_indicators(df).copy()
        
        df = self.clean_dataset(df)

        return df




    def feature_target_split_dataset(self, df: pd.DataFrame) -> tuple[ pd.DataFrame, pd.Series ]:

        # =========================
        # Feature / target split
        # =========================

        # Separate features X and target y
        X = df.drop(columns=["Signal"]).copy()
        y = df["Signal"].copy()
        
        return X, y



    def build_pipeline(self) -> Pipeline:

        # ===========================================================================
        # Build MLP pipeline with PCA dimensionality reduction and scaling
        # ===========================================================================

        pipeline = Pipeline([
            ("variance_filter", VarianceThreshold(threshold=0.0)),
            ("scaler", StandardScaler()),
            ("pca", PCA(n_components=self.pca_n_components, svd_solver="full")),
            ("mlp", MLPClassifier(random_state=self.random_state, max_iter=self.max_iter)),
        ])

        return pipeline



    def train_and_fit(self, X_train: pd.DataFrame, y_train: pd.Series) -> Pipeline:

        # ================================================================
        # Train the MLP with past data and fit the model
        # ================================================================

        # Create a fresh model pipeline for this date.
        model = self.build_pipeline()

        # Silence convergence warnings if max_iter is too low
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=ConvergenceWarning)
            # Train the model using only the training window
            model.fit(X_train, y_train)

        return model



    def reset_feature_diagnostics(self) -> None:

        # Reset per-run feature diagnostics so repeated runs do not append stale records.
        self.pca_components_by_date_ = {}
        self.pca_components_list_ = []
        self.feature_selection_records_ = []
        self.feature_selection_diagnostics_ = pd.DataFrame()



    def record_pca_diagnostics( self, prediction_date: pd.Timestamp, model: Pipeline, X_train: pd.DataFrame) -> None:

        '''
        Store PCA components for one prediction date, together with explained
        variance and the largest original-feature loadings per component.
        '''

        # Skip silently when diagnostics are disabled.
        if not self.feature_diagnostics_enabled:
            return

        # Recover feature names after the pipeline's VarianceThreshold step.
        variance_filter = model.named_steps["variance_filter"]
        kept_after_variance = X_train.columns[variance_filter.get_support()]

        # Recover PCA component loadings and explained variance diagnostics.
        pca = model.named_steps["pca"]
        component_names = [f"PC{i + 1}" for i in range(pca.n_components_)]
        explained_variance_ratio = pd.Series(
            pca.explained_variance_ratio_,
            index=component_names,
            name="explained_variance_ratio",
        )
        cumulative_explained_variance = explained_variance_ratio.cumsum()

        # Store the exact PCA components passed through to the MLP on this date.
        self.pca_components_by_date_[prediction_date] = component_names
        self.pca_components_list_ = [
            {"prediction_date": date, "components": components}
            for date, components in self.pca_components_by_date_.items()
        ]

        # Store the top original-feature contributors to each component.
        loadings = pd.DataFrame(
            pca.components_,
            index=component_names,
            columns=kept_after_variance,
        )

        for component in component_names:
            component_loadings = loadings.loc[component].replace([np.inf, -np.inf], np.nan)
            ranked_loadings = component_loadings.abs().sort_values(ascending=False)

            if self.feature_diagnostics_top_n is not None:
                ranked_loadings = ranked_loadings.head(int(self.feature_diagnostics_top_n))

            for rank, feature in enumerate(ranked_loadings.index, start=1):
                loading = component_loadings.loc[feature]
                self.feature_selection_records_.append({
                    "prediction_date": prediction_date,
                    "component": component,
                    "feature": feature,
                    "loading": loading,
                    "abs_loading": abs(loading),
                    "rank": rank,
                    "explained_variance_ratio": explained_variance_ratio.loc[component],
                    "cumulative_explained_variance": cumulative_explained_variance.loc[component],
                    "n_components": pca.n_components_,
                    "selected": True,
                })

        # Keep a DataFrame version ready for notebook inspection.
        self.feature_selection_diagnostics_ = pd.DataFrame(self.feature_selection_records_)



    def plot_feature_frequency( self, top_n: int = 30, selected_only: bool = True, start_date: str | pd.Timestamp = None, end_date: str | pd.Timestamp = None):

        '''
        Plot how frequently features appeared among top PCA loadings per date/component.
        '''

        # Require a completed prediction run first.
        if self.feature_selection_diagnostics_ is None or self.feature_selection_diagnostics_.empty:
            raise ValueError("No feature diagnostics available. Run generate_signals() first.")

        diagnostics = self.feature_selection_diagnostics_.copy()

        # Default to the configured prediction window, excluding any Sharpe warm-up diagnostics.
        start_date = self.start_date if start_date is None else start_date
        end_date = self.end_date if end_date is None else end_date

        # Optionally restrict to diagnostic rows marked as selected and/or a date window.
        if selected_only:
            diagnostics = diagnostics[diagnostics["selected"]]

        diagnostics = diagnostics[diagnostics["prediction_date"] >= pd.to_datetime(start_date)]

        diagnostics = diagnostics[diagnostics["prediction_date"] <= pd.to_datetime(end_date)]

        if diagnostics.empty:
            raise ValueError("No feature diagnostics remain after filtering.")

        # Count how often each feature appears among top PCA loadings, then plot most frequent features.
        feature_counts = diagnostics["feature"].value_counts().head(top_n).sort_values()

        ax = feature_counts.plot(
            kind="barh",
            figsize=(10, max(4, 0.35 * len(feature_counts))),
            color="#4C78A8",
        )

        ax.set_title("PCA Loading Feature Frequency")
        ax.set_xlabel("Number of Prediction Dates")
        ax.set_ylabel("Feature")
        plt.tight_layout()

        return ax



    def predict(self, df: pd.DataFrame, show_progress: bool = True, prediction_start_date: str | pd.Timestamp = None) -> pd.Series:
        
        '''
        Simulates the “real trading” process: on each date, train using only past data, 
        predict today's signal, then move forward one day.
        '''

        # ================================================================
        # Predict the signal for prediction_date
        # ================================================================

        df = df.copy()

        # Start earlier when Sharpe-filter warm-up predictions are needed
        prediction_start_date = self.start_date if prediction_start_date is None else prediction_start_date
        # get the indices (dates) between the configured start and end prediction dates
        prediction_date_range = df.loc[prediction_start_date:self.end_date].index
        # count how many dates are in that prediction window
        total_dates = len(prediction_date_range)
        # initialize empty Series with all date indices to store predictions
        predictions = pd.Series(index=prediction_date_range, dtype=float, name="prediction")

        # Reset feature diagnostics for this prediction run.
        self.reset_feature_diagnostics()

        # start timer to report how long all predictions take
        predict_start_time = time.time()
        last_elapsed = 0.0

        # walk forward through each date, train on training_years past data, and predict the signal for that date
        for i, prediction_date in enumerate(prediction_date_range):
            
            # start timer to report how long each predictions takes
            start_time = time.time()

            # print progress and time taken if verbose=true
            if show_progress or self.verbose:
                elapsed_time = time.time() - predict_start_time
                if elapsed_time > 60:
                    elapsed_time_str = f"{int(elapsed_time//60)}m {elapsed_time%60:.1f}s"
                else:
                    elapsed_time_str = f"{elapsed_time:.1f}s"

                print(
                    f"\r🧠 Predicting {prediction_date.date()} | ",
                    f"{i + 1}/{total_dates} | ",
                    f"last: {last_elapsed:.2f}s | ",
                    f"elapsed: {elapsed_time_str}",
                    end="",
                    flush=True,
                )

            # define beginning of training window
            train_start = prediction_date - pd.DateOffset(years=self.training_years)

            # Skip this date if train_start is earlier than first date in dataset.
            if train_start < df.index[0]:
                continue

            # Train MLP with past data up to day i-2 (yesterday (i-1) would not have a signal),
            # to then predict signal for today (i) which will execute tomorrow at the open (with price Open_t+1).
            train_end = df.index[df.index.get_loc(prediction_date) - 2]

            # Take only the training range of data
            train_df = df.loc[train_start:train_end]

            # Prepare training dataset, split into features and target
            X_train, y_train = self.feature_target_split_dataset(train_df)

            self.validate_pca_available_components(X_train)
            
            # Extract only the features from the row we want to predict.
            X_predict, _ = self.feature_target_split_dataset(df.loc[[prediction_date]])


            # Train and fit the MLP model using specific training dataset for this date.
            model = self.train_and_fit(X_train, y_train)

            # Record PCA components and top original-feature loadings for this date.
            self.record_pca_diagnostics(prediction_date, model, X_train)

            # Use the trained model to predict the signal for prediction_date.
            predictions.loc[prediction_date] = model.predict(X_predict)[0]

            last_elapsed = time.time() - start_time


        # print progress and time taken if verbose=true
        if show_progress or self.verbose:
            time_predict = time.time() - predict_start_time
            if time_predict > 60:
                print(f"\n✅ Prediction complete. Took {int(time_predict//60)}m {time_predict%60:.1f}s\n")
            else:
                print(f"\n✅ Prediction complete. Took {time_predict:.1f}s\n")


        return predictions.fillna(value=0)



    def predict_debug(self, df: pd.DataFrame, show_progress: bool = True) -> pd.Series:
        
        '''
        This function prints and displays many things along the prediction process to be able to
        debug what is wrong with the prediction process.
        '''

        # ================================================================
        # Predict the signal for prediction_date
        # ================================================================

        df = df.copy()

        # get the indices (dates) between the configured start and end prediction dates
        prediction_date_range = df.loc[self.start_date:self.end_date].index
        # count how many dates are in that prediction window
        total_dates = len(prediction_date_range)
        # initialize empty Series with all date indices to store predictions
        predictions = pd.Series(index=prediction_date_range, dtype=float, name="prediction")

        predict_start_time = time.time()
        last_elapsed = 0.0

        # walk forward through each date, train on training_years past data, and predict the signal for that date
        for i, prediction_date in enumerate(prediction_date_range):
            
            start_time = time.time()

            # print progress and time taken if verbose=true
            if show_progress or self.verbose:
                elapsed_time = time.time() - predict_start_time
                if elapsed_time > 60:
                    elapsed_time_str = f"{int(elapsed_time//60)}m {elapsed_time%60:.1f}s"
                else:
                    elapsed_time_str = f"{elapsed_time:.1f}s"

                print(
                    f"\r🧠 Predicting {prediction_date.date()} | ",
                    f"{i + 1}/{total_dates} | ",
                    f"last: {last_elapsed:.2f}s | ",
                    f"elapsed: {elapsed_time_str}",
                    end="",
                    flush=True,
                )

            # define beginning of training window
            train_start = prediction_date - pd.DateOffset(years=self.training_years)

            # Skip this date if train_start is earlier than first date in dataset.
            if train_start < df.index[0]:
                continue

            # Train MLP with past data up to day i-2 (yesterday (i-1) would not have a signal),
            # to then predict signal for today (i) which will execute tomorrow at the open (with price Open_t+1).
            train_end = df.index[df.index.get_loc(prediction_date) - 2]

            # Take only the training range of data
            train_df = df.loc[train_start:train_end]

            # Prepare training dataset, split into features and target
            X_train, y_train = self.feature_target_split_dataset(train_df)

            self.validate_pca_available_components(X_train)
            
            # Extract only the features from the row we want to predict.
            X_predict, _ = self.feature_target_split_dataset(df.loc[[prediction_date]])
            display(X_predict.tail(1))

            # Train and fit the MLP model using specific training dataset for this date.
            model = self.train_and_fit(X_train, y_train)
            display(model)

        #     # Use the trained model to predict the signal for prediction_date.
        #     predictions.loc[prediction_date] = model.predict(X_predict)[0]

        #     last_elapsed = time.time() - start_time


        # # print progress and time taken if verbose=true
        # if show_progress or self.verbose:
        #     time_predict = time.time() - predict_start_time
        #     if time_predict > 60:
        #         print(f"\n✅ Prediction complete. Took {int(time_predict//60)}m {time_predict%60:.1f}s\n")
        #     else:
        #         print(f"\n✅ Prediction complete. Took {time_predict:.1f}s\n")


        # return predictions.fillna(value=0)
        # return X_predict
        return model



    def sharpe_filtered_signal(self, df: pd.DataFrame, predictions: pd.Series) -> pd.DataFrame:

        '''
        Convert raw MLP predictions into target weights, filtered by the predictor's
        recently realized shadow-strategy rolling Sharpe.
        '''

        # Work on copies so diagnostics and intermediate calculations do not mutate inputs.
        df = df.copy()
        predictions = predictions.copy().astype(float)

        if "Open" not in df.columns:
            raise ValueError("sharpe_filtered_signal requires an 'Open' column in df.")

        # Raw model weights: this is the always-follow-the-predictor shadow strategy.
        raw_weights = predictions * self.position_size
        raw_weights.name = "raw_weight"

        # Prediction at t targets the open-to-open move from t+1 to t+2.
        forward_open_return = (df["Open"].shift(-2) / df["Open"].shift(-1) - 1.0)
        forward_open_return = forward_open_return.reindex(predictions.index).rename("forward_open_return")

        # Shadow returns measure how the predictor would have performed if always followed.
        shadow_return = (raw_weights * forward_open_return).rename("shadow_return")

        # Compute rolling Sharpe in bars, with annualization controlled by params.
        rolling_mean = shadow_return.rolling(
            window=self.rolling_sharpe_window,
            min_periods=self.rolling_sharpe_min_periods,
        ).mean()

        rolling_vol = shadow_return.rolling(
            window=self.rolling_sharpe_window,
            min_periods=self.rolling_sharpe_min_periods,
        ).std()

        annualiser = 1.0 if self.rolling_sharpe_annualisation is None else np.sqrt(self.rolling_sharpe_annualisation)
        rolling_sharpe = (rolling_mean / rolling_vol.replace(0, np.nan)) * annualiser
        rolling_sharpe = rolling_sharpe.replace([np.inf, -np.inf], np.nan)

        # Lag the Sharpe so today's trust decision only uses already-realized predictor performance.
        rolling_sharpe = rolling_sharpe.shift(self.sharpe_performance_lag).rename("rolling_sharpe")

        # Apply hysteresis: enter above enter threshold, stay on until falling below exit threshold.
        trust_model = pd.Series(False, index=predictions.index, name="trust_model")

        if self.sharpe_filter_enabled:
            trusted = False

            for date, sharpe_value in rolling_sharpe.items():
                if pd.isna(sharpe_value):
                    trusted = False
                elif trusted and sharpe_value < self.sharpe_exit_threshold:
                    trusted = False
                elif not trusted and sharpe_value > self.sharpe_enter_threshold:
                    trusted = True

                trust_model.loc[date] = trusted
        else:
            trust_model.loc[:] = True

        # Final target weights: follow predictor only when recent shadow Sharpe says to trust it.
        final_weight = raw_weights.where(trust_model, 0.0).rename(self.symbol)
        weights = pd.DataFrame(final_weight)

        # Store diagnostics for notebook inspection and debugging.
        self.weight_diagnostics_ = pd.DataFrame({
            "prediction": predictions,
            "raw_weight": raw_weights,
            "forward_open_return": forward_open_return,
            "shadow_return": shadow_return,
            "rolling_sharpe": rolling_sharpe,
            "trust_model": trust_model,
            "final_weight": final_weight,
        }, index=predictions.index)

        return weights



    def generate_signals(self, df: pd.DataFrame = None, show_progress: bool = True) -> pd.DataFrame:
        
        # If no pre-prepared dataset is provided, prepare it first (down/load, assign signal, compute indicators, clean).
        if df is None:
            df = self.prepare_dataset()

        # Start predictions before start_date if the Sharpe filter needs warm-up history.
        prediction_start_date = self.get_prediction_start_date(df)    
        
        # Predict signals for each day sequentially, retraining and refitting the model for each day.
        predictions = self.predict(df, show_progress=show_progress, prediction_start_date=prediction_start_date)

        # Convert predictions into Sharpe-filtered target weights.
        weights = self.sharpe_filtered_signal(df, predictions)

        # Return only the configured live/backtest prediction window.
        weights = weights.loc[self.start_date:self.end_date]

        return weights



    def generate_signals_OLD_before_sharpe_filter(self, df: pd.DataFrame = None, show_progress: bool = True) -> pd.DataFrame:
        
        # If no pre-prepared dataset is provided, prepare it first (down/load, assign signal, compute indicators, clean).
        if df is None:
            df = self.prepare_dataset()
        
        # Predict signals for each day sequentially, retraining and refitting the model for each day.
        predictions = self.predict(df, show_progress=show_progress)

        # Convert predictions to target weights by multiplying by position size.
        weights = pd.DataFrame(index=predictions.index)
        weights[self.symbol] = predictions * self.position_size

        return weights





    """
    The following are versions of the previous functions: one that only predicts the next day, 
    one that walks forward through the dataset to predict each day, and one that generates the signals. 
    These are useful for real-time prediction, for backtesting and evaluating the strategy on 
    historical data an event-event basis.
    """



    def predict_single_event(self, df: pd.DataFrame, prediction_date: pd.Timestamp) -> int | float:
        
        '''
        Simulates the “real trading” process: on each date, train using only past data, 
        predict today's signal.
        '''

        # ================================================================
        # Predict the signal for prediction_date
        # ================================================================

        df = df.copy()

        # define beginning of training window
        train_start = prediction_date - pd.DateOffset(years=self.training_years)

        # Skip this date if train_start is earlier than first date in dataset.
        if train_start < df.index[0]:
            return 0

        # Train MLP with past data up to day i-2 (yesterday (i-1) would not have a signal),
        # to then predict signal for today (i) which will execute tomorrow at the open (with price Open_t+1).
        train_end = df.index[df.index.get_loc(prediction_date) - 2]

        # Take only the training range of data
        train_df = df.loc[train_start:train_end]

        # Prepare training dataset, split into features and target
        X_train, y_train = self.feature_target_split_dataset(train_df)

        self.validate_pca_available_components(X_train)
        
        # Extract only the features from the row we want to predict.
        X_predict, _ = self.feature_target_split_dataset(df.loc[[prediction_date]])


        # Train and fit the MLP model using specific training dataset for this date.
        model = self.train_and_fit(X_train, y_train)

        # Record PCA components and top original-feature loadings for this date.
        self.record_pca_diagnostics(prediction_date, model, X_train)

        # Use the trained model to predict the signal for prediction_date.
        prediction = model.predict(X_predict)[0]


        return prediction
    


    def walk_forward_predictions(self, df: pd.DataFrame, show_progress: bool = True) -> pd.Series:


        # ================================================================
        # Predict the signal for prediction_date
        # ================================================================

        df = df.copy()

        # get the indices (dates) between the configured start and end prediction dates
        prediction_date_range = df.loc[self.start_date:self.end_date].index
        # count how many dates are in that prediction window
        total_dates = len(prediction_date_range)
        # initialize empty Series with all date indices to store predictions
        predictions = pd.Series(index=prediction_date_range, dtype=float, name="prediction")

        # Reset feature diagnostics for this event-style prediction run.
        self.reset_feature_diagnostics()

        # start timer to report how long all predictions take
        predict_start_time = time.time()
        last_elapsed = 0.0

        # walk forward through each date, train on training_years past data, and predict the signal for that date
        for i, prediction_date in enumerate(prediction_date_range):
            
            # start timer to report how long each predictions takes
            start_time = time.time()

            # print progress and time taken if verbose=true
            if show_progress or self.verbose:
                elapsed_time = time.time() - predict_start_time
                if elapsed_time > 60:
                    elapsed_time_str = f"{int(elapsed_time//60)}m {elapsed_time%60:.1f}s"
                else:
                    elapsed_time_str = f"{elapsed_time:.1f}s"

                print(
                    f"\r🧠 Predicting {prediction_date.date()} | ",
                    f"{i + 1}/{total_dates} | ",
                    f"last: {last_elapsed:.2f}s | ",
                    f"elapsed: {elapsed_time_str}",
                    end="",
                    flush=True,
                )

            # define beginning of training window
            train_start = prediction_date - pd.DateOffset(years=self.training_years)

            # Skip this date if train_start is earlier than first date in dataset.
            if train_start < df.index[0]:
                continue
            
            # Use the trained model to predict the signal for prediction_date.
            prediction = self.predict_single_event(df, prediction_date)

            # Update the predictions Series with the predicted signal for this date.
            predictions.loc[prediction_date] = prediction

            last_elapsed = time.time() - start_time


        # print progress and time taken if verbose=true
        if show_progress or self.verbose:
            time_predict = time.time() - predict_start_time
            if time_predict > 60:
                print(f"\n✅ Prediction complete. Took {int(time_predict//60)}m {time_predict%60:.1f}s\n")
            else:
                print(f"\n✅ Prediction complete. Took {time_predict:.1f}s\n")


        return predictions.fillna(value=0)



    def generate_signals_event(self, df: pd.DataFrame = None, show_progress: bool = True) -> pd.DataFrame:
        
        # If no pre-prepared dataset is provided, prepare it first (download, assign signal, compute indicators, clean).
        if df is None:
            df = self.prepare_dataset()
        
        # Predict signals for each day sequentially, retraining and refitting the model for each day.
        predictions = self.walk_forward_predictions(df, show_progress=show_progress)

        # Convert predictions into Sharpe-filtered target weights.
        weights = self.sharpe_filtered_signal(df, predictions)

        return weights
    


    def generate_signals_event_OLD_before_sharpe_filter(self, df: pd.DataFrame = None, show_progress: bool = True) -> pd.DataFrame:
            
        # If no pre-prepared dataset is provided, prepare it first (download, assign signal, compute indicators, clean).
        if df is None:
            df = self.prepare_dataset()
        
        # Predict signals for each day sequentially, retraining and refitting the model for each day.
        predictions = self.walk_forward_predictions(df, show_progress=show_progress)

        # Convert predictions to target weights by multiplying by position size.
        weights = pd.DataFrame(index=predictions.index)
        weights[self.symbol] = predictions * self.position_size

        return weights
