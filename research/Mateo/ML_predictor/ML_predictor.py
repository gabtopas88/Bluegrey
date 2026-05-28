# ======================================
# Initialisation -- import libraries
# ======================================

# Core
import numpy as np
import pandas as pd
import time
from IPython.display import display

# Data download
import yfinance as yf

# Technical indicators
# !pip install pandas-ta  --quiet  # May need to install it first   ------ IGNORE FOR PYTHON 3.11 ------
# !pip install pandas-ta-classic --quiet   # May need to install this too
# !pip install TA-Lib --quiet     # May need to install this too
# import pandas_ta as ta                                          # ------ IGNORE FOR PYTHON 3.11 ------
import pandas_ta_classic as ta

# Machine learning
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import Pipeline
from sklearn.feature_selection import SelectKBest, VarianceThreshold

# Feature analysis
from scipy.stats import pearsonr

# Inherit BaseStrategy
from strategies.base import BaseStrategy


# ==================================================
# Pearson-correlation feature scoring function
# ==================================================

def pearson_score(X: np.ndarray, y: np.ndarray) -> tuple[ np.ndarray, np.ndarray ]:

    # Compute absolute Pearson correlation for each feature
    scores = np.array([
        abs(pearsonr(X[:, i], y)[0])    # Use abs() since feature selection should rank by strength, not sign.
        for i in range(X.shape[1])
    ])

    # Replace NaNs (e.g. constant features) with 0
    scores = np.nan_to_num(scores, nan=0.0)

    # sklearn expects (scores, pvalues) -- return dummy p-values because SelectKBest expects two arrays.
    return scores, np.zeros_like(scores)



# ==================================================
# Class definition for the MLP Predictor Strategy
# ==================================================

class MLPredictorStrategy(BaseStrategy):

    def __init__(self, instruments: dict = None, params: dict = None):
        super().__init__(instruments or {}, params or {})    # initialises self.instruments and self.params 
                                                             # in the BaseStrategy parent class

        self.symbol = self.params.get("symbol", "ECH")
        self.signal_threshold = self.params.get("signal_threshold", 0.02)  # delimiter of '0' signal from above and below – log-return units
        self.n_features = self.params.get("n_features", 8)
        self.position_size = self.params.get("position_size", 0.10)

        self.start_date = self.params.get("start_date", "2008-01-01")
        self.end_date = self.params.get("end_date", "2020-01-01")
        self.training_years = self.params.get("training_years", 10)
        self.indicator_warmup_years = self.params.get("indicator_warmup_years", 1)  # extra data to download before training window to account for indicators that require more history (e.g. DPO with length 20 would need at least 20 days of data before the training window starts)

        self.nan_threshold = self.params.get("nan_threshold", 0.05)
        self.random_state = self.params.get("random_state", 42)
        self.max_iter = self.params.get("max_iter", 2000)
        self.verbose = self.params.get("verbose", True)
        self.model = None      # set model to None initially, will be defined in build_pipeline() later



    def download_dataset(self, symbol: str = None, start_date: str = None, end_date: str = None, training_years: float = None) -> pd.DataFrame:

        symbol = symbol or self.symbol
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date
        training_years = training_years or self.training_years
        indicator_warmup_years = self.indicator_warmup_years

        # Download extra data for training window
        start_date_download = pd.to_datetime(start_date) - pd.DateOffset(years=training_years + indicator_warmup_years)
        start_date_download = start_date_download.strftime("%Y-%m-%d")

        # =========================
        # Download data
        # =========================
        if self.verbose:
            print(f"⬇️ Downloading data for {symbol} from {start_date_download} to {end_date}...")

        # Download data from Yahoo Finance
        df = yf.download(
            symbol,
            start=start_date_download,
            end=end_date,                   # Note: 'end' is exlusive
            auto_adjust=True,
            progress=self.verbose
            )

        # Since we only download one ticker, remove unnecessary Ticker MultiIndex
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # Keep only OHLCV columns (auto-adjusted from download) and make a copy
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()

        df.columns.name = self.symbol

        return df
    


    def assign_signal(self, df: pd.DataFrame) -> pd.DataFrame:

        # =========================
        # Assign signal
        # =========================

        if self.verbose:
            print("🧪 Assigning singal to historic dataset...")

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

        # ==================================================================
        # Compute all technical indicators with Pandas-TA and TA-Lib
        # ==================================================================

        if self.verbose:
            print("🧪 Computing technical indicators...")

        # Run "strategy" to compute and append all available technical indicators.
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
        for length in [30, 90, 180, 252, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000]: 
            if length < len(df):
                # linear regression
                lr = df.ta.linreg(close="Close", length=length, append=False)
                # standard deviation
                sd = df.ta.stdev(close="Close", length=length, append=False)

                df[f"TOS_STDEVALL_{length}_LR"] = lr

                for n in [1, 2, 3]:
                    df[f"TOS_STDEVALL_{length}_L_{n}"] = lr - n * sd
                    df[f"TOS_STDEVALL_{length}_U_{n}"] = lr + n * sd


        return df



    def list_technical_indicators(self, df: pd.DataFrame) -> list[str]:

        # ==================================================================
        # List all technical indicators in the dataset
        # ==================================================================

        cols = df.columns.tolist()

        # Find total number of columns (technical indicators) and how many were added by Pandas-TA
        n_total = len(cols)
        n_added = n_total - 9  # there were already 9 to start with

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

        # Visualise
        if self.verbose:
            print("✅ We now have", rows_after_cleaning, "rows and", df.shape[1], "columns (features = OHLCV + indicators + signal) in the dataset.", "\n")

        return df
    


    def prepare_dataset(self) -> pd.DataFrame:
    

        df = self.download_dataset()

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
        # Build MLP pipeline with feature selection and scaling
        # ===========================================================================

        pipeline = Pipeline([
            ("variance_filter", VarianceThreshold(threshold=0.0)),
            ("scaler", MinMaxScaler()),
            ("selector", SelectKBest(score_func=pearson_score, k=self.n_features)),
            ("mlp", MLPClassifier(random_state=self.random_state, max_iter=self.max_iter)),
        ])

        return pipeline



    def train_and_fit(self, X_train: pd.DataFrame, y_train: pd.Series) -> Pipeline:

        # ================================================================
        # Train the MLP with past data and fit the model
        # ================================================================

        # Create a fresh model pipeline for this date.
        model = self.build_pipeline()

        # Train the model using only the training window.
        model.fit(X_train, y_train)

        return model



    def predict(self, df: pd.DataFrame, show_progress: bool = True) -> pd.Series:
        
        '''
        Simulates the “real trading” process: on each date, train using only past data, 
        predict today's signal, then move forward one day.
        '''

        # ================================================================
        # Predict the signal for prediction_date
        # ================================================================

        df = df.copy()

        # get the indeces (dates) between the configured start and end prediction dates
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

            # n_features cannot exceed number of available columns
            if self.n_features > X_train.shape[1]:
                raise ValueError(
                    f"n_features ({self.n_features}) cannot be larger than "
                    f"the number of available features ({X_train.shape[1]})."
                )
            
            # Extract only the features from the row we want to predict.
            X_predict, _ = self.feature_target_split_dataset(df.loc[[prediction_date]])


            # Train and fit the MLP model using specific training dataset for this date.
            model = self.train_and_fit(X_train, y_train)

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


    def predict_mulitcore(self, df: pd.DataFrame, show_progress: bool = True) -> pd.Series:
        
        '''
        Simulates the “real trading” process: on each date, train using only past data, 
        predict today's signal, then move forward one day.
        '''

        # ================================================================
        # Predict the signal for prediction_date
        # ================================================================

        df = df.copy()

        # get the indeces (dates) between the configured start and end prediction dates
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

            # n_features cannot exceed number of available columns
            if self.n_features > X_train.shape[1]:
                raise ValueError(
                    f"n_features ({self.n_features}) cannot be larger than "
                    f"the number of available features ({X_train.shape[1]})."
                )
            
            # Extract only the features from the row we want to predict.
            X_predict, _ = self.feature_target_split_dataset(df.loc[[prediction_date]])


            # Train and fit the MLP model using specific training dataset for this date.
            model = self.train_and_fit(X_train, y_train)

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
        Simulates the “real trading” process: on each date, train using only past data, 
        predict today's signal, then move forward one day.
        '''

        # ================================================================
        # Predict the signal for prediction_date
        # ================================================================

        df = df.copy()

        # get the indeces (dates) between the configured start and end prediction dates
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

            # n_features cannot exceed number of available columns
            if self.n_features > X_train.shape[1]:
                raise ValueError(
                    f"n_features ({self.n_features}) cannot be larger than "
                    f"the number of available features ({X_train.shape[1]})."
                )
            
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



    def generate_signals(self, df: pd.DataFrame = None, show_progress: bool = True) -> pd.DataFrame:
        
        # If no pre-prepared dataset is provided, prepare it first (download, assign signal, compute indicators, clean).
        if df is None:
            df = self.prepare_dataset()
        
        # Predict signals for each day sequentially, retrinaing and refitting the model for each day.
        predictions = self.predict(df, show_progress=show_progress)

        # Convert predictions to target weights by multiplying by position size.
        weights = pd.DataFrame(index=predictions.index)
        weights[self.symbol] = predictions * self.position_size

        return weights






    """
    Below is an attempt to make two functions: one that predicts the next day, 
    and one that walks forward through the dataset to predict each day. 
    The former is useful for real-time prediction, while the latter is useful 
    for backtesting and evaluating the strategy on historical data.
    """

    def predict_new(self, df: pd.DataFrame, prediction_date: pd.Timestamp) -> int | float:
        
        '''
        Simulates the “real trading” process: on each date, train using only past data, 
        predict today's signal.
        '''

        # ================================================================
        # Predict the signal for prediction_date
        # ================================================================

        # df = self.prepare_dataset().copy()
        df = df.copy()

        # define beginning of training window
        train_start = prediction_date - pd.DateOffset(years=self.training_years)

        # Train MLP with past data up to yesterday (i-1), to then predict signal for today (i)
        # which will execute tomorrow at the open (with price Open_t+1).
        i = df.index.get_loc(prediction_date)
        train_end = df.index[i - 1]

        # Take only the training range of data
        train_df = df.loc[train_start:train_end]

        # Prepare training dataset, split into features and target
        X_train, y_train = self.feature_target_split_dataset(train_df)

        # Extract only the row we want to predict.
        X_predict = df.loc[[prediction_date]]

        # Train and fit the MLP model using specific training dataset for this date.
        model = self.train_and_fit(X_train, y_train)

        # Use the trained model to predict the signal for prediction_date.
        prediction = model.predict(X_predict)[0]

        return prediction



    def walk_forward_predictions(self, df: pd.DataFrame) -> pd.Series:


        # ================================================================
        # Predict the signal for prediction_date
        # ================================================================

        # df = self.prepare_dataset().copy()
        df = df.copy()

        # initialize empty Series with all date indices to store predictions
        predictions = pd.Series(index=df.index, dtype=float, name="prediction")

        # walk forward through each date, train on training_years past data, and predict the signal for that date
        for i in range( len(df) ): 

            # take the date at row i
            prediction_date = df.index[i]

            # define beginning of training window
            train_start = prediction_date - pd.DateOffset(years=self.training_years)

            # Skip this date if train_start is earlier than first date in dataset.
            if train_start < df.index[0]:
                continue
            
            # Use the trained model to predict the signal for prediction_date.
            prediction = self.predict_new(df, prediction_date)

            # Update the predictions Series with the predicted signal for this date.
            predictions.loc[prediction_date] = prediction

        return predictions.fillna(value=0)




    def generate_signals_new(self, df: pd.DataFrame | None = None) -> pd.DataFrame:
        
        # If no pre-prepared dataset is provided, prepare it first (download, assign signal, compute indicators, clean).
        if df is None:
            df = self.prepare_dataset()
        
        # Predict signals for each day sequentially, retrinaing and refitting the model for each day.
        predictions = self.walk_forward_predictions(df)

        # Convert predictions to target weights by multiplying by position size.
        weights = pd.DataFrame(index=predictions.index)
        weights[self.symbol] = predictions * self.position_size

        return weights