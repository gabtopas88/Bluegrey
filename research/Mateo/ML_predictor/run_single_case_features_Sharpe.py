'''
This file contains the function run_single_case, which runs a single backtest case for the ML predictor strategy.
It prepares the dataset, generates signals, runs the backtest, and produces a tearsheet with the results.
'''

'''
IMPORT COMMAND:
from run_single_case import run_single_case
'''

from ML_predictor_features_Sharpe import MLPredictorStrategy
# from src.backtest.vector_backtester import PortfolioVectorEngine
from src.backtest.vector_backtester__Loss_rate_and_bm_rates import PortfolioVectorEngine

def run_single_case_features_Sharpe(symbol,
                    asset_class = "STK",
                    start_date = "2025-01-01",
                    end_date = "2026-01-01",
                    signal_threshold = 0.002,
                    n_features = 6,
                    training_years = 10, 
                    rolling_sharpe_window = 63,
                    sharpe_enter_threshold = 1.05,
                    sharpe_exit_threshold = 0.95,
                    sharpe_filter_enabled = False,
                    rolling_sharpe_min_periods = 63,
                    rolling_sharpe_annualization = 252,
                    sharpe_performance_lag = 2,
                    feature_diagnostics_enabled = True,
                    feature_diagnostics_top_n = None,
                    position_size = 1.0,
                    initial_capital = 1_000_000, 
                    max_iter = 5000,
                    execution_delay = 1,
                    verbose = False,
                    show_progress = True,
                    return_params = False,
                    return_strategy = False,
                    ):
    
    params = {
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date,
        "signal_threshold": signal_threshold,
        "n_features": n_features,
        "training_years": training_years,
        "rolling_sharpe_window": rolling_sharpe_window,
        "sharpe_enter_threshold": sharpe_enter_threshold,
        "sharpe_exit_threshold": sharpe_exit_threshold,
        "sharpe_filter_enabled": sharpe_filter_enabled,
        "rolling_sharpe_min_periods": rolling_sharpe_min_periods,
        "rolling_sharpe_annualization": rolling_sharpe_annualization,
        "sharpe_performance_lag": sharpe_performance_lag,
        "feature_diagnostics_enabled": feature_diagnostics_enabled,
        "feature_diagnostics_top_n": feature_diagnostics_top_n,
        "position_size": position_size,
        "max_iter": max_iter,
        "verbose": verbose,
        "show_progress": show_progress,
    }

    # Check that the asset_class is valid for correct fee model structure:
    if asset_class not in {"STK", "CRYPTO", "FX"}:
        raise ValueError('asset_class must be "STK", "CRYPTO", or "FX" for correct fee model structure.')

    # Run the ML predictor strategy for the given parameters, prepare the dataset and generate the signals:
    strategy = MLPredictorStrategy(params=params)
    dataset = strategy.prepare_dataset()
    weights = strategy.generate_signals(dataset, show_progress=show_progress)

    # Stop if no weights were generated (i.e. the backtest would not run):
    if weights.empty:
        if return_params and return_strategy:
            return {"params": {**params}, "strategy": strategy}
        if return_params:
            return {**params}
        if return_strategy:
            return strategy
        return None

    # Prepare the prices DataFrame for the backtest, using the same index as the weights:
    prices = dataset[["Open"]].rename(columns={"Open": symbol}).loc[weights.index]

    # Create the PortfolioVectorEngine for the backtest, using the generated weights and prices:
    engine_run = PortfolioVectorEngine(
        prices = prices,
        signals = weights,
        asset_class = asset_class,
        initial_capital = initial_capital,
        execution_delay = execution_delay,
    )

    # Run the backtest
    engine_run.run()

    # Generate a tearsheet
    engine_run.tearsheet(f"{symbol} ML Walk Forward")


    # Return requested run objects for notebook inspection.
    if return_params and return_strategy:
        return {"params": {**params}, "strategy": strategy}
    if return_params:
        return {**params}
    if return_strategy:
        return strategy



'''      EXAMPLES       '''

if __name__ == "__main__":

    # Example for S&P 500:
    run_single_case_features_Sharpe(symbol="SPY", n_features=9, signal_threshold=0.002)

    # Example for Bitcoin:
    run_single_case_features_Sharpe(symbol="BTC-USD", asset_class="CRYPTO", n_features=5, training_years=3)
