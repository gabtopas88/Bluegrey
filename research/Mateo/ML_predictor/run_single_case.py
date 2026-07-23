'''
This file contains the function run_single_case, which runs a single backtest case for the ML predictor strategy.
It prepares the dataset, generates signals, runs the backtest, and produces a tearsheet with the results.
'''

'''
IMPORT COMMAND:
from run_single_case import run_single_case
'''

from ML_predictor import MLPredictorStrategy
# from src.vector_backtester import PortfolioVectorEngine
from src.vector_backtester_Mateo import PortfolioVectorEngine

def run_single_case(symbol,
                    asset_class = "STK",
                    start_date = "2025-01-01",
                    end_date = "2026-01-01",
                    signal_threshold = 0.002,
                    n_features = 6,
                    training_years = 10, 
                    position_size = 1.0,
                    initial_capital = 1_000_000, 
                    max_iter = 5000,
                    execution_delay = 1,
                    verbose = False,
                    show_progress = True,
                    return_params = False
                    ):
    
    params = {
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date,
        "signal_threshold": signal_threshold,
        "n_features": n_features,
        "training_years": training_years,
        "position_size": position_size,
        "max_iter": max_iter,
        "verbose": verbose,
        "show_progress": show_progress
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


    # Return the parameters if requested
    if return_params:
        return {**params}



'''      EXAMPLES       '''

if __name__ == "__main__":

    # Example for S&P 500:
    run_single_case(symbol="SPY", n_features=9, signal_threshold=0.002)

    # Example for Bitcoin:
    run_single_case(symbol="BTC-USD", asset_class="CRYPTO", n_features=5, training_years=3)