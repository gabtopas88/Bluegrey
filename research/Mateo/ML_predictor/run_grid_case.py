'''
Function for grid search of parameters for the ML predictor strategy.
It runs a backtest for each combination of parameters and returns the results in a DataFrame.
It is parallelisable - run on multiple cores using joblib.Parallel.
'''

'''
IMPORT COMMAND:
from run_grid_case import parameter_grid_search
'''


from joblib import Parallel, delayed
from pandas import DataFrame
import matplotlib.pyplot as plt
import seaborn as sns
from IPython.display import display

from ML_predictor import MLPredictorStrategy
# from src.backtest.vector_backtester import PortfolioVectorEngine
from src.backtest.vector_backtester__Loss_rate_and_bm_rates import PortfolioVectorEngine



def run_grid_case(symbol,
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
                  show_progress = False
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
    engine = PortfolioVectorEngine(
        prices = prices,
        signals = weights,
        asset_class = asset_class,
        initial_capital = initial_capital,
        execution_delay = execution_delay,
    )

    # Run the backtest
    stats = engine.run()

    
    return {
        **params,
        "net_return_pct": (stats["net_equity"].iloc[-1] / initial_capital - 1) * 100,
        "gross_return_pct": (stats["gross_equity"].iloc[-1] / initial_capital - 1) * 100,
        "final_net_equity": stats["net_equity"].iloc[-1],
        "final_gross_equity": stats["gross_equity"].iloc[-1],
    }




def parameter_grid_search(symbol, var_1 : str, list_1 : list, var_2 : str, list_2 : list, n_jobs = -1, verbose = 10, **extra_kwargs):

    # Check for any grid arguments duplicated in the fixed keyword arguments:
    duplicated_grid_args = {var_1, var_2}.intersection(extra_kwargs)
    if duplicated_grid_args:
        raise ValueError(
            "Do not pass grid-search variables as fixed keyword arguments: "
            f"{sorted(duplicated_grid_args)}"
        )

    # Check symbol is only passed through the symbol argument:
    if "symbol" in extra_kwargs:
        raise ValueError("Pass symbol only through the symbol argument.")

    # Create the grid: all combinations of the two parameters:
    cases = [
        (value_1, value_2)
        for value_1 in list_1
        for value_2 in list_2
    ]

    # Run the grid search in parallel using joblib.Parallel for 'n_jobs' cores:
    grid_records = Parallel(n_jobs=n_jobs, backend="loky", verbose=verbose)(
        delayed(run_grid_case)(
            symbol=symbol,
            **extra_kwargs,
            **{var_1: value_1, var_2: value_2},
        )
        for value_1, value_2 in cases
    )

    # Filter out any None results (cases where the backtest did not produce any weights):
    grid_records = [record for record in grid_records if record is not None]

    # Create a DataFrame from the results and pivot it for heatmap visualisation:
    grid_df = DataFrame(grid_records)
    grid_df = grid_df.sort_values([var_1, var_2]).reset_index(drop=True)
    grid_heatmap_data = grid_df.pivot(index=var_1, columns=var_2, values="net_return_pct")

    # Display the results in a table:
    display(grid_df)

    # Plot the results as a heatmap:
    plt.figure(figsize=(1.4*len(grid_heatmap_data.columns), 1.3*len(grid_heatmap_data.index)))
    sns.heatmap(grid_heatmap_data, annot=True, fmt=".1f", cmap="RdYlGn", center=0)
    plt.title(f"ML Predictor {symbol} Grid Search -- Net Return %")
    plt.xlabel(var_2)
    plt.ylabel(var_1)
    plt.tight_layout()
    plt.show()

    return grid_df, grid_heatmap_data




'''      EXAMPLE       '''

if __name__ == "__main__":
    
    # following example takes about 50 seconds to run on a 4-core parallel setup:
    parameter_grid_search(symbol="ECH", 
                          var_1="n_features", list_1=[7, 8], 
                          var_2="signal_threshold", list_2=[0.001, 0.003],
                          start_date="2018-01-01",
                          end_date="2018-01-10",
                          )
