"""
src/optimizer.py
Institutional Combinatorial Purged Cross-Validation (CPCV) Orchestrator.
Funnels parameterized strategies through alternative historical regimes to destroy overfitting.
"""
import itertools
import math
from typing import Optional, Dict

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from concurrent.futures import ProcessPoolExecutor, as_completed

# Anchor imports to your infrastructure
# NOTE: the vectorized research engine is PortfolioVectorEngine (multi-asset,
# DataFrame-in/DataFrame-out). The previous import name `VectorEngine` did not
# exist in src/vector_backtester.py, so this module failed at import.
from src.vector_backtester import PortfolioVectorEngine
from src.cpcv import PurgedKFold
from src.holdout import HoldoutVault


class CPCVOptimizer:

    # Hard ceiling on combinatorial paths. The auto-calibration below was
    # designed for 2-4 year minute-bar datasets; fed an ~18-year DAILY panel it
    # produces ~36 splits / 12 test blocks = C(36,12) ≈ 1.25 BILLION paths,
    # which would hang forever at list(cpcv.split(...)) without ever erroring.
    # The guard turns that silent hang into a loud, instructive failure. Long
    # panels must pass explicit n_splits / n_test_splits overrides instead
    # (e.g. the S3 TSMOM study runs 12/2 -> 66 paths, ~19-month blocks).
    MAX_COMBINATORIAL_PATHS = 500

    def __init__(self, df: pd.DataFrame, target_col: str, strategy_class, param_grid: dict,
                 asset_class: str, max_lookback_bars: int,
                 initial_capital: float = 100_000.0,
                 n_splits: Optional[int] = None,
                 n_test_splits: Optional[int] = None,
                 holdout: Optional[HoldoutVault] = None,
                 extra_data: Optional[Dict[str, pd.DataFrame]] = None,
                 slippage_bps: Optional[float] = None):
        """
        :param df: The historical CLOSE-price matrix (Index=Datetime, Columns=Tickers).
                   This is the same shape the strategies consume via
                   generate_signals({'close': df}) and the same shape
                   PortfolioVectorEngine prices/aligns against. For a single-asset
                   strategy this is simply a 1-column frame.
        :param target_col: Retained for API compatibility / single-asset callers.
                   Under the multi-asset contract the FULL close matrix is handed
                   to the strategy and the engine, so this is informational only.
        :param strategy_class: The uninstantiated class inheriting from BaseStrategy.
        :param param_grid: Dictionary of parameters to test { 'fast_window': [10, 20], 'slow_window': [50, 100] }
        :param asset_class: 'STK', 'FX', or 'CRYPTO' (Required for exact fee modeling).
        :param max_lookback_bars: The maximum memory of your strategy (e.g., 200). Used to purge data leakage.
                   NOTE for ML strategies: this must cover feature memory PLUS the
                   label horizon (e.g. 252 + 21 = 273 for the S3 study), because a
                   training label whose window overlaps the test block is leakage
                   even when every feature is causal.
        :param initial_capital: Capital base handed to PortfolioVectorEngine.
                   NOT cosmetic: IBKR's per-share commission minimums make modeled
                   cost drag in bps a function of book size — a $100K backtest
                   overstates commissions by up to ~10x relative to a $1M book.
                   This must match the deployment mandate, or the optimizer
                   selects parameters for a cost world that will never exist.
        :param n_splits / n_test_splits: Explicit CPCV geometry override. Pass
                   BOTH or NEITHER. When omitted, the original auto-calibration
                   (≈180-day blocks) applies — correct for the minute-bar
                   datasets this optimizer was born on, catastrophic for long
                   daily panels (see MAX_COMBINATORIAL_PATHS).
        :param holdout: Optional HoldoutVault. When provided, the terminal
                   holdout fence is applied to df (and extra_data) BEFORE
                   calibration, so the splits, the purge geometry and
                   bars_per_year are all computed on fenced data only. The
                   optimizer can never see, slice, or score the holdout window.
        :param extra_data: Optional additional matrices handed to the strategy
                   alongside 'close' — e.g. {'eligible': bool_matrix} for a
                   liquidity screen, or precomputed feature matrices for ML
                   strategies. Each is fenced by the holdout exactly like df.
                   Keeps expensive features computed ONCE outside the grid loop.
        :param slippage_bps: Optional override of the engine's flat slippage
                   assumption (engine default is 1.0 bps). Used by the cost
                   sensitivity battery: same grid, same paths, different friction.
        """
        self.df = df.copy()
        if not isinstance(self.df.index, pd.DatetimeIndex):
            raise ValueError("Dataframe MUST have a DatetimeIndex for CPCV boundary calculations.")

        # --- TERMINAL HOLDOUT FENCE ---
        # Applied first, unconditionally, before any geometry is derived from
        # the data. If the fence changes the dataset, every downstream number
        # (splits, paths, annualization) must reflect the fenced view.
        self.holdout = holdout
        self.extra_data = {k: v.copy() for k, v in (extra_data or {}).items()}
        if self.holdout is not None:
            self.df = self.holdout.enforce(self.df)
            self.extra_data = {k: self.holdout.enforce(v) for k, v in self.extra_data.items()}

        self.target_col = target_col
        self.strategy_class = strategy_class
        self.param_grid = param_grid
        self.asset_class = asset_class
        self.max_lookback = max_lookback_bars
        self.initial_capital = float(initial_capital)
        self.slippage_bps = slippage_bps
        self._n_splits_override = n_splits
        self._n_test_splits_override = n_test_splits
        self.results = []
        # Per-parameter-set path Sharpe arrays, keyed by repr(params). Retained
        # so later studies can run PAIRED per-path comparisons (e.g. ML vs
        # baseline on identical paths) instead of comparing two marginal P5s.
        self.path_detail: Dict[str, np.ndarray] = {}

        # 1. Dynamically Calibrate CPCV Boundaries
        self._calibrate_cpcv()

    def _calibrate_cpcv(self):
        """
        Reads the dataset dimensions and calculates mathematically safe boundaries.
        Prevents running valid math on invalid data sizes.
        """
        total_days = (self.df.index[-1] - self.df.index[0]).days

        # CONSTRAINT 1: Absolute Minimum Data Depth
        if total_days < 730:  # 2 Years
            raise ValueError(f"❌ Dataset too short ({total_days} days). CPCV requires at least 2 years (730 days) to prevent regime overfitting.")

        # CONSTRAINT 2: Split geometry.
        # Explicit override path first — overrides are validated, never silently
        # corrected. Rationale: the researcher who passes 12/2 has read the study
        # charter and knows the block length they want; the researcher who passes
        # 3/5 has made an error that must fail loudly, not be "fixed".
        if self._n_splits_override is not None or self._n_test_splits_override is not None:
            if self._n_splits_override is None or self._n_test_splits_override is None:
                raise ValueError(
                    "❌ Pass BOTH n_splits and n_test_splits, or neither. "
                    "Half-specified geometry is ambiguous."
                )
            self.n_splits = int(self._n_splits_override)
            self.n_test_splits = int(self._n_test_splits_override)

            if self.n_splits < 4:
                raise ValueError(f"❌ n_splits must be >= 4 (got {self.n_splits}). Fewer blocks cannot span multiple regimes.")
            if not (1 <= self.n_test_splits < self.n_splits):
                raise ValueError(
                    f"❌ n_test_splits must satisfy 1 <= n_test_splits < n_splits "
                    f"(got {self.n_test_splits} vs {self.n_splits} splits)."
                )

            block_days = total_days / self.n_splits
            if block_days < 90:
                raise ValueError(
                    f"❌ Override yields {block_days:.0f}-day blocks. Blocks under "
                    f"~90 days cannot contain a macro regime; reduce n_splits."
                )
        else:
            # Default auto-calibration (unchanged): Minimum 6-Months per Block.
            # We want blocks of roughly 180 days to ensure macro-regime exposure.
            self.n_splits = max(4, int(total_days / 180))
            self.n_test_splits = max(2, int(self.n_splits / 3))

        # PATH-COUNT GUARD — must run BEFORE list(cpcv.split(...)) below, because
        # enumerating a billion-path generator hangs rather than errors.
        n_paths = math.comb(self.n_splits, self.n_test_splits)
        if n_paths > self.MAX_COMBINATORIAL_PATHS:
            raise ValueError(
                f"❌ CPCV geometry ({self.n_splits} splits, {self.n_test_splits} test blocks) "
                f"yields {n_paths:,} combinatorial paths (limit {self.MAX_COMBINATORIAL_PATHS}). "
                f"This is almost always the auto-calibration meeting a long daily panel. "
                f"Pass explicit n_splits/n_test_splits — e.g. n_splits=12, n_test_splits=2 "
                f"for ~19 years of daily bars (66 paths, ~19-month blocks)."
            )

        # Calculate bars per year dynamically for Sharpe annualization
        self.bars_per_year = len(self.df) / (total_days / 365.25)

        # Initialize the Slicer
        self.cpcv = PurgedKFold(
            n_splits=self.n_splits,
            n_test_splits=self.n_test_splits,
            purge_window=self.max_lookback,
            embargo_window=int(self.max_lookback * 0.1)  # 10% embargo buffer
        )
        self.paths = list(self.cpcv.split(self.df))

        print(f"📐 CPCV Calibrated: {total_days} days of data.")
        print(f"   -> Splits: {self.n_splits} | Test Blocks: {self.n_test_splits}")
        print(f"   -> Combinatorial Paths: {len(self.paths)}")
        print(f"   -> Purge Window: {self.max_lookback} bars")
        print(f"   -> Capital Base: ${self.initial_capital:,.0f}")
        if self.slippage_bps is not None:
            print(f"   -> Slippage override: {self.slippage_bps} bps")
        if self.extra_data:
            print(f"   -> Extra data matrices: {list(self.extra_data.keys())}")
        if self.holdout is not None:
            print(f"   -> 🔒 Holdout '{self.holdout.study_id}' ENFORCED (cutoff {self.holdout.cutoff})")
        else:
            print(f"   -> ⚠️ NO holdout vault attached. Acceptable only for throwaway exploration.")

    def _evaluate_params(self, params: dict):
        """Evaluates a single parameter combination across ALL combinatorial paths."""
        try:
            # 1. Instantiate the Strategy Contract
            strategy = self.strategy_class(instruments={}, params=params)

            # 2. Generate signals across the entire continuous dataframe
            # (Rolling metrics do not leak future data, so this is safe and extremely fast)
            #
            # ⚠️ ML CONTRACT: the statement above holds ONLY for causal rolling
            # transforms. A strategy containing a FITTED model must implement
            # internal walk-forward fitting inside generate_signals() — the model
            # scoring bar t trained only on data through t - purge_gap. A model
            # fit once on the full frame and then "evaluated" on the CPCV test
            # slices below is leakage, and the P5_Sharpe it produces is fiction.
            #
            # BaseStrategy.generate_signals expects a dict of matrices keyed by
            # field; wrap the close matrix (plus any extra matrices) accordingly.
            # Returns a target-weights DataFrame whose columns match the price matrix.
            target_weights = strategy.generate_signals({'close': self.df, **self.extra_data})

            # 3. Execute Vector Engine to get exact Net Returns (Including Tiered IBKR Fees)
            # PortfolioVectorEngine is multi-asset: prices and signals are both
            # DataFrames with matching ticker columns. Feed the full close matrix
            # as prices so per-asset returns/turnover align with the weights.
            #
            # Capital is pinned to the study mandate, not a literal: per-share
            # commission minimums make cost drag size-dependent, so optimizing
            # at the wrong capital optimizes against the wrong cost surface.
            engine = PortfolioVectorEngine(
                prices=self.df,
                signals=target_weights,
                asset_class=self.asset_class,
                initial_capital=self.initial_capital,
                execution_delay=1
            )
            if self.slippage_bps is not None:
                # The engine constructs its fee model with the 1 bps default;
                # override in place so the sensitivity battery needs no engine change.
                engine.fee_model.slippage_bps = self.slippage_bps / 10000.0
            res_df = engine.run()
            net_returns = res_df['net_returns']

            # 4. Slicer Execution: Extract Out-Of-Sample (Test) returns for each path
            path_sharpes = []

            for train_idx, test_idx in self.paths:
                oos_returns = net_returns.iloc[test_idx]

                if oos_returns.std() == 0:
                    path_sharpes.append(0.0)
                else:
                    # Annualize Sharpe based on exact bar resolution
                    sharpe = (oos_returns.mean() / oos_returns.std()) * np.sqrt(self.bars_per_year)
                    path_sharpes.append(sharpe)

            path_sharpes = np.array(path_sharpes)

            # 5. Extract Distribution Metrics
            return {
                **params,
                'Mean_Sharpe': np.mean(path_sharpes),
                'Median_Sharpe': np.median(path_sharpes),
                'P5_Sharpe': np.percentile(path_sharpes, 5),  # The Conservative Robustness Metric
                'Max_Sharpe': np.max(path_sharpes),
                'Win_Rate_Paths': (np.sum(path_sharpes > 0) / len(self.paths)) * 100,
                '_path_sharpes': path_sharpes,   # popped into path_detail by run()
            }

        except Exception as e:
            print(f"⚠️ Error testing {params}: {e}")
            return None

    def run(self, parallel=True):
        """
        Ignites the Grid Search.
        Fans out hundreds of thousands of path backtests across all CPU cores.
        """
        keys = self.param_grid.keys()
        combinations = [dict(zip(keys, prod)) for prod in itertools.product(*self.param_grid.values())]
        total_simulations = len(combinations) * len(self.paths)

        print(f"🚀 Igniting CPU Cluster: Testing {len(combinations)} Parameter Sets.")
        print(f"   -> Total Path Simulations: {total_simulations}")

        self.results = []
        self.path_detail = {}

        def _absorb(res):
            if not res:
                return
            path_sharpes = res.pop('_path_sharpes', None)
            key = repr({k: res[k] for k in keys})
            if path_sharpes is not None:
                self.path_detail[key] = path_sharpes
            self.results.append(res)

        if parallel:
            with ProcessPoolExecutor() as executor:
                futures = {executor.submit(self._evaluate_params, p): p for p in combinations}
                for i, future in enumerate(as_completed(futures)):
                    _absorb(future.result())
                    if (i + 1) % 50 == 0:
                        print(f"   [{i + 1}/{len(combinations)}] parameter grids processed...")
        else:
            for i, p in enumerate(combinations):
                _absorb(self._evaluate_params(p))
                if (i + 1) % 50 == 0:
                    print(f"   [{i + 1}/{len(combinations)}] parameter grids processed...")

        print("✅ Optimization Complete.")

        # Return sorted by the 5th Percentile Sharpe (The Institutional Standard)
        results_df = pd.DataFrame(self.results)
        return results_df.sort_values(by='P5_Sharpe', ascending=False).reset_index(drop=True)

    def plot_heatmap(self, x_param: str, y_param: str, metric: str = 'P5_Sharpe'):
        """Visualizes the parameter landscape to find robust, flat zones."""
        if not self.results:
            print("⚠️ No results to plot. Run optimize() first.")
            return

        df = pd.DataFrame(self.results)

        if x_param not in df.columns or y_param not in df.columns:
            print(f"⚠️ Parameters {x_param} or {y_param} not found in results.")
            return

        pivot_table = df.pivot(index=y_param, columns=x_param, values=metric)

        plt.figure(figsize=(10, 8))
        # We use a distinct colormap to visualize edge boundaries
        sns.heatmap(pivot_table, annot=True, cmap="YlGnBu", fmt=".2f")
        plt.title(f"Alpha Landscape: {metric} Distribution\n({y_param} vs {x_param})")
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plt.show()