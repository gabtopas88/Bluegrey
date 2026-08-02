"""
research/parity_harness.py
Backtest-Live Parity Harness (Workstream B, Tier 1-3).

Answers the question "are the event backtests trustworthy?" — but only by first
splitting it into two questions that need completely different instruments:

    Q1  Is the ENGINE faithful?
        Given identical inputs, does event_backtester.py reproduce the live
        engine's decisions bar-for-bar? DETERMINISTIC. Exact answer. Hard gate.
        -> Tier 1

    Q2  Are the INPUTS and COST MODEL representative?
        Does Polygon data plus fees.py describe what IBKR execution actually
        costs? STATISTICAL. No exact answer, only a distribution and a bias.
        -> Tiers 2 and 3

Conflating these is the classic harness mistake. Polygon and IBKR IDEALPRO are
different vendors constructing bars from different venue sets, so they will never
match bar-for-bar; a blended "backtest vs live divergence" number would therefore
be dominated by vendor noise and answer NEITHER question.

Tier 1 sidesteps that entirely by replaying the live tape itself
(TelemetryReplaySource), driving data divergence to zero BY CONSTRUCTION. Any
residual difference is engine logic. Tiers 2 and 3 then measure the two things
that genuinely cannot be made exact.

    Tier 1  Replay Parity      GATE  signal_type / current_pos / z / beta
    Tier 2  Cost Realism       INFO  realized vs modelled slippage & commission
    Tier 3  Vendor Divergence  INFO  Polygon vs live-recorded IBKR closes

Only Tier 1 can fail. Tiers 2 and 3 are measurements: a non-zero result is the
finding, not an error.

Usage
-----
    from research.parity_harness import ParityHarness
    from strategies.kalman_pairs import KalmanPairsStrategy
    from src import config

    harness = ParityHarness(
        strategy_class=KalmanPairsStrategy,
        instruments=config.INSTRUMENTS,
        params=config.STRATEGY_PARAMS,
        live_run_id='<run_id from a live session manifest>',
        data_library=config.LIBS['fx_min'],
    )
    report = harness.run_all()
    harness.print_report(report)

Or from the command line:

    python -m research.parity_harness --run-id <live_run_id>
"""
import argparse
import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from src import config
from src.bar_source import ArcticBarSource, TelemetryReplaySource, POLICY_REJECT
from src.event_backtester import BacktestEngine
from src.store import DataStore
from src.telemetry import read_telemetry_range

logger = logging.getLogger("ParityHarness")

# --- VERDICTS ---
PASS = 'PASS'
FAIL = 'FAIL'
INFO = 'INFO'
SKIPPED = 'SKIPPED'

# Float tolerance for z and beta. These are recomputed by the same code on the
# same inputs, so the only legitimate difference is floating-point association
# order. Anything larger is a real divergence, not numerical noise.
DEFAULT_FLOAT_TOLERANCE = 1e-9

# Signal types emitted before the strategy has usable state. Used to locate the
# warm-up boundary in the backtest stream.
WARMUP_SIGNAL_TYPES = ('WARMUP_BETA', 'WARMUP_ZSCORE', 'AWAITING_DATA')

# Reported thresholds for Tier 3. Not gates — they turn a distribution into a
# statement a human can act on ("X% of bars disagree by more than 1 bp").
VENDOR_DIVERGENCE_BUCKETS_BPS = (0.5, 1.0, 2.0, 5.0)


@dataclass
class TierResult:
    """Outcome of a single tier."""
    tier: str
    name: str
    status: str                                   # PASS / FAIL / INFO / SKIPPED
    headline: str = ''
    metrics: Dict[str, Any] = field(default_factory=dict)
    findings: List[str] = field(default_factory=list)
    # Small tables (e.g. first N mismatching rows) kept for the JSON report so a
    # failure is diagnosable without re-running the harness.
    samples: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ParityReport:
    """Full harness output. verdict is driven by GATE tiers only."""
    live_run_id: str
    backtest_run_id: Optional[str] = None
    verdict: str = INFO
    window_start: Optional[str] = None
    window_end: Optional[str] = None
    tiers: List[TierResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            'live_run_id': self.live_run_id,
            'backtest_run_id': self.backtest_run_id,
            'verdict': self.verdict,
            'window_start': self.window_start,
            'window_end': self.window_end,
            'tiers': [asdict(t) for t in self.tiers],
        }


def _to_utc(series_or_index):
    """Normalises to tz-aware UTC. ArcticDB indices are typically naive UTC while
    telemetry timestamps are explicitly tz-aware; joining them without this
    produces an empty result that looks like total divergence."""
    idx = pd.DatetimeIndex(series_or_index)
    return idx.tz_localize('UTC') if idx.tz is None else idx.tz_convert('UTC')


def _describe(values: np.ndarray) -> Dict[str, Optional[float]]:
    """Distribution summary. Returns Nones rather than NaNs so the JSON report
    stays valid and unambiguous."""
    clean = np.asarray([v for v in values if v is not None and not pd.isna(v)], dtype=float)
    if clean.size == 0:
        return {'n': 0, 'mean': None, 'median': None, 'std': None,
                'p05': None, 'p95': None, 'min': None, 'max': None}
    return {
        'n': int(clean.size),
        'mean': float(np.mean(clean)),
        'median': float(np.median(clean)),
        'std': float(np.std(clean, ddof=1)) if clean.size > 1 else 0.0,
        'p05': float(np.percentile(clean, 5)),
        'p95': float(np.percentile(clean, 95)),
        'min': float(np.min(clean)),
        'max': float(np.max(clean)),
    }


class ParityHarness:
    """
    Reconciles a live session against a backtest of the same window.

    Constructing the harness performs no work; each tier is run explicitly so a
    failing Tier 1 can be investigated without paying for Tiers 2 and 3.
    """

    def __init__(self,
                 strategy_class,
                 instruments: dict,
                 params: dict,
                 live_run_id: str,
                 live_telemetry_path=None,
                 data_library: Optional[str] = None,
                 risk_mode: Optional[str] = None,
                 fee_params: Optional[dict] = None,
                 backtest_base_path=None,
                 output_dir=None,
                 warmup_exclusion='auto',
                 float_tolerance: float = DEFAULT_FLOAT_TOLERANCE,
                 execution_delay_bars: int = 0):
        """
        :param live_run_id: the live session to reconcile against. Found in
            data/telemetry/sessions/*.json.
        :param warmup_exclusion: 'auto' (default) locates the warm-up boundary
            from the backtest's own signal stream; an int excludes exactly that
            many leading bars.

            Exclusion is REQUIRED, not cosmetic. The live engine primes Kalman
            state from IBKR historical bars at boot (prime_state), while the
            backtester cold-starts and warms over z_lookback bars. Those two
            states are legitimately different and comparing them would report a
            guaranteed failure that says nothing about engine fidelity. We
            exclude rather than prime the replay, because priming it from IBKR
            history would reintroduce exactly the vendor contamination the
            replay design exists to eliminate.
        :param execution_delay_bars: defaults to 0 to match the live engine,
            which places a market order immediately after the bar closes. This
            does not affect Tier 1 (strategy state is driven by the
            pending-transition protocol, not by fills) but it matters for the
            fill-price comparison in Tier 2.
        """
        self.strategy_class = strategy_class
        self.instruments = instruments
        self.params = params
        self.live_run_id = live_run_id
        self.live_telemetry_path = Path(live_telemetry_path) if live_telemetry_path is not None \
            else Path(getattr(config, 'TELEMETRY_PATH', config.DATA_DIR / 'telemetry'))
        self.data_library = data_library
        self.risk_mode = risk_mode
        self.fee_params = fee_params
        self.backtest_base_path = backtest_base_path
        self.warmup_exclusion = warmup_exclusion
        self.float_tolerance = float_tolerance
        self.execution_delay_bars = execution_delay_bars

        self.output_dir = Path(output_dir) if output_dir is not None else \
            Path(getattr(config, 'ROOT_DIR', Path('.'))) / 'research' / 'parity' / live_run_id

        # Populated by run_tier1 and reused by later tiers.
        self.engine: Optional[BacktestEngine] = None
        self.backtest_run_id: Optional[str] = None
        self._live_decisions: Optional[pd.DataFrame] = None
        self._window = (None, None)

    # ==========================================
    # 📥 SHARED LOADING
    # ==========================================
    def _load_live_decisions(self) -> pd.DataFrame:
        """Loads and caches the live decisions stream for this run."""
        if self._live_decisions is not None:
            return self._live_decisions

        # Unbounded range: the run's own rows are selected by run_id, and we do
        # not know the window until we have read them.
        df = read_telemetry_range(
            base_path=self.live_telemetry_path,
            stream='decisions',
            start=pd.Timestamp('1970-01-01', tz='UTC'),
            end=pd.Timestamp('2100-01-01', tz='UTC'),
            run_id=self.live_run_id,
        )

        if not df.empty:
            df = df.sort_values('timestamp_utc')
            # A live session that restarted can emit two rows for one minute.
            df = df.drop_duplicates(subset='timestamp_utc', keep='last')
            self._window = (df['timestamp_utc'].iloc[0], df['timestamp_utc'].iloc[-1])

        self._live_decisions = df
        return df

    @staticmethod
    def _expand_meta(df: pd.DataFrame, prefix: str = '') -> pd.DataFrame:
        """
        Parses the JSON `meta` column into typed columns.

        KalmanPairsStrategy._build_meta guarantees these keys on EVERY return
        path — including warmup and invalid-price bars — which is what makes a
        uniform row-by-row comparison possible at all.
        """
        # NOTE: 'current_pos' is deliberately absent. It is already a
        # first-class column in the decisions schema, and expanding it from meta
        # too would create duplicate column names that collide under the merge
        # suffixes — turning a Series comparison into a DataFrame comparison.
        # The schema column is the authoritative one; both engines populate it
        # from meta.get('current_pos', 0) anyway.
        keys = ['z', 'beta', 'P', 'errors_buffer',
                'halted', 'pending_current_pos']
        parsed = []
        for raw in df['meta']:
            try:
                m = json.loads(raw) if raw else {}
            except (TypeError, ValueError):
                m = {}
            parsed.append({f'{prefix}{k}': m.get(k) for k in keys})
        return pd.DataFrame(parsed, index=df.index)

    # ==========================================
    # 🎯 TIER 1 — REPLAY PARITY (HARD GATE)
    # ==========================================
    def run_tier1(self) -> TierResult:
        """
        Replays the live tape through the event backtester and compares
        decisions bar-for-bar.

        This is the trustworthiness verdict. Because the backtester consumes the
        exact bar series the live strategy saw, there is no vendor difference to
        explain away: a mismatch here is an engine bug.
        """
        result = TierResult(tier='Tier 1', name='Replay Parity', status=SKIPPED)

        live = self._load_live_decisions()
        if live.empty:
            result.headline = f"No live decisions found for run_id={self.live_run_id}."
            result.findings.append(
                "Check the run_id against data/telemetry/sessions/*.json.")
            return result

        start, end = self._window
        result.metrics['live_bars'] = int(len(live))
        result.metrics['window_start'] = str(start)
        result.metrics['window_end'] = str(end)

        # --- Replay the tape ---
        source = TelemetryReplaySource(
            telemetry_base_path=self.live_telemetry_path,
            run_id=self.live_run_id,
            instrument_keys=list(self.instruments.keys()),
        )
        self.engine = BacktestEngine(
            strategy_class=self.strategy_class,
            instruments=self.instruments,
            params=self.params,
            bar_source=source,
            risk_mode=self.risk_mode,
            fee_params=self.fee_params,
            missing_bar_policy=POLICY_REJECT,
            execution_delay_bars=self.execution_delay_bars,
            emit_telemetry=True,
            telemetry_base_path=self.backtest_base_path,
        )
        self.engine.run(start, end)
        self.backtest_run_id = self.engine.run_id

        bt = read_telemetry_range(
            base_path=self.engine.telemetry.base_path,
            stream='decisions',
            start=start - timedelta(days=1),
            end=end + timedelta(days=1),
            run_id=self.backtest_run_id,
        )
        result.metrics['backtest_bars'] = int(len(bt))

        if bt.empty:
            result.status = FAIL
            result.headline = "Backtest produced no decisions from the replay tape."
            return result

        bt = bt.sort_values('timestamp_utc').drop_duplicates(subset='timestamp_utc', keep='last')

        # --- Join ---
        live_x = pd.concat([live.reset_index(drop=True),
                            self._expand_meta(live.reset_index(drop=True))], axis=1)
        bt_x = pd.concat([bt.reset_index(drop=True),
                          self._expand_meta(bt.reset_index(drop=True))], axis=1)

        joined = live_x.merge(bt_x, on='timestamp_utc', suffixes=('_live', '_bt'))
        result.metrics['joined_bars'] = int(len(joined))

        unmatched_live = len(live) - len(joined)
        unmatched_bt = len(bt) - len(joined)
        if unmatched_live or unmatched_bt:
            # Should be zero: the replay tape IS the live timeline. A non-zero
            # count means bars were dropped or duplicated somewhere in the path.
            result.findings.append(
                f"Timestamp coverage imperfect: {unmatched_live} live bar(s) and "
                f"{unmatched_bt} backtest bar(s) failed to join. Expected 0 for a replay."
            )

        if joined.empty:
            result.status = FAIL
            result.headline = "Replay produced no overlapping timestamps with the live run."
            return result

        # --- Warm-up exclusion ---
        n_excluded = self._resolve_warmup_exclusion(joined)
        compared = joined.iloc[n_excluded:].copy()
        result.metrics['warmup_bars_excluded'] = int(n_excluded)
        result.metrics['compared_bars'] = int(len(compared))

        if compared.empty:
            result.status = SKIPPED
            result.headline = (
                f"Every joined bar fell inside the warm-up window "
                f"({n_excluded} bars). Nothing left to compare — the live run is "
                f"shorter than the strategy's warm-up requirement."
            )
            return result

        # --- Compare ---
        sig_match = (compared['signal_type_live'] == compared['signal_type_bt'])
        sig_rate = float(sig_match.mean())
        pos_match = (compared['current_pos_live'].astype('Int64') ==
                     compared['current_pos_bt'].astype('Int64'))
        pos_rate = float(pos_match.mean())

        z_diff = self._abs_diff(compared['z_live'], compared['z_bt'])
        beta_diff = self._abs_diff(compared['beta_live'], compared['beta_bt'])

        result.metrics['signal_type_match_rate'] = sig_rate
        result.metrics['current_pos_match_rate'] = pos_rate
        result.metrics['z_max_abs_diff'] = None if z_diff.size == 0 else float(np.max(z_diff))
        result.metrics['beta_max_abs_diff'] = None if beta_diff.size == 0 else float(np.max(beta_diff))
        result.metrics['z_compared'] = int(z_diff.size)
        result.metrics['beta_compared'] = int(beta_diff.size)
        result.metrics['float_tolerance'] = self.float_tolerance

        # --- Gap asymmetry finding (surfaced, never silently reconciled) ---
        self._add_gap_findings(result, compared)

        # --- Verdict ---
        z_ok = z_diff.size == 0 or float(np.max(z_diff)) <= self.float_tolerance
        beta_ok = beta_diff.size == 0 or float(np.max(beta_diff)) <= self.float_tolerance
        passed = (sig_rate == 1.0) and (pos_rate == 1.0) and z_ok and beta_ok

        result.status = PASS if passed else FAIL
        if passed:
            result.headline = (
                f"Engine is faithful: {len(compared)} bars compared, "
                f"100% signal and position match, z/beta within {self.float_tolerance:g}."
            )
        else:
            result.headline = (
                f"Divergence detected over {len(compared)} bars: "
                f"signal {sig_rate:.4%}, position {pos_rate:.4%}, "
                f"z max diff {result.metrics['z_max_abs_diff']}, "
                f"beta max diff {result.metrics['beta_max_abs_diff']}."
            )
            mismatches = compared[~sig_match | ~pos_match]
            for _, row in mismatches.head(20).iterrows():
                result.samples.append({
                    'timestamp_utc': str(row['timestamp_utc']),
                    'signal_live': row['signal_type_live'],
                    'signal_bt': row['signal_type_bt'],
                    'pos_live': row['current_pos_live'],
                    'pos_bt': row['current_pos_bt'],
                    'z_live': row['z_live'],
                    'z_bt': row['z_bt'],
                })
            if len(mismatches) > 20:
                result.findings.append(
                    f"{len(mismatches)} mismatching bars total; first 20 captured.")

        return result

    def _resolve_warmup_exclusion(self, joined: pd.DataFrame) -> int:
        """
        Locates the warm-up boundary.

        'auto' finds the last bar the BACKTEST spent in a warm-up state and
        excludes everything up to and including it. Derived from the data rather
        than from z_lookback because the two can legitimately differ — a replay
        containing incomplete cross-sections warms more slowly than the
        parameter alone implies.
        """
        if self.warmup_exclusion != 'auto':
            return max(0, int(self.warmup_exclusion))

        is_warmup = joined['signal_type_bt'].isin(WARMUP_SIGNAL_TYPES)
        if not is_warmup.any():
            return 0
        return int(np.max(np.where(is_warmup.values)[0])) + 1

    @staticmethod
    def _abs_diff(a: pd.Series, b: pd.Series) -> np.ndarray:
        """Absolute differences over rows where BOTH sides carry a value.

        Rows where one side is null and the other is not are NOT silently
        skipped — they are caught by the signal_type comparison, since a null z
        only occurs on warmup / invalid / low-volatility bars which carry their
        own distinct signal_type."""
        mask = a.notna() & b.notna()
        if not mask.any():
            return np.array([])
        return np.abs(a[mask].astype(float).values - b[mask].astype(float).values)

    def _add_gap_findings(self, result: TierResult, compared: pd.DataFrame):
        """
        Reports gap-detection asymmetry rather than reconciling it.

        The live engine detects gaps PER-LEG from tick staleness inside
        DataManager; the backtester detects them from the delta between
        consecutive bar timestamps. The mechanisms are different and fire at
        different moments, so any reconciliation would be an invented
        equivalence. We surface the evidence and let a human judge.
        """
        bt_gaps = getattr(self.engine, 'gap_event_count', 0)
        # A live reset shows up as the strategy re-entering a warm-up state
        # partway through the run.
        late_warmups = compared['signal_type_live'].isin(WARMUP_SIGNAL_TYPES).sum()

        result.metrics['backtest_gap_resets'] = int(bt_gaps)
        result.metrics['live_post_warmup_warmup_bars'] = int(late_warmups)

        if bt_gaps or late_warmups:
            result.findings.append(
                f"Gap-detection asymmetry: backtest fired {bt_gaps} gap reset(s) "
                f"(bar-timestamp delta); live shows {late_warmups} post-warm-up "
                f"warm-up bar(s) (per-leg tick staleness). These mechanisms are "
                f"not equivalent by design — treat gap-adjacent divergence as "
                f"expected rather than as an engine bug."
            )

    # ==========================================
    # 💸 TIER 2 — COST REALISM (MEASUREMENT)
    # ==========================================
    def run_tier2(self) -> TierResult:
        """
        Compares realized live execution cost against the modelled cost.

        Note what this can and cannot say. The backtest's slippage_bps is a
        CONSTANT by construction — VirtualBroker applies default_slippage_bps
        and computes slippage against the same frictionless price. So the
        informative quantity is the LIVE distribution; the modelled side is just
        the assumption being tested. The output is therefore a calibration
        recommendation, not a pass/fail.
        """
        result = TierResult(tier='Tier 2', name='Cost Realism', status=INFO)

        if self.engine is None or self.engine.telemetry is None:
            result.status = SKIPPED
            result.headline = "Tier 1 must run first (it produces the backtest fills)."
            return result

        start, end = self._window
        live_fills = read_telemetry_range(
            self.live_telemetry_path, 'fills',
            pd.Timestamp('1970-01-01', tz='UTC'), pd.Timestamp('2100-01-01', tz='UTC'),
            run_id=self.live_run_id)
        bt_fills = read_telemetry_range(
            self.engine.telemetry.base_path, 'fills',
            start - timedelta(days=1), end + timedelta(days=1),
            run_id=self.backtest_run_id)

        result.metrics['live_fills'] = int(len(live_fills))
        result.metrics['backtest_fills'] = int(len(bt_fills))

        if live_fills.empty:
            result.status = SKIPPED
            result.headline = (
                "No live fills in this session — nothing to calibrate against. "
                "Run the paper engine until it has traded, then re-run.")
            return result

        live_slip = _describe(live_fills['slippage_bps'].values)
        bt_slip = _describe(bt_fills['slippage_bps'].values) if not bt_fills.empty else _describe(np.array([]))
        result.metrics['live_slippage_bps'] = live_slip
        result.metrics['modelled_slippage_bps'] = bt_slip

        modelled = (self.engine.broker.fee_params or {}).get('default_slippage_bps')
        result.metrics['configured_slippage_bps'] = modelled

        # --- Commission, normalised per notional so sizes don't distort it ---
        live_comm_bps = self._commission_bps(live_fills)
        bt_comm_bps = self._commission_bps(bt_fills) if not bt_fills.empty else np.array([])
        result.metrics['live_commission_bps'] = _describe(live_comm_bps)
        result.metrics['modelled_commission_bps'] = _describe(bt_comm_bps)

        # --- Per-symbol breakdown: the two legs can behave very differently ---
        per_symbol = {}
        for sym, grp in live_fills.groupby('symbol'):
            per_symbol[str(sym)] = {
                'fills': int(len(grp)),
                'slippage_bps': _describe(grp['slippage_bps'].values),
                'commission_bps': _describe(self._commission_bps(grp)),
            }
        result.metrics['live_per_symbol'] = per_symbol

        # --- The fees.py verdict ---
        live_median = live_slip['median']
        if live_median is not None and modelled is not None:
            bias = live_median - modelled
            result.metrics['slippage_bias_bps'] = float(bias)
            result.headline = (
                f"Live median slippage {live_median:.3f} bps vs modelled "
                f"{modelled:.3f} bps (bias {bias:+.3f} bps over {live_slip['n']} fills)."
            )
            result.findings.append(
                f"CALIBRATION: setting config.FEE_MODEL_PARAMS['default_slippage_bps'] "
                f"to {live_median:.3f} would centre the model on observed live "
                f"execution for this session."
            )
            if live_slip['n'] < 30:
                result.findings.append(
                    f"⚠️ Only {live_slip['n']} live fills. This is a directional "
                    f"indication, not a calibration — the dispersion "
                    f"(std {live_slip['std']:.3f} bps) is wide relative to the sample. "
                    f"Do not change fees.py on this alone."
                )
            if live_slip['std'] is not None and live_median != 0 and live_slip['std'] > abs(live_median):
                result.findings.append(
                    "Slippage dispersion exceeds its own median — a FLAT bps model "
                    "cannot represent this. Consider a spread-aware FX cost term "
                    "rather than re-tuning the constant."
                )
        else:
            result.headline = f"Measured {live_slip['n']} live fills; no modelled baseline to compare."

        result.findings.append(
            "realized_pnl is deliberately excluded: backtest fills emit 0.0 because "
            "VirtualBroker tracks net cash, not IBKR's lot-matching convention."
        )
        return result

    @staticmethod
    def _commission_bps(fills: pd.DataFrame) -> np.ndarray:
        """Commission as basis points of traded notional. Comparing raw dollars
        would conflate cost with position size."""
        if fills.empty:
            return np.array([])
        notional = fills['shares'].abs() * fills['price'].abs()
        with np.errstate(divide='ignore', invalid='ignore'):
            bps = np.where(notional > 0, fills['commission'] / notional * 10000.0, np.nan)
        return bps

    # ==========================================
    # 📊 TIER 3 — VENDOR DIVERGENCE (MEASUREMENT)
    # ==========================================
    def run_tier3(self) -> TierResult:
        """
        Quantifies how far the historical vendor's bars sit from what the live
        engine actually saw.

        This number does not exist anywhere else, and without it every
        Polygon-based backtest carries an unknown error bar. It is NOT a gate:
        the two series come from different venue sets and different bar
        construction, so they cannot agree. The point is to know by how much.
        """
        result = TierResult(tier='Tier 3', name='Vendor Divergence', status=INFO)

        if not self.data_library:
            result.status = SKIPPED
            result.headline = "No data_library supplied — cannot load vendor bars."
            return result

        live = self._load_live_decisions()
        if live.empty:
            result.status = SKIPPED
            result.headline = "No live decisions to compare against."
            return result

        start, end = self._window

        # --- Live-recorded closes, from the same snapshots the strategy saw ---
        live_rows = {}
        for ts, raw in zip(live['timestamp_utc'], live['market_snapshot']):
            try:
                live_rows[ts] = json.loads(raw) if raw else {}
            except (TypeError, ValueError):
                live_rows[ts] = {}
        live_closes = pd.DataFrame.from_dict(live_rows, orient='index')
        live_closes.index = _to_utc(live_closes.index)

        # --- Vendor bars over the same window ---
        try:
            source = ArcticBarSource(
                library_name=self.data_library,
                instrument_keys=list(self.instruments.keys()),
                missing_bar_policy=POLICY_REJECT,
            )
            vendor = source.load(start, end)
        except Exception as e:
            result.status = SKIPPED
            result.headline = f"Could not load vendor bars: {e}"
            return result

        if vendor is None or vendor.empty:
            result.status = SKIPPED
            result.headline = "Vendor library returned no bars for this window."
            return result

        vendor_closes = pd.DataFrame(
            {asset: vendor[(asset, 'close')] for asset in self.instruments.keys()
             if (asset, 'close') in vendor.columns})
        vendor_closes.index = _to_utc(vendor_closes.index)

        result.metrics['live_bars'] = int(len(live_closes))
        result.metrics['vendor_bars'] = int(len(vendor_closes))

        common = live_closes.index.intersection(vendor_closes.index)
        result.metrics['matched_timestamps'] = int(len(common))
        result.metrics['live_only_timestamps'] = int(len(live_closes.index.difference(vendor_closes.index)))
        result.metrics['vendor_only_timestamps'] = int(len(vendor_closes.index.difference(live_closes.index)))

        if len(common) == 0:
            result.headline = (
                "Zero overlapping timestamps between vendor bars and the live tape. "
                "Either the window has no vendor coverage, or the two series are "
                "offset (check timezone handling in the ingestion tool).")
            return result

        coverage = len(common) / max(len(live_closes), 1)
        result.metrics['timestamp_coverage'] = float(coverage)
        if coverage < 0.95:
            result.findings.append(
                f"Only {coverage:.1%} of live bars have a vendor counterpart. "
                f"Backtests over this window are running on a materially "
                f"different sample than the live engine traded.")

        per_leg = {}
        all_diffs = []
        for asset in self.instruments.keys():
            if asset not in live_closes.columns or asset not in vendor_closes.columns:
                continue
            lv = live_closes.loc[common, asset].astype(float)
            vd = vendor_closes.loc[common, asset].astype(float)
            mask = lv.notna() & vd.notna() & (lv != 0)
            if not mask.any():
                continue
            diff_bps = ((vd[mask] - lv[mask]) / lv[mask] * 10000.0).values
            all_diffs.append(diff_bps)

            buckets = {
                f'pct_abs_gt_{b}bps': float(np.mean(np.abs(diff_bps) > b))
                for b in VENDOR_DIVERGENCE_BUCKETS_BPS
            }
            per_leg[str(asset)] = {
                'diff_bps': _describe(diff_bps),
                'abs_diff_bps': _describe(np.abs(diff_bps)),
                **buckets,
            }

        result.metrics['per_leg'] = per_leg

        if all_diffs:
            pooled = np.abs(np.concatenate(all_diffs))
            pooled_stats = _describe(pooled)
            result.metrics['pooled_abs_diff_bps'] = pooled_stats
            result.headline = (
                f"Vendor bars differ from live by a median {pooled_stats['median']:.3f} bps "
                f"(p95 {pooled_stats['p95']:.3f} bps) across {len(common)} matched bars."
            )
            median_abs = pooled_stats['median'] or 0.0
            result.findings.append(
                f"Interpretation: a strategy whose edge per trade is smaller than "
                f"~{median_abs:.2f} bps cannot be distinguished from vendor noise in a "
                f"Polygon-based backtest. Treat this as the resolution floor of "
                f"historical research, not as an error to fix."
            )
        else:
            result.headline = "No comparable close pairs found across the matched timestamps."

        return result

    # ==========================================
    # 🧾 ORCHESTRATION & REPORTING
    # ==========================================
    def run_all(self, include_tier2: bool = True, include_tier3: bool = True) -> ParityReport:
        """Runs the tiers in order. Tier 1 first because it produces the backtest
        run that Tier 2 measures, and because a failing gate makes the
        measurements hard to interpret."""
        report = ParityReport(live_run_id=self.live_run_id)

        t1 = self.run_tier1()
        report.tiers.append(t1)

        if include_tier2:
            report.tiers.append(self.run_tier2())
        if include_tier3:
            report.tiers.append(self.run_tier3())

        report.backtest_run_id = self.backtest_run_id
        report.window_start = str(self._window[0]) if self._window[0] is not None else None
        report.window_end = str(self._window[1]) if self._window[1] is not None else None

        # Verdict is driven by GATE tiers only. Tiers 2 and 3 are measurements —
        # a large number there is a finding about the world, not a test failure.
        report.verdict = t1.status
        return report

    def print_report(self, report: ParityReport):
        """Human-readable summary."""
        print("\n" + "=" * 74)
        print("  BLUEGREY PARITY HARNESS")
        print("=" * 74)
        print(f"  Live run      : {report.live_run_id}")
        print(f"  Backtest run  : {report.backtest_run_id}")
        print(f"  Window        : {report.window_start}  ->  {report.window_end}")
        print(f"  VERDICT       : {report.verdict}  (Tier 1 gate)")
        print("=" * 74)

        for t in report.tiers:
            icon = {PASS: '✅', FAIL: '❌', INFO: 'ℹ️', SKIPPED: '⏭️'}.get(t.status, '•')
            print(f"\n{icon} {t.tier} — {t.name}  [{t.status}]")
            if t.headline:
                print(f"   {t.headline}")
            for k, v in t.metrics.items():
                if isinstance(v, dict):
                    if 'median' in v:
                        if v.get('n'):
                            print(f"   {k}: n={v['n']} median={v['median']:.4f} "
                                  f"mean={v['mean']:.4f} std={v['std']:.4f} "
                                  f"p05={v['p05']:.4f} p95={v['p95']:.4f}")
                        else:
                            print(f"   {k}: (no data)")
                    else:
                        print(f"   {k}:")
                        for sk, sv in v.items():
                            print(f"      {sk}: {sv}")
                else:
                    print(f"   {k}: {v}")
            for f in t.findings:
                print(f"   ⚠️  {f}")
            if t.samples:
                print("   First mismatches:")
                for s in t.samples[:10]:
                    print(f"      {s}")
        print("\n" + "=" * 74 + "\n")

    def save_report(self, report: ParityReport) -> Path:
        """Persists the report as JSON so runs can be diffed over time."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        path = self.output_dir / 'parity_report.json'
        with open(path, 'w') as f:
            json.dump(report.to_dict(), f, indent=2, default=str)
        return path


def _load_strategy_class(name: str):
    """Mirrors TradingEngine._load_strategy so the CLI resolves the same class
    the live engine ran."""
    if name == "KalmanPairsStrategy":
        from strategies.kalman_pairs import KalmanPairsStrategy
        return KalmanPairsStrategy
    import importlib
    module = importlib.import_module("strategies")
    return getattr(module, name)


def main():
    parser = argparse.ArgumentParser(description="Bluegrey backtest-live parity harness.")
    parser.add_argument('--run-id', required=True,
                        help="Live run_id to reconcile (see data/telemetry/sessions/).")
    parser.add_argument('--data-library', default=None,
                        help="ArcticDB library for Tier 3. Defaults to config.LIBS['fx_min'].")
    parser.add_argument('--risk-mode', default=None,
                        help="Override risk mode. Should match the live session's manifest.")
    parser.add_argument('--skip-tier2', action='store_true')
    parser.add_argument('--skip-tier3', action='store_true')
    parser.add_argument('--warmup', default='auto',
                        help="'auto' or an integer number of leading bars to exclude.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    warmup = args.warmup if args.warmup == 'auto' else int(args.warmup)
    library = args.data_library or getattr(config, 'LIBS', {}).get('fx_min')

    harness = ParityHarness(
        strategy_class=_load_strategy_class(config.STRATEGY_CLASS),
        instruments=config.INSTRUMENTS,
        params=config.STRATEGY_PARAMS,
        live_run_id=args.run_id,
        data_library=library,
        risk_mode=args.risk_mode,
        warmup_exclusion=warmup,
    )
    report = harness.run_all(include_tier2=not args.skip_tier2,
                             include_tier3=not args.skip_tier3)
    harness.print_report(report)
    path = harness.save_report(report)
    print(f"📄 Report saved to {path}")

    raise SystemExit(0 if report.verdict in (PASS, SKIPPED) else 1)


if __name__ == '__main__':
    main()