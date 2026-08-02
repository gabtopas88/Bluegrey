"""
src/bar_source.py
Bar sources for the event-driven backtester.

The event engine needs one thing: a time-indexed frame of aligned OHLCV bars
with (asset, metric) MultiIndex columns. WHERE those bars come from is a
separate concern, and this module owns it.

Two implementations ship today:

    ArcticBarSource        — historical vendor bars out of ArcticDB. The normal
                             research path. Default. Nothing about it depends on
                             the parity harness.

    TelemetryReplaySource  — replays the EXACT bar series a live session
                             consumed, reconstructed from its persisted
                             decisions stream. Opt-in, used only by the parity
                             harness.

Why the replay source exists
----------------------------
Historical data (Polygon) and live data (IBKR IDEALPRO) are different vendors
with different bar-construction methods, so they will NEVER match bar-for-bar.
That is structural, not a bug. It means a naive "backtest vs live" comparison
conflates two questions that need different instruments:

    Q1  Is the ENGINE faithful?  (deterministic — exact answer possible)
    Q2  Are the INPUTS and COST MODEL representative?  (statistical only)

Replaying the live tape drives data divergence to zero BY CONSTRUCTION, so any
residual difference is engine logic or cost model — which is exactly Q1. Vendor
divergence is then measured separately as its own data-quality report.

⚠️ Replay is CLOSE-ONLY. The live telemetry `market_snapshot` persists
{symbol: close} per bar, so open/high/low/volume are not recoverable. That is
sufficient for KalmanPairsStrategy (it reads closes exclusively) but a strategy
touching high/low cannot be replayed until the snapshot schema is widened.
available_metrics makes this explicit rather than silently synthesising bars.
"""
import json
import logging
from abc import ABC, abstractmethod
from typing import Iterable, List, Optional

import pandas as pd

from src.store import DataStore
from src.telemetry import read_telemetry_range

logger = logging.getLogger(__name__)

# Canonical metric ordering. The event engine reads columns positionally off a
# MultiIndex, so a stable order across sources is not cosmetic.
OHLCV_METRICS = ['open', 'high', 'low', 'close', 'volume']

# --- MISSING BAR POLICIES ---
# What to do when one leg has no bar for a timestamp but another does.
#
#   'reject' — leave the hole as NaN. The strategy sees an incomplete
#              cross-section and refuses to trade it (KalmanPairsStrategy
#              returns INVALID_PRICE on NaN). This mirrors the LIVE engine,
#              where DataManager._finalize_all_bars writes an explicit NaN row
#              for any leg that received no ticks in the minute.
#
#   'ffill'  — carry the last known price forward. LEGACY behaviour, retained
#              so prior research results remain reproducible.
#
# 'reject' is the default because ffill is a correctness problem in its own
# right, independent of any parity concern: a spread computed from one fresh
# leg and one carried-forward leg produces mean-reversion signals that are an
# artifact of the fill, not of the market. That is inflated backtest alpha.
POLICY_REJECT = 'reject'
POLICY_FFILL = 'ffill'
VALID_MISSING_BAR_POLICIES = (POLICY_REJECT, POLICY_FFILL)


class BarSource(ABC):
    """
    Interface between the event engine and its data.

    Contract for load(): returns a DataFrame indexed by a sorted DatetimeIndex,
    with MultiIndex columns (asset_key, metric). An empty DataFrame signals
    "no data" — implementations must not raise for an empty range.
    """

    @abstractmethod
    def load(self, start_date, end_date) -> pd.DataFrame:
        """Returns aligned bars for the range as a (asset, metric) MultiIndex frame."""
        raise NotImplementedError

    @property
    def available_metrics(self) -> List[str]:
        """Which OHLCV metrics this source can actually supply."""
        return list(OHLCV_METRICS)

    @property
    def description(self) -> str:
        """One-line description for run banners and telemetry manifests."""
        return self.__class__.__name__


class ArcticBarSource(BarSource):
    """
    Historical bars from ArcticDB. The default research path.

    Fixes a latent contract mismatch: DataStore.load() returns a dict of
    {metric -> Time x Assets matrix} (the shape the VECTOR path wants), but the
    event engine previously treated that return as a per-asset OHLCV frame and
    indexed it accordingly. Every code path raised. This source performs the
    pivot properly, calling the PUBLIC API once for the whole universe rather
    than reaching into _fetch_raw per symbol.
    """

    def __init__(self, library_name: str, instrument_keys: Iterable[str],
                 missing_bar_policy: str = POLICY_REJECT,
                 store: Optional[DataStore] = None):
        """
        :param store: optional pre-built DataStore, primarily for testing. When
            omitted one is constructed from library_name.
        """
        if missing_bar_policy not in VALID_MISSING_BAR_POLICIES:
            raise ValueError(
                f"missing_bar_policy must be one of {VALID_MISSING_BAR_POLICIES}, "
                f"got {missing_bar_policy!r}"
            )
        self.library_name = library_name
        self.instrument_keys = list(instrument_keys)
        self.missing_bar_policy = missing_bar_policy
        self.store = store if store is not None else DataStore(library_name=library_name)

    @property
    def description(self) -> str:
        return f"ArcticBarSource(library={self.library_name}, policy={self.missing_bar_policy})"

    def load(self, start_date, end_date) -> pd.DataFrame:
        # ONE call for the whole universe. DataStore.load aligns every symbol
        # onto a common index, which is precisely what the event loop needs;
        # calling it per-symbol and re-aligning afterwards would duplicate that
        # work and risk a different alignment.
        #
        # ffill is delegated to the store so the policy is applied BEFORE
        # alignment. Applying it afterwards cannot reconstruct which values were
        # genuinely missing — the information is already gone.
        matrices = self.store.load(
            self.instrument_keys,
            start_date,
            end_date,
            ffill=(self.missing_bar_policy == POLICY_FFILL),
        )

        if not matrices:
            logger.error("❌ No data returned from ArcticDB for the requested range.")
            return pd.DataFrame()

        close_matrix = matrices.get('close')
        if close_matrix is None or close_matrix.empty:
            logger.error("❌ No 'close' matrix in the loaded data.")
            return pd.DataFrame()

        missing = [k for k in self.instrument_keys if k not in close_matrix.columns]
        if missing:
            # Fail loudly. A silently-dropped leg would turn a pairs backtest
            # into a single-leg backtest that still "runs" and still prints a
            # tearsheet — the worst possible failure mode.
            logger.error(f"❌ Instruments absent from the loaded universe: {missing}")
            return pd.DataFrame()

        # Pivot {metric -> Time x Assets} into {asset -> Time x Metrics}, then
        # concat into the (asset, metric) MultiIndex the event loop expects.
        frames = {}
        for asset in self.instrument_keys:
            cols = {}
            for metric in OHLCV_METRICS:
                matrix = matrices.get(metric)
                if matrix is not None and asset in matrix.columns:
                    cols[metric] = matrix[asset]
            frames[asset] = pd.DataFrame(cols, columns=[m for m in OHLCV_METRICS if m in cols])

        universe = pd.concat(frames, axis=1)
        universe.sort_index(inplace=True)
        return universe


class TelemetryReplaySource(BarSource):
    """
    Replays the exact bar series a live session consumed.

    Source of truth is the live decisions stream: main._record_decision persists
    a market_snapshot ({symbol: close}) on EVERY bar, unconditionally, including
    warmup and rejected bars. That makes the stream a verbatim recording of what
    the strategy actually saw — no vendor, no re-derivation.

    Opt-in only. Nothing on the research path constructs this.
    """

    def __init__(self, telemetry_base_path, run_id: str,
                 instrument_keys: Optional[Iterable[str]] = None):
        """
        :param telemetry_base_path: the LIVE telemetry tree (e.g. data/telemetry).
        :param run_id: the live session to replay.
        :param instrument_keys: optional column restriction/ordering. When
            omitted, whatever the snapshots contain is used.
        """
        self.telemetry_base_path = telemetry_base_path
        self.run_id = run_id
        self.instrument_keys = list(instrument_keys) if instrument_keys else None

    @property
    def available_metrics(self) -> List[str]:
        # Close-only, by construction. Stated explicitly so callers can guard
        # rather than discover it through a KeyError mid-simulation.
        return ['close']

    @property
    def description(self) -> str:
        return f"TelemetryReplaySource(run_id={self.run_id}, close-only)"

    def load(self, start_date, end_date) -> pd.DataFrame:
        decisions = read_telemetry_range(
            base_path=self.telemetry_base_path,
            stream='decisions',
            start=start_date,
            end=end_date,
            run_id=self.run_id,
        )

        if decisions.empty:
            logger.error(f"❌ No decisions found for run_id={self.run_id} in the requested range.")
            return pd.DataFrame()

        # Each row carries a JSON {symbol: close}. Rows where every leg was
        # tickless produce an empty snapshot; those are preserved as all-NaN so
        # the replayed timeline keeps the SAME bar count as the live session.
        # Dropping them would silently re-time every downstream comparison.
        records = {}
        for ts, raw in zip(decisions['timestamp_utc'], decisions['market_snapshot']):
            try:
                snap = json.loads(raw) if raw else {}
            except (TypeError, ValueError):
                snap = {}
            records[ts] = snap

        frame = pd.DataFrame.from_dict(records, orient='index')
        frame.index = pd.to_datetime(frame.index, utc=True)
        frame.sort_index(inplace=True)

        # A live session that restarts mid-window can emit two rows for the same
        # minute. Keep the last — it reflects the most recent finalized bar.
        frame = frame[~frame.index.duplicated(keep='last')]

        if self.instrument_keys:
            for key in self.instrument_keys:
                if key not in frame.columns:
                    frame[key] = float('nan')
            frame = frame[self.instrument_keys]

        # Wrap as (asset, 'close'). No synthetic OHLC: fabricating open/high/low
        # from close would let a strategy that reads them run and produce
        # confident, meaningless numbers.
        universe = pd.concat({asset: frame[[asset]].rename(columns={asset: 'close'})
                              for asset in frame.columns}, axis=1)

        logger.info(
            f"🎞️ Replay tape loaded: {len(universe)} bars, "
            f"{len(frame.columns)} legs, run_id={self.run_id}"
        )
        return universe