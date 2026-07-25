"""
Read-only telemetry access layer for the Bluegrey dashboard.

WHY THIS EXISTS (and why it does NOT reuse TelemetryStore directly)
-------------------------------------------------------------------
The live engine persists telemetry via ``src.telemetry.TelemetryStore``. That
class is a WRITER: its ``__init__`` creates the stream directories and writes a
session manifest (``data/telemetry/sessions/{run_id}.json``). Instantiating it
from a monitoring dashboard would inject phantom sessions and mkdir into the
live telemetry tree — unacceptable for a read-only viewer.

So this module:
  - imports ONLY the schema constants (SCHEMAS / column names) from
    ``src.telemetry``, which are side-effect-free, to stay DRY and drift-proof;
    and
  - re-implements the exact resilient read pattern used by
    ``TelemetryStore.read_range()`` — glob the day-partitioned Parquet files,
    read each inside try/except, and SKIP any file that is momentarily
    unreadable. (The engine does read-modify-write straight to the final path,
    so a read that races a write can raise; skipping and retrying on the next
    refresh is the intended behaviour.)

It adds two things ``read_range()`` does not need but the dashboard does:
  - filename-date prefiltering, so a long-lived run does not force a re-read of
    every historical file on every refresh; and
  - a tiny per-file retry, to shrink the window in which a mid-write file is
    momentarily skipped.

Nothing in here writes to disk.
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger("DashboardReader")

# --- Schema column names: single source of truth = src.telemetry -------------
# We import ONLY the schema objects (no TelemetryStore instantiation, no side
# effects). If the import path is somehow unavailable at runtime, we fall back
# to a locally-mirrored column list so the dashboard degrades gracefully instead
# of hard-crashing. The primary source of truth remains src.telemetry.
try:
    from src.telemetry import SCHEMAS as _SCHEMAS

    _STREAM_COLUMNS: Dict[str, List[str]] = {
        stream: [field.name for field in schema]
        for stream, schema in _SCHEMAS.items()
    }
    _SCHEMA_SOURCE = "src.telemetry"
except Exception as exc:  # noqa: BLE001 (defensive import fallback)
    logger.warning(
        "Could not import schema from src.telemetry (%s). "
        "Falling back to locally-mirrored column names.", exc,
    )
    # Last-resort mirror of the engine's schemas (keep in sync with src.telemetry).
    _STREAM_COLUMNS = {
        'decisions': [
            'timestamp_utc', 'strategy', 'run_id', 'signal_type', 'current_pos',
            'held_qty_y', 'held_qty_x', 'meta', 'market_snapshot',
        ],
        'orders': [
            'timestamp_utc', 'run_id', 'ib_order_id', 'signal_type', 'symbol',
            'con_id', 'action', 'qty', 'order_type', 'limit_price',
            'estimated_price', 'estimated_volatility',
        ],
        'fills': [
            'timestamp_utc', 'run_id', 'ib_order_id', 'exec_id', 'symbol',
            'con_id', 'side', 'shares', 'price', 'commission', 'realized_pnl',
            'estimated_price', 'slippage_bps',
        ],
    }
    _SCHEMA_SOURCE = "local-fallback"

VALID_STREAMS = tuple(_STREAM_COLUMNS.keys())

# Per-file read retry: one quick retry absorbs the sub-second window in which the
# engine is mid-writing today's Parquet file (read-modify-write to final path).
_READ_RETRIES = 1
_READ_RETRY_SLEEP_S = 0.15


class TelemetryReader:
    """
    Resilient, read-only view over the Parquet telemetry tree.

    Layout (written by the engine):
        {telemetry_dir}/{stream}/{YYYY-MM-DD}.parquet   # UTC-dated partitions
        {telemetry_dir}/sessions/{run_id}.json          # per-boot manifests
    """

    def __init__(self, telemetry_dir: Optional[str] = None):
        # Default matches the container's :ro mount; override via TELEMETRY_DIR.
        self.base_path = Path(
            telemetry_dir or os.getenv("TELEMETRY_DIR", "/app/data/telemetry")
        )
        self.sessions_dir = self.base_path / "sessions"
        self.schema_source = _SCHEMA_SOURCE

    # ------------------------------------------------------------------ #
    # Sessions / run resolution
    # ------------------------------------------------------------------ #
    def list_sessions(self) -> List[dict]:
        """
        Parse every ``sessions/{run_id}.json`` manifest, newest first.

        Each returned dict is the raw manifest (run_id, strategy, started_utc,
        and merged context like risk_mode / market_data_type) augmented with a
        parsed ``started_dt`` (tz-aware UTC) for sorting/uptime. Unreadable
        manifests are skipped rather than raising.
        """
        if not self.sessions_dir.exists():
            return []

        sessions: List[dict] = []
        for path in self.sessions_dir.glob("*.json"):
            try:
                with open(path) as fh:
                    manifest = json.load(fh)
            except (OSError, ValueError) as exc:  # open + JSON decode errors
                logger.warning("Skipping unreadable session manifest %s: %s", path, exc)
                continue

            if not isinstance(manifest, dict):
                continue

            manifest["started_dt"] = _parse_utc(manifest.get("started_utc"))
            # Fall back to the filename stem if a manifest lacks run_id.
            manifest.setdefault("run_id", path.stem)
            sessions.append(manifest)

        # Newest first. Manifests without a parseable start sort last.
        _floor = pd.Timestamp.min.tz_localize("UTC")
        sessions.sort(
            key=lambda m: m.get("started_dt") or _floor,
            reverse=True,
        )
        return sessions

    def resolve_active_run(self) -> Optional[dict]:
        """Return the most-recently-started session manifest, or None."""
        sessions = self.list_sessions()
        return sessions[0] if sessions else None

    # ------------------------------------------------------------------ #
    # Stream reads
    # ------------------------------------------------------------------ #
    def read_stream(
        self,
        stream: str,
        start: datetime,
        end: datetime,
        run_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Load a stream over an inclusive UTC time range, resiliently.

        Mirrors ``TelemetryStore.read_range()``'s skip-on-unreadable behaviour,
        adds filename-date prefiltering and a per-file retry. Always returns a
        DataFrame with the correct columns (possibly empty). Rows are sorted by
        ``timestamp_utc`` ascending.
        """
        if stream not in VALID_STREAMS:
            raise ValueError(
                f"Unknown stream: {stream!r}. Expected one of {VALID_STREAMS}."
            )

        columns = _STREAM_COLUMNS[stream]
        stream_dir = self.base_path / stream
        if not stream_dir.exists():
            return pd.DataFrame(columns=columns)

        start_utc = _to_utc_ts(start)
        end_utc = _to_utc_ts(end)
        wanted_dates = _utc_date_strings(start_utc, end_utc)

        frames: List[pd.DataFrame] = []
        for path in sorted(stream_dir.glob("*.parquet")):
            # Prefilter by the UTC date encoded in the filename ({YYYY-MM-DD}).
            # Any file whose name isn't a recognisable date is still attempted
            # (belt-and-suspenders) rather than silently dropped.
            stem = path.stem
            if _looks_like_date(stem) and stem not in wanted_dates:
                continue

            df = self._read_parquet_resilient(path)
            if df is None or df.empty:
                continue

            try:
                df = df[
                    (df["timestamp_utc"] >= start_utc)
                    & (df["timestamp_utc"] <= end_utc)
                ]
                if run_id:
                    df = df[df["run_id"] == run_id]
            except (KeyError, TypeError, ValueError) as exc:  # missing col / dtype
                logger.warning("Filtering failed for %s: %s. Skipping file.", path, exc)
                continue

            if not df.empty:
                frames.append(df)

        if not frames:
            return pd.DataFrame(columns=columns)

        combined = pd.concat(frames, ignore_index=True)
        return combined.sort_values("timestamp_utc").reset_index(drop=True)

    @staticmethod
    def _read_parquet_resilient(path: Path) -> Optional[pd.DataFrame]:
        """
        Read a single Parquet file, tolerating a mid-write torn read.

        The engine writes today's file via read-modify-write straight to the
        final path, so a read can momentarily race the write and raise. We retry
        once after a short sleep; if it still fails, we return None and let the
        caller skip it for this refresh cycle (it will be complete next cycle).
        """
        for attempt in range(_READ_RETRIES + 1):
            try:
                return pd.read_parquet(path)
            except Exception as exc:  # noqa: BLE001 (torn-read resilience)
                if attempt < _READ_RETRIES:
                    time.sleep(_READ_RETRY_SLEEP_S)
                    continue
                logger.warning("Skipping unreadable telemetry file %s: %s", path, exc)
                return None
        return None

    def stream_columns(self, stream: str) -> List[str]:
        """Expose the canonical column list for a stream (for empty frames)."""
        if stream not in VALID_STREAMS:
            raise ValueError(f"Unknown stream: {stream!r}.")
        return list(_STREAM_COLUMNS[stream])


# ---------------------------------------------------------------------------- #
# Module-level helpers (pure, no I/O)
# ---------------------------------------------------------------------------- #
def _parse_utc(value) -> Optional[pd.Timestamp]:
    """Parse an ISO-ish timestamp string into a tz-aware UTC Timestamp, or None."""
    if value is None:
        return None
    try:
        ts = pd.Timestamp(value)
    except (ValueError, TypeError):  # bad/overflow/unparseable input -> treat as absent
        return None
    if pd.isna(ts):
        return None
    return ts.tz_localize("UTC") if ts.tz is None else ts.tz_convert("UTC")


def _to_utc_ts(value) -> pd.Timestamp:
    """Coerce any datetime-like into a tz-aware UTC Timestamp."""
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tz is None else ts.tz_convert("UTC")


def _utc_date_strings(start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> set:
    """Inclusive set of {YYYY-MM-DD} UTC date strings spanning [start, end]."""
    if end_utc < start_utc:
        start_utc, end_utc = end_utc, start_utc
    days = set()
    cursor = start_utc.normalize()
    last = end_utc.normalize()
    while cursor <= last:
        days.add(cursor.strftime("%Y-%m-%d"))
        cursor += timedelta(days=1)
    return days


def _looks_like_date(stem: str) -> bool:
    """True if a filename stem is a YYYY-MM-DD date."""
    try:
        datetime.strptime(stem, "%Y-%m-%d")  # noqa: DTZ007 (format check only)
        return True
    except ValueError:
        return False