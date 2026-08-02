"""
src/telemetry.py
Production Telemetry Store — append-only, Parquet-backed event log.

Three streams capture the full lifecycle of every trading decision:
    decisions: what the strategy thought   (one row per on_bar)
    orders:    what we asked IBKR to do    (one row per order sent)
    fills:     what IBKR actually did      (one row per exec callback)

Design principles:
    - Write-on-event. No buffering. A crash loses zero rows.
    - UTC daily partitioning: data/telemetry/{stream}/{YYYY-MM-DD}.parquet
    - run_id tags every row, so a session can be sliced cleanly out of months of history.
    - con_id present everywhere — symbol strings are not trustworthy joins.
    - Denormalized estimated_price on fills means slippage analysis needs no join.

The store is the shared schema between live and backtest. Both write the same
columns, and the parity harness (Workstream B) joins them on (timestamp, symbol).

WORKSTREAM B (PARITY HARNESS) — RUN IDENTITY:
    The live engine lets the store mint its own uuid4 run_id (unchanged
    behaviour). The event backtester INJECTS a deterministic run_id derived from
    a params_hash, so a backtest of a given configuration is addressable,
    reproducible, and matchable against the live session it is meant to
    reconcile with. See compute_params_hash() / make_backtest_run_id().

    Backtests write to an ISOLATED tree (data/backtests/{run_id}/) rather than
    the live telemetry tree. _append() does read-modify-write on a per-UTC-day
    file, so a backtest pointed at data/telemetry/ would physically concatenate
    its rows into the live event log — distinguishable only by run_id, but
    bloating and risking the one artifact that must stay pristine.
"""
import hashlib
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger("Telemetry")


# ==========================================
# 🛡️ TRUST BOUNDARY SANITIZER
# ==========================================
# IBKR's TWS API protocol uses sentinel values to mean "field not set" instead
# of nulls. UNSET_DOUBLE = sys.float_info.max ≈ 1.7976931348623157e+308. The
# ib_async wire decoder applies `float(value or 0)` which catches None and
# empty strings, but DOES NOT catch the sentinel string — "1.79...E308" parses
# to a valid (but absurd) float. If that ever leaks through, casting it to
# float here won't crash; it will silently corrupt the telemetry with garbage
# that breaks every downstream aggregation.
#
# We treat this module as the trust boundary: anything sourced from IBKR's
# wire protocol passes through _safe_ibkr_float once, exactly here. Anything
# past this layer can be trusted by downstream consumers (parity harness,
# dashboards, PnL reports).
_IBKR_UNSET_DOUBLE = 1.7976931348623157e+308
_IMPLAUSIBLE_MAGNITUDE = 1e15  # No legitimate trade value reaches this


def _safe_ibkr_float(value, default: float = 0.0) -> float:
    """
    Defensive coercion of IBKR-sourced numerics.

    Handles None, empty strings, NaN, the UNSET_DOUBLE sentinel, and any
    other implausibly-large value that would corrupt aggregations. Anything
    ambiguous becomes `default`.

    Apply only to fields sourced from the IBKR wire protocol. Do NOT apply
    to locally-computed values — they're trusted, and sanitizing them would
    hide real bugs in our own code behind silent zeros.
    """
    if value is None or value == "":
        return default
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    # NaN check (NaN != NaN is the canonical idiom)
    if f != f:
        return default
    if abs(f) >= _IMPLAUSIBLE_MAGNITUDE:
        logger.warning(
            f"⚠️ IBKR sanitizer caught implausible value {f!r}. "
            f"Substituting {default}. Likely UNSET_DOUBLE sentinel leak."
        )
        return default
    return f


# ==========================================
# 🔑 RUN IDENTITY — params_hash & run_id
# ==========================================
# The parity harness must answer: "was this backtest run under the SAME
# configuration as this live session?" A run_id alone cannot answer that — it is
# an opaque label. params_hash makes the CONFIGURATION itself addressable.
#
# Three inputs are folded in, deliberately:
#   1. strategy params  — entry_z / exit_z / z_lookback / delta / vt / sizing
#   2. risk mode        — ENFORCE vs SHADOW changes which orders survive at all
#   3. fee model params — the modelled cost of crossing the spread
#
# Omitting ANY of the three would let two runs share a hash while trading
# differently — precisely the silent mismatch the harness exists to catch. A
# SHADOW backtest must never be comparable-by-hash to an ENFORCE live session,
# and a backtest run at 1.0 bps modelled slippage must never be confused with
# one run at 3.0 bps once Tier 2 starts calibrating fees.py.

# 48 bits of SHA-256. At the scale of "runs a human will ever launch", the
# collision probability is negligible and the id stays readable in a filename.
PARAMS_HASH_LEN = 12

# run_id becomes BOTH a manifest filename ({run_id}.json) and a directory
# component for backtest trees, so it is a path-injection surface. Constrain it.
RUN_ID_MAX_LEN = 128
_RUN_ID_SAFE_RE = re.compile(r'^[A-Za-z0-9._-]+$')

# Manifest 'run_kind' values. The harness uses these to tell a live session from
# a replayed backtest without inspecting run_id string shape.
RUN_KIND_LIVE = 'live'
RUN_KIND_BACKTEST = 'backtest'


def _canonicalize(value):
    """
    Recursively coerce a config value into a stable, JSON-serialisable form.

    Stability is the entire point: the same logical configuration must produce
    byte-identical JSON in every process on every machine, or the hash is worse
    than useless — it would produce false parity mismatches that look like
    engine bugs.

    Rules:
      - dict      -> keys coerced to str (sorting is applied at dump time)
      - list/tuple-> order PRESERVED (order is meaningful), tuple flattened to
                     list since JSON has no tuple type
      - bool      -> left as bool. Checked BEFORE int because bool subclasses
                     int in Python; without this, True would become 1.0 and
                     collide with the integer 1.
      - int/float -> float. So a notebook passing base_qty=100 hashes identically
                     to a config carrying 100.0. Numeric type is not a strategy
                     difference and must not fork the hash.
      - None      -> None
      - anything else (Contract objects, Enums, Path) -> str(). Lossy BY DESIGN:
                     this is a fingerprint, not a serialiser.
    """
    if isinstance(value, dict):
        return {str(k): _canonicalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonicalize(v) for v in value]
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return float(value)
    if value is None:
        return None
    return str(value)


def canonical_params_payload(strategy_params: Optional[dict],
                             risk_mode: Optional[str],
                             fee_params: Optional[dict]) -> dict:
    """
    The exact structure that gets hashed.

    Exposed separately from the hash so the session manifest can persist it
    verbatim. When a parity check fails on a hash mismatch you need to see WHICH
    key differs — two disagreeing 12-char digests tell you nothing actionable.
    """
    return {
        'strategy_params': _canonicalize(strategy_params or {}),
        'risk_mode':       str(risk_mode or '').upper(),
        'fee_params':      _canonicalize(fee_params or {}),
    }


def compute_params_hash(strategy_params: Optional[dict],
                        risk_mode: Optional[str],
                        fee_params: Optional[dict]) -> str:
    """
    Deterministic short digest of the full trading configuration.

    Identical configuration -> identical hash, across processes and machines.
    Used to build backtest run_ids and to assert live/backtest comparability.

    :param strategy_params: e.g. config.STRATEGY_PARAMS
    :param risk_mode: the EFFECTIVE, validated RiskManager mode — read it off
        the constructed RiskManager (risk.mode), never off raw config. Config
        may hold an unknown value that the RiskManager silently downgrades to
        ENFORCE; hashing the config string would then record a mode that was
        never actually in force.
    :param fee_params: the IBKRFeeModel construction parameters, e.g.
        {'default_slippage_bps': 1.0}.
    """
    payload = canonical_params_payload(strategy_params, risk_mode, fee_params)
    # sort_keys makes dict insertion order irrelevant; separators strips the
    # incidental whitespace that would otherwise vary by json version.
    blob = json.dumps(
        payload,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=True,
        allow_nan=True,
        default=str,
    )
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()[:PARAMS_HASH_LEN]


def make_backtest_run_id(params_hash: str,
                         timestamp: Optional[datetime] = None,
                         suffix: Optional[str] = None) -> str:
    """
    Builds the run_id for a backtest:
        backtest_{params_hash}_{YYYYmmddTHHMMSS.ffffffZ}_{suffix}

    The params_hash makes every run of a given configuration groupable; the
    timestamp and suffix keep each individual execution distinct.

    Distinctness is NOT cosmetic. _append() does read-modify-write into
    {stream}/{date}.parquet, so re-running with an identical run_id against an
    identical tree would silently concatenate the second run's rows onto the
    first — doubling every decision and quietly corrupting every parity
    comparison downstream. The store also guards this (see
    allow_existing_run_id), but the id itself should not invite the collision.

    Why microseconds AND a random suffix: second resolution is not enough. The
    same configuration backtested over several timeframes or date ranges in a
    loop produces an identical params_hash, and those runs launch within the
    same second — so a second-resolution id collides on exactly the workflow
    this is built for. Microseconds fix the common case; the random suffix
    removes the dependency on clock resolution entirely.

    :param suffix: optional explicit uniquifier, primarily for tests that need
        a reproducible id. Randomly generated when omitted.
    """
    ts = timestamp or datetime.now(timezone.utc)
    ts_utc = ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    tag = suffix if suffix is not None else uuid.uuid4().hex[:6]
    return f"backtest_{params_hash}_{ts_utc.strftime('%Y%m%dT%H%M%S.%fZ')}_{tag}"


def _validate_run_id(run_id: str) -> str:
    """
    Guards the injection point.

    An injected run_id becomes a manifest filename, a directory component in the
    isolated backtest tree, and a column value in every Parquet row. An unchecked
    string is therefore a path-traversal vector ('../../etc/passwd') and a
    corrupt-file risk. Fail at construction time, loudly, before anything
    touches the filesystem.
    """
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("run_id must be a non-empty string.")

    run_id = run_id.strip()

    if len(run_id) > RUN_ID_MAX_LEN:
        raise ValueError(
            f"run_id exceeds {RUN_ID_MAX_LEN} characters: {run_id[:48]}..."
        )
    if not _RUN_ID_SAFE_RE.match(run_id):
        raise ValueError(
            f"run_id contains unsafe characters: {run_id!r}. "
            "Allowed: ASCII letters, digits, dot, underscore, hyphen."
        )
    return run_id


# ==========================================
# 📐 SCHEMAS — Single Source of Truth
# ==========================================
# Defined explicitly via PyArrow so dtype drift between sessions is impossible.
# Schema changes are an explicit version bump, not an accident.

DECISIONS_SCHEMA = pa.schema([
    ('timestamp_utc',   pa.timestamp('ns', tz='UTC')),
    ('strategy',        pa.string()),
    ('run_id',          pa.string()),
    ('signal_type',     pa.string()),
    ('current_pos',     pa.int8()),
    ('held_qty_y',      pa.float64()),
    ('held_qty_x',      pa.float64()),
    ('meta',            pa.string()),              # JSON-encoded strategy-specific dict
    ('market_snapshot', pa.string()),              # JSON-encoded {symbol: close}
])

ORDERS_SCHEMA = pa.schema([
    ('timestamp_utc',        pa.timestamp('ns', tz='UTC')),
    ('run_id',               pa.string()),
    ('ib_order_id',          pa.int64()),
    ('signal_type',          pa.string()),
    ('symbol',               pa.string()),
    ('con_id',               pa.int64()),
    ('action',               pa.string()),
    ('qty',                  pa.float64()),
    ('order_type',           pa.string()),
    ('limit_price',          pa.float64()),
    ('estimated_price',      pa.float64()),
    ('estimated_volatility', pa.float64()),
])

FILLS_SCHEMA = pa.schema([
    ('timestamp_utc',   pa.timestamp('ns', tz='UTC')),
    ('run_id',          pa.string()),
    ('ib_order_id',     pa.int64()),
    ('exec_id',         pa.string()),
    ('symbol',          pa.string()),
    ('con_id',          pa.int64()),
    ('side',            pa.string()),
    ('shares',          pa.float64()),
    ('price',           pa.float64()),
    ('commission',      pa.float64()),
    ('realized_pnl',    pa.float64()),
    ('estimated_price', pa.float64()),             # Denormalized from orders for fast slippage queries
    ('slippage_bps',    pa.float64()),             # Pre-computed, signed by trade direction
])

SCHEMAS = {
    'decisions': DECISIONS_SCHEMA,
    'orders':    ORDERS_SCHEMA,
    'fills':     FILLS_SCHEMA,
}


def read_telemetry_range(base_path, stream: str, start: datetime, end: datetime,
                         run_id: Optional[str] = None) -> pd.DataFrame:
    """
    Module-level reader for a telemetry stream over an inclusive UTC date range.

    Deliberately NOT a method: reading must not require constructing a
    TelemetryStore. Construction writes a session manifest and trips the
    run_id collision guard, so a reader that instantiates a store to read an
    EXISTING run would either corrupt it or refuse to open it. The parity
    harness and TelemetryReplaySource both need pure reads.

    TelemetryStore.read_range() delegates here so there is exactly one
    implementation of the read path.

    :param base_path: telemetry tree root (live tree or a backtest tree)
    :param stream: 'decisions', 'orders', or 'fills'
    :param start, end: bounding datetimes (UTC)
    :param run_id: optional filter to slice a single session
    """
    if stream not in SCHEMAS:
        raise ValueError(f"Unknown stream: {stream}")

    stream_dir = Path(base_path) / stream
    if not stream_dir.exists():
        return pd.DataFrame(columns=[f.name for f in SCHEMAS[stream]])

    # Walk every Parquet file in the stream directory, filter by date range.
    # For minute-bar volumes this is fine; if telemetry grows to GBs,
    # switch to pyarrow.dataset for pushdown filtering.
    start_utc = pd.Timestamp(start).tz_convert('UTC') if pd.Timestamp(start).tz else pd.Timestamp(start).tz_localize('UTC')
    end_utc = pd.Timestamp(end).tz_convert('UTC') if pd.Timestamp(end).tz else pd.Timestamp(end).tz_localize('UTC')

    frames = []
    for path in sorted(stream_dir.glob('*.parquet')):
        try:
            df = pd.read_parquet(path)
            df = df[(df['timestamp_utc'] >= start_utc) & (df['timestamp_utc'] <= end_utc)]
            if run_id:
                df = df[df['run_id'] == run_id]
            if not df.empty:
                frames.append(df)
        except Exception as e:
            logger.warning(f"Skipping unreadable telemetry file {path}: {e}")

    if not frames:
        return pd.DataFrame(columns=[f.name for f in SCHEMAS[stream]])

    return pd.concat(frames, ignore_index=True).sort_values('timestamp_utc')


class TelemetryStore:
    """
    Append-only Parquet event log.

    Constructed once at engine boot. The run_id is set in the constructor
    and embedded in every row written by this instance. A new boot = new run_id.

    Files are partitioned by UTC date. The store auto-rotates when the day
    rolls over mid-session, so a session that crosses midnight produces two files
    per stream — exactly what range queries expect.

    Workstream B: run_id is now injectable. Omit it and the store mints a uuid4
    exactly as before (the live engine's path is byte-for-byte unchanged);
    inject it and the backtester gets a deterministic, reproducible identity.
    """

    # Buffered mode auto-flushes past this many queued rows, bounding memory on
    # long backtests without giving up batching.
    FLUSH_THRESHOLD_ROWS = 50_000

    def __init__(self, base_path: Path, strategy_name: str = "unknown",
                 session_context: Optional[dict] = None,
                 run_id: Optional[str] = None,
                 run_kind: str = RUN_KIND_LIVE,
                 allow_existing_run_id: bool = False,
                 buffered: bool = False):
        """
        :param session_context: Optional dict of extra run metadata merged into
            the session manifest (e.g. {'risk_mode': 'SHADOW', 'market_data_type': 3}).
            Makes every run_id self-describing for the parity harness and audit —
            "was this run risk-gated? what data did it trade on?" — without
            re-deriving it from logs. Forward-compatible: Workstream B can add a
            params_hash here so backtest/live runs key off the same identifier.
            (Workstream B now does exactly that — callers should pass
            'params_hash' and 'params' from compute_params_hash() /
            canonical_params_payload().)
        :param run_id: Optional explicit run identifier. When None (the live
            engine's path) a uuid4 is minted, preserving the original behaviour
            exactly. When supplied it is validated before any filesystem access.
        :param run_kind: 'live' or 'backtest'. Recorded in the manifest so the
            parity harness can classify a run without pattern-matching the
            run_id string. Purely descriptive — it changes no write behaviour.
        :param allow_existing_run_id: Reusing a run_id against a tree that
            already holds one is a silent-corruption hazard: _append() does
            read-modify-write, so the second run's rows would be concatenated
            onto the first and every parity count would double. We therefore
            REFUSE by default and fail loudly. Set True only when deliberately
            resuming into an existing run. The live path is unaffected — a fresh
            uuid4 can never collide.
        :param buffered: accumulate rows in memory and write them in batches
            instead of one file rewrite per row.

            LIVE MUST STAY UNBUFFERED (the default). _append() does
            read-modify-write so that a crash loses zero rows; that guarantee is
            the whole reason the live event log is trustworthy.

            A BACKTEST is the opposite case: it emits thousands of rows per
            second, and unbuffered writes are O(n^2) — a one-year minute
            backtest would rewrite a growing Parquet file ~375,000 times. A
            backtest is also deterministic and re-runnable, so losing a partial
            run to a crash costs nothing. Callers using buffered=True MUST call
            flush() or close() (or use the store as a context manager), or the
            tail of the run is never written.
        """
        self.base_path = Path(base_path)
        self.strategy_name = strategy_name

        # Injectable identity (Workstream B). Validate BEFORE touching disk so a
        # malformed id can never create a directory or a manifest.
        self.run_id = _validate_run_id(run_id) if run_id is not None else str(uuid.uuid4())
        self.run_kind = str(run_kind or RUN_KIND_LIVE).lower()

        self.session_context = session_context or {}

        # Write batching. _buffer maps a target Parquet path to the rows queued
        # for it, so a flush touches each file exactly once.
        self._buffered = bool(buffered)
        self._buffer = {}
        self._buffered_row_count = 0

        # Ensure stream directories exist upfront.
        for stream in SCHEMAS.keys():
            (self.base_path / stream).mkdir(parents=True, exist_ok=True)

        # Collision guard. An existing manifest for this run_id means this tree
        # already holds rows under this identity; appending more would silently
        # merge two runs into one. Fail before writing anything.
        manifest_path = self._manifest_path()
        if manifest_path.exists() and not allow_existing_run_id:
            raise ValueError(
                f"run_id '{self.run_id}' already exists at {manifest_path}. "
                "Appending would silently merge two runs and corrupt parity "
                "counts. Use a new run_id, or pass allow_existing_run_id=True "
                "if you are deliberately resuming."
            )

        # Write the session manifest. This is a human-readable breadcrumb that
        # makes "what was running in this session" answerable at a glance.
        self._write_session_manifest()

        logger.info(f"📒 Telemetry initialized at {self.base_path}")
        logger.info(f"   run_id = {self.run_id}")
        logger.info(f"   run_kind = {self.run_kind}")
        if self._buffered:
            logger.info(f"   write mode = BUFFERED (flush required)")

    # ==========================================
    # 🗂️ MANIFEST & FILE PATHS
    # ==========================================
    def _manifest_path(self) -> Path:
        """Path of this run's session manifest. Single definition, used by the
        collision guard and the writer so the two can never disagree."""
        return self.base_path / 'sessions' / f"{self.run_id}.json"

    def _write_session_manifest(self):
        """One JSON file per run, written at boot. Contains the run metadata."""
        manifest_dir = self.base_path / 'sessions'
        manifest_dir.mkdir(parents=True, exist_ok=True)

        manifest = {
            'run_id':       self.run_id,
            'strategy':     self.strategy_name,
            'run_kind':     self.run_kind,
            'started_utc':  datetime.now(timezone.utc).isoformat(),
        }
        # Merge caller-supplied run context (risk_mode, market_data_type, and
        # later a params_hash). Explicit keys above win on collision.
        for k, v in self.session_context.items():
            manifest.setdefault(k, v)

        manifest_path = self._manifest_path()
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2, default=str)

    def _path_for(self, stream: str, ts: datetime) -> Path:
        """Returns the Parquet file path for a given stream and timestamp."""
        date_str = ts.astimezone(timezone.utc).strftime('%Y-%m-%d')
        return self.base_path / stream / f"{date_str}.parquet"

    # ==========================================
    # 🖊️ WRITE PATH
    # ==========================================
    def _append(self, stream: str, row: dict):
        """
        Atomic append of a single row to the day's Parquet file.

        Parquet doesn't support true row-level append (it's a columnar format
        with footer metadata), so we read-modify-write the day's file. At
        ~1-3 events/minute on minute bars, this is invisible. If event rate
        ever exceeds ~10/second sustained, switch to a buffered write path or
        a row-oriented format (Arrow IPC stream) for hot writes with nightly
        Parquet consolidation.
        """
        schema = SCHEMAS[stream]
        ts = row['timestamp_utc']
        path = self._path_for(stream, ts)

        # Buffered path: queue the row and let flush() do the file I/O. The row
        # is still schema-validated at flush time, so a schema violation is
        # caught either way — just later.
        if self._buffered:
            self._buffer.setdefault((stream, path), []).append(row)
            self._buffered_row_count += 1
            if self._buffered_row_count >= self.FLUSH_THRESHOLD_ROWS:
                self.flush()
            return

        # Coerce to a single-row Arrow table with schema enforcement.
        try:
            new_table = pa.Table.from_pylist([row], schema=schema)
        except Exception as e:
            logger.error(f"❌ Telemetry schema violation in '{stream}': {e}")
            logger.error(f"   Row: {row}")
            return

        if path.exists():
            try:
                existing = pq.read_table(path)
                combined = pa.concat_tables([existing, new_table])
            except Exception as e:
                logger.error(f"❌ Failed to read existing {path}: {e}. Skipping append.")
                return
        else:
            combined = new_table

        try:
            pq.write_table(combined, path, compression='snappy')
        except Exception as e:
            logger.error(f"❌ Failed to write {path}: {e}")

    def flush(self):
        """
        Writes every buffered row to disk and clears the buffer.

        No-op when unbuffered. Groups by target file so each Parquet file is
        read-modified-written ONCE per flush rather than once per row — that
        difference is what makes instrumented backtests viable.
        """
        if not self._buffered or not self._buffer:
            return

        for (stream, path), rows in self._buffer.items():
            schema = SCHEMAS[stream]
            try:
                new_table = pa.Table.from_pylist(rows, schema=schema)
            except Exception as e:
                logger.error(f"❌ Telemetry schema violation in '{stream}' during flush: {e}")
                continue

            if path.exists():
                try:
                    existing = pq.read_table(path)
                    combined = pa.concat_tables([existing, new_table])
                except Exception as e:
                    logger.error(f"❌ Failed to read existing {path}: {e}. Skipping flush for this file.")
                    continue
            else:
                combined = new_table

            try:
                pq.write_table(combined, path, compression='snappy')
            except Exception as e:
                logger.error(f"❌ Failed to write {path}: {e}")

        self._buffer = {}
        self._buffered_row_count = 0

    def close(self):
        """Flushes any pending rows. Safe to call repeatedly."""
        self.flush()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # Flush even on an exception: a partial backtest log is far more useful
        # for diagnosis than no log at all.
        self.close()
        return False

    # ==========================================
    # 📝 PUBLIC RECORDERS — One per stream
    # ==========================================
    def record_decision(
        self,
        timestamp: datetime,
        signal_type: str,
        current_pos: int,
        held_qty_y: float,
        held_qty_x: float,
        meta: dict,
        market_snapshot: dict,
    ):
        """Called from main.py after every on_bar(), unconditionally."""
        # Coerce to UTC-aware Timestamp regardless of input form:
        #   - naive datetime/Timestamp -> tz_localize('UTC')
        #   - tz-aware (any zone)      -> tz_convert('UTC')
        row = {
            'timestamp_utc':   pd.Timestamp(timestamp).tz_convert('UTC') if pd.Timestamp(timestamp).tz else pd.Timestamp(timestamp).tz_localize('UTC'),
            'strategy':        self.strategy_name,
            'run_id':          self.run_id,
            'signal_type':     signal_type,
            'current_pos':     int(current_pos),
            'held_qty_y':      float(held_qty_y),
            'held_qty_x':      float(held_qty_x),
            'meta':            json.dumps(meta, default=str),
            'market_snapshot': json.dumps(market_snapshot, default=str),
        }
        self._append('decisions', row)

    def record_order(
        self,
        ib_order_id: int,
        signal_type: str,
        symbol: str,
        con_id: int,
        action: str,
        qty: float,
        order_type: str,
        estimated_price: float,
        limit_price: Optional[float] = None,
        estimated_volatility: Optional[float] = None,
        timestamp: Optional[datetime] = None,
    ):
        """Called from ExecutionHandler.execute_signal right after placeOrder()."""
        ts = timestamp or datetime.now(timezone.utc)
        # All numeric fields here are sourced from our own strategy/execution
        # layer (not the IBKR wire), so they're trusted — no sanitizer needed.
        row = {
            'timestamp_utc':        pd.Timestamp(ts).tz_convert('UTC') if pd.Timestamp(ts).tz else pd.Timestamp(ts).tz_localize('UTC'),
            'run_id':               self.run_id,
            'ib_order_id':          int(ib_order_id),
            'signal_type':          signal_type,
            'symbol':               symbol,
            'con_id':               int(con_id) if con_id else 0,
            'action':               action.upper(),
            'qty':                  float(qty),
            'order_type':           order_type.upper(),
            'limit_price':          float(limit_price) if limit_price is not None else float('nan'),
            'estimated_price':      float(estimated_price),
            'estimated_volatility': float(estimated_volatility) if estimated_volatility is not None else float('nan'),
        }
        self._append('orders', row)

    def record_fill(
        self,
        ib_order_id: int,
        exec_id: str,
        symbol: str,
        con_id: int,
        side: str,
        shares: float,
        price: float,
        commission: float,
        realized_pnl: float,
        estimated_price: float,
        timestamp: Optional[datetime] = None,
    ):
        """
        Called from ExecutionHandler.on_exec_details.

        Slippage is computed here, signed against the trade direction:
          BOT: positive bps = paid above the estimated price (bad)
          SLD: positive bps = received below the estimated price (bad)
        So positive slippage_bps always means cost was higher than modeled.

        TRUST BOUNDARY: shares/price/commission/realized_pnl come from the
        IBKR wire and pass through _safe_ibkr_float. estimated_price came
        from our own ExecutionHandler.active_orders cache; slippage_bps is
        computed locally below. Those two are trusted as-is.
        """
        ts = timestamp or datetime.now(timezone.utc)

        # Sanitize IBKR-sourced numerics at the trust boundary.
        safe_shares       = _safe_ibkr_float(shares)
        safe_price        = _safe_ibkr_float(price)
        safe_commission   = _safe_ibkr_float(commission)
        safe_realized_pnl = _safe_ibkr_float(realized_pnl)

        # Slippage uses the sanitized price (post-trust-boundary) plus our own
        # estimated_price (already trusted). Compute on sanitized inputs so a
        # rogue fill price can't contaminate the slippage column.
        if estimated_price and estimated_price > 0 and safe_price > 0:
            raw_bps = (safe_price - estimated_price) / estimated_price * 10000.0
            slippage_bps = raw_bps if side.upper() in ('BOT', 'BUY') else -raw_bps
        else:
            slippage_bps = float('nan')

        row = {
            'timestamp_utc':   pd.Timestamp(ts).tz_convert('UTC') if pd.Timestamp(ts).tz else pd.Timestamp(ts).tz_localize('UTC'),
            'run_id':          self.run_id,
            'ib_order_id':     int(ib_order_id),
            'exec_id':         str(exec_id),
            'symbol':          symbol,
            'con_id':          int(con_id) if con_id else 0,
            'side':            side.upper(),
            'shares':          safe_shares,
            'price':           safe_price,
            'commission':      safe_commission,
            'realized_pnl':    safe_realized_pnl,
            'estimated_price': float(estimated_price) if estimated_price else float('nan'),
            'slippage_bps':    float(slippage_bps),
        }
        self._append('fills', row)

    # ==========================================
    # 📖 READ PATH — for the Parity Harness
    # ==========================================
    def read_range(
        self,
        stream: str,
        start: datetime,
        end: datetime,
        run_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Loads a stream over an inclusive UTC date range.

        :param stream: 'decisions', 'orders', or 'fills'
        :param start, end: bounding datetimes (UTC)
        :param run_id: optional filter to slice a single session
        """
        # Delegates to the module-level reader so there is one implementation
        # of the read path. Instance state supplies only the base_path.
        return read_telemetry_range(
            base_path=self.base_path,
            stream=stream,
            start=start,
            end=end,
            run_id=run_id,
        )