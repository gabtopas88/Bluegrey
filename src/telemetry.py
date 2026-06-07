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
"""
import json
import logging
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


class TelemetryStore:
    """
    Append-only Parquet event log.

    Constructed once at engine boot. The run_id is set in the constructor
    and embedded in every row written by this instance. A new boot = new run_id.

    Files are partitioned by UTC date. The store auto-rotates when the day
    rolls over mid-session, so a session that crosses midnight produces two files
    per stream — exactly what range queries expect.
    """

    def __init__(self, base_path: Path, strategy_name: str = "unknown"):
        self.base_path = Path(base_path)
        self.strategy_name = strategy_name
        self.run_id = str(uuid.uuid4())

        # Ensure stream directories exist upfront.
        for stream in SCHEMAS.keys():
            (self.base_path / stream).mkdir(parents=True, exist_ok=True)

        # Write the session manifest. This is a human-readable breadcrumb that
        # makes "what was running in this session" answerable at a glance.
        self._write_session_manifest()

        logger.info(f"📒 Telemetry initialized at {self.base_path}")
        logger.info(f"   run_id = {self.run_id}")

    # ==========================================
    # 🗂️ MANIFEST & FILE PATHS
    # ==========================================
    def _write_session_manifest(self):
        """One JSON file per run, written at boot. Contains the run metadata."""
        manifest_dir = self.base_path / 'sessions'
        manifest_dir.mkdir(parents=True, exist_ok=True)

        manifest = {
            'run_id':       self.run_id,
            'strategy':     self.strategy_name,
            'started_utc':  datetime.now(timezone.utc).isoformat(),
        }
        manifest_path = manifest_dir / f"{self.run_id}.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)

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
        if stream not in SCHEMAS:
            raise ValueError(f"Unknown stream: {stream}")

        stream_dir = self.base_path / stream
        if not stream_dir.exists():
            return pd.DataFrame()

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