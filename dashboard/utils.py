"""
Display helpers for the Bluegrey telemetry dashboard.

Pure formatting / timezone utilities. The engine logs everything in UTC; the
operator thinks in Europe/Madrid — so most views show BOTH. Nothing here does
any I/O.
"""
import json
import logging
import os
from datetime import timedelta
from typing import Optional

import pandas as pd

logger = logging.getLogger("DashboardUtils")

# Operator-facing "local" timezone, shown alongside UTC. Overridable via APP_TZ.
APP_TZ = os.getenv("APP_TZ", "Europe/Madrid")

# Sentinel shown when a value is missing / not-yet-computable.
NA = "—"


# ---------------------------------------------------------------------------- #
# Timezone
# ---------------------------------------------------------------------------- #
def to_utc(ts) -> Optional[pd.Timestamp]:
    """Coerce a scalar timestamp to a tz-aware UTC Timestamp (or None)."""
    if ts is None:
        return None
    try:
        t = pd.Timestamp(ts)
    except Exception:
        return None
    if pd.isna(t):
        return None
    return t.tz_localize("UTC") if t.tz is None else t.tz_convert("UTC")


def to_local(ts) -> Optional[pd.Timestamp]:
    """Coerce a scalar timestamp to the operator-facing local tz (APP_TZ)."""
    t = to_utc(ts)
    if t is None:
        return None
    try:
        return t.tz_convert(APP_TZ)
    except Exception:
        # Unknown tz name -> fall back to UTC rather than crashing the UI.
        return t


def add_local_column(
    df: pd.DataFrame,
    utc_col: str = "timestamp_utc",
    local_col: str = "timestamp_local",
) -> pd.DataFrame:
    """
    Return a copy of df with a local-tz column derived from a UTC column.
    Safe on empty frames and on frames missing the source column.
    """
    out = df.copy()
    if utc_col not in out.columns or out.empty:
        out[local_col] = pd.Series(dtype="datetime64[ns, UTC]")
        return out

    s = pd.to_datetime(out[utc_col], utc=True, errors="coerce")
    try:
        out[local_col] = s.dt.tz_convert(APP_TZ)
    except Exception:
        out[local_col] = s
    return out


def fmt_ts_dual(ts) -> str:
    """'YYYY-MM-DD HH:MM:SS <local> / HH:MM:SS UTC', or NA."""
    u = to_utc(ts)
    if u is None:
        return NA
    loc = to_local(ts)
    loc_str = loc.strftime("%Y-%m-%d %H:%M:%S") if loc is not None else NA
    return f"{loc_str} {LOCAL_TZ_LABEL} / {u.strftime('%H:%M:%S')} UTC"


def _tz_abbr(tz_name: str) -> str:
    """Short label for the local tz, e.g. 'Madrid' from 'Europe/Madrid'."""
    return tz_name.split("/")[-1].replace("_", " ")


# Precomputed short label for column headers / captions.
LOCAL_TZ_LABEL = _tz_abbr(APP_TZ)


# ---------------------------------------------------------------------------- #
# Numbers
# ---------------------------------------------------------------------------- #
def fmt_price(x, decimals: int = 5) -> str:
    return NA if _isna(x) else f"{float(x):,.{decimals}f}"


def fmt_qty(x) -> str:
    return NA if _isna(x) else f"{float(x):,.0f}"


def fmt_money(x, decimals: int = 2) -> str:
    return NA if _isna(x) else f"{float(x):,.{decimals}f}"


def fmt_bps(x, decimals: int = 2) -> str:
    return NA if _isna(x) else f"{float(x):,.{decimals}f} bps"


def fmt_signed(x, decimals: int = 2) -> str:
    """Signed number with an explicit + for positives (for P&L figures)."""
    return NA if _isna(x) else f"{float(x):+,.{decimals}f}"


def humanize_timedelta(delta: Optional[timedelta]) -> str:
    """Compact 'Xd Yh Zm Ws' for uptimes / staleness ages."""
    if delta is None:
        return NA
    total = int(delta.total_seconds())
    sign = "-" if total < 0 else ""
    total = abs(total)
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    if minutes or hours or days:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return sign + " ".join(parts)


# ---------------------------------------------------------------------------- #
# JSON (meta / market_snapshot) parsing
# ---------------------------------------------------------------------------- #
def parse_json_dict(raw) -> dict:
    """
    Parse a telemetry JSON string column into a dict, tolerantly.
    Returns {} on null / blank / malformed input.
    """
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        if pd.isna(raw):
            return {}
    except (TypeError, ValueError):
        pass
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def position_label(current_pos) -> str:
    """Map current_pos (-1/0/+1) to a human label."""
    try:
        p = int(current_pos)
    except (TypeError, ValueError):
        return NA
    return {1: "LONG SPREAD", -1: "SHORT SPREAD", 0: "FLAT"}.get(p, NA)


# ---------------------------------------------------------------------------- #
# Internal
# ---------------------------------------------------------------------------- #
def _isna(x) -> bool:
    if x is None:
        return True
    try:
        return bool(pd.isna(x))
    except (TypeError, ValueError):
        return False