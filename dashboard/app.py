"""
Bluegrey — read-only telemetry dashboard (Streamlit entrypoint).

Run inside the cloud stack:
    streamlit run dashboard/app.py --server.port=8501 --server.address=0.0.0.0

Reach it via SSH tunnel (nothing is exposed publicly):
    ssh -L 8501:localhost:8501 <vm>   ->   http://localhost:8501

This app is strictly READ-ONLY: it reads the engine's Parquet telemetry via
TelemetryReader (which never instantiates the writer) and never connects to
IBKR. It therefore cannot disturb the single market-data session the engine
holds, nor affect strategy behaviour.

Auto-refresh uses st.fragment(run_every=...), which reruns only the live panels
(requires Streamlit >= 1.37; falls back gracefully on older versions). Sidebar
controls live outside the fragment and trigger a full rerun on change.
"""
import os
import sys

# Make `from dashboard...` and `from src.telemetry...` importable even if
# PYTHONPATH is not set (e.g. when running locally outside the container).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from datetime import timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard import utils
from dashboard.telemetry_reader import TelemetryReader

# ---------------------------------------------------------------------------- #
# Configuration (env-driven; defaults mirror the strategy / compose service)
# ---------------------------------------------------------------------------- #
TELEMETRY_DIR = os.getenv("TELEMETRY_DIR", "/app/data/telemetry")

# Entry / exit z reference lines drawn on the spread chart. Defaults mirror the
# KalmanPairsStrategy thresholds (entry_z=2.0, exit_z=0.0). They are NOT present
# in the telemetry stream, so they are configured here (overridable via env) to
# avoid hard-coupling the dashboard to the strategy's config module.
ENTRY_Z = float(os.getenv("DASH_ENTRY_Z", "2.0"))
EXIT_Z = float(os.getenv("DASH_EXIT_Z", "0.0"))

# UI auto-refresh cadence (seconds). The engine writes ~1-3 events/min on minute
# bars, so ~12s is ample without hammering the disk.
REFRESH_SECONDS = int(os.getenv("DASH_REFRESH_SECONDS", "12"))

# Liveness: if no decision row has landed in this many seconds, warn. The engine
# writes one decision per minute bar, so ~3 missed bars is a genuine anomaly.
STALE_AFTER_SECONDS = max(REFRESH_SECONDS * 3, 180)

# Recent-view window for the (heavy) 'decisions' stream that drives the z-score
# chart. Orders / fills / P&L span the WHOLE run instead — they're sparse.
LOOKBACK_CHOICES = {
    "Last 6 hours": timedelta(hours=6),
    "Last 24 hours": timedelta(hours=24),
    "Last 3 days": timedelta(days=3),
    "Last 7 days": timedelta(days=7),
}
DEFAULT_LOOKBACK = "Last 24 hours"

# Human-readable IBKR market-data modes (reqMarketDataType), mirrors src.main.
_MARKET_DATA_MODES = {1: "LIVE", 2: "FROZEN", 3: "DELAYED", 4: "DELAYED-FROZEN"}

st.set_page_config(
    page_title="Bluegrey · Telemetry",
    page_icon="📈",
    layout="wide",
)

# One reader for the whole app; it only holds a path (cheap to re-create).
reader = TelemetryReader(TELEMETRY_DIR)


# ---------------------------------------------------------------------------- #
# Last-good cache helpers (survive across fragment reruns via session_state)
# ---------------------------------------------------------------------------- #
def _remember_good(key: str, value) -> None:
    st.session_state[f"_lastgood_{key}"] = value


def _recall_good(key: str, default=None):
    return st.session_state.get(f"_lastgood_{key}", default)


# ---------------------------------------------------------------------------- #
# Small pure helpers
# ---------------------------------------------------------------------------- #
def _latest_row(df: pd.DataFrame):
    """Last row (newest, since reads are sorted ascending) or None."""
    return df.iloc[-1] if not df.empty else None


def _num_or_none(v):
    """Float or None (treats NaN / non-numeric as None)."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(f) else f


# ---------------------------------------------------------------------------- #
# Sidebar controls (full rerun on change)
# ---------------------------------------------------------------------------- #
def _render_sidebar():
    """Build sidebar controls and return (pinned_run_id_or_None, lookback)."""
    st.sidebar.title("Bluegrey Telemetry")
    st.sidebar.caption("Read-only · never touches IBKR")

    sessions = reader.list_sessions()

    auto = "Auto (latest session)"
    run_options = [auto]
    label_to_id = {}
    for s in sessions:
        rid = s.get("run_id", "?")
        started = s.get("started_dt")
        started_txt = utils.fmt_ts_dual(started) if started is not None else "unknown start"
        label = f"{str(rid)[:8]} · {s.get('strategy', '?')} · {started_txt}"
        run_options.append(label)
        label_to_id[label] = rid

    selected_label = st.sidebar.selectbox(
        "Session (run_id)",
        run_options,
        index=0,
        help="'Auto' always follows the newest session, even after an engine restart.",
    )
    pinned_run_id = None if selected_label == auto else label_to_id.get(selected_label)

    lookback_label = st.sidebar.selectbox(
        "Signal chart window",
        list(LOOKBACK_CHOICES.keys()),
        index=list(LOOKBACK_CHOICES.keys()).index(DEFAULT_LOOKBACK),
        help="Time window for the z-score / β chart. Orders, fills and P&L "
             "always cover the whole selected session.",
    )
    lookback = LOOKBACK_CHOICES[lookback_label]

    st.sidebar.divider()
    st.sidebar.caption(
        f"Auto-refresh every {REFRESH_SECONDS}s · times shown in "
        f"{utils.LOCAL_TZ_LABEL} + UTC"
    )
    if reader.schema_source != "src.telemetry":
        st.sidebar.warning(
            "Schema loaded from local fallback (src.telemetry import failed). "
            "Column names may drift from the engine."
        )
    return pinned_run_id, lookback


# ---------------------------------------------------------------------------- #
# Panels
# ---------------------------------------------------------------------------- #
def _render_session_status(active, decisions, now_utc, decisions_stale):
    st.subheader("Session status")

    run_id = active.get("run_id", utils.NA)
    strategy = active.get("strategy", utils.NA)
    started_dt = active.get("started_dt")
    risk_mode = active.get("risk_mode", utils.NA)
    mdt = active.get("market_data_type", None)
    mdt_label = _MARKET_DATA_MODES.get(mdt, str(mdt) if mdt is not None else utils.NA)

    latest = _latest_row(decisions)
    latest_meta = utils.parse_json_dict(latest["meta"]) if latest is not None else {}
    last_decision_ts = utils.to_utc(latest["timestamp_utc"]) if latest is not None else None

    uptime = (now_utc - started_dt) if started_dt is not None else None
    decision_age = (now_utc - last_decision_ts) if last_decision_ts is not None else None

    # Halt banner takes precedence over everything.
    halted = bool(latest_meta.get("halted", False))
    if halted:
        st.error(
            f"🛑 STRATEGY HALTED — {latest_meta.get('halt_reason') or 'no reason recorded'}"
        )

    # Liveness indicator.
    if decision_age is None:
        st.warning("No decisions found in the selected window yet.")
    elif decision_age.total_seconds() > STALE_AFTER_SECONDS:
        st.warning(
            f"⚠️ Last decision was {utils.humanize_timedelta(decision_age)} ago "
            f"(> {STALE_AFTER_SECONDS}s). The engine may be down, disconnected, "
            "or the market may be closed."
        )
    else:
        st.success(
            f"🟢 Live — last decision {utils.humanize_timedelta(decision_age)} ago"
        )

    if decisions_stale:
        st.caption(
            "⏳ Showing last-good decisions snapshot (a fresh read came back empty "
            "— likely a mid-write; retrying automatically)."
        )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Run ID", str(run_id)[:8])
    c1.caption(f"full: `{run_id}`")
    c2.metric("Strategy", str(strategy))
    c3.metric("Risk mode", str(risk_mode))
    c4.metric("Market data", str(mdt_label))

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Uptime", utils.humanize_timedelta(uptime))
    c5.caption(f"since {utils.fmt_ts_dual(started_dt)}")
    c6.metric(
        "Last decision",
        f"{utils.humanize_timedelta(decision_age)} ago" if decision_age is not None else utils.NA,
    )
    c6.caption(utils.fmt_ts_dual(last_decision_ts) if last_decision_ts is not None else utils.NA)
    c7.metric("Last signal", str(latest["signal_type"]) if latest is not None else utils.NA)
    c8.metric("Halted", "YES" if halted else "no")


def _render_signal_chart(decisions):
    st.subheader("Spread signal — z-score & Kalman β")

    if decisions.empty:
        st.info("No decision rows in the selected window.")
        return

    df = utils.add_local_column(decisions)
    metas = df["meta"].apply(utils.parse_json_dict)
    df = df.assign(
        z=metas.apply(lambda m: _num_or_none(m.get("z"))),
        beta=metas.apply(lambda m: _num_or_none(m.get("beta"))),
    )

    x_local = df["timestamp_local"]
    utc_str = df["timestamp_utc"].dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    fig = go.Figure()

    # z-score (primary y-axis). connectgaps=False so warmup gaps show as breaks.
    fig.add_trace(go.Scatter(
        x=x_local, y=df["z"], name="z-score", mode="lines",
        connectgaps=False, line=dict(width=2), customdata=utc_str,
        hovertemplate="z = %{y:.3f}<br>%{x|%Y-%m-%d %H:%M:%S} "
                      + f"{utils.LOCAL_TZ_LABEL}<br>%{{customdata}}<extra></extra>",
    ))

    # β on the secondary y-axis.
    fig.add_trace(go.Scatter(
        x=x_local, y=df["beta"], name="β (hedge ratio)", mode="lines",
        connectgaps=False, line=dict(width=1, dash="dot"), yaxis="y2",
        customdata=utc_str,
        hovertemplate="β = %{y:.4f}<br>%{x|%Y-%m-%d %H:%M:%S} "
                      + f"{utils.LOCAL_TZ_LABEL}<br>%{{customdata}}<extra></extra>",
    ))

    # Entry / exit reference lines on the z-axis.
    fig.add_hline(y=ENTRY_Z, line_dash="dash", line_color="crimson",
                  annotation_text=f"+{ENTRY_Z:g} entry", annotation_position="top left")
    fig.add_hline(y=-ENTRY_Z, line_dash="dash", line_color="crimson",
                  annotation_text=f"-{ENTRY_Z:g} entry", annotation_position="bottom left")
    fig.add_hline(y=EXIT_Z, line_dash="dot", line_color="gray",
                  annotation_text=f"{EXIT_Z:g} exit", annotation_position="top left")

    fig.update_layout(
        height=430,
        margin=dict(l=10, r=10, t=30, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis=dict(title=f"Time ({utils.LOCAL_TZ_LABEL})"),
        yaxis=dict(title="z-score"),
        yaxis2=dict(title="β", overlaying="y", side="right", showgrid=False),
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)

    latest = df.iloc[-1]
    lz, lb = latest["z"], latest["beta"]
    dist = (ENTRY_Z - abs(lz)) if lz is not None else None
    k1, k2, k3 = st.columns(3)
    k1.metric("Latest z", utils.fmt_money(lz, 3) if lz is not None else "warming up")
    k2.metric("Latest β", utils.fmt_money(lb, 4) if lb is not None else utils.NA)
    k3.metric(
        "z distance to entry",
        utils.fmt_money(dist, 3) if dist is not None else utils.NA,
        help="How far |z| is from the ±entry threshold. ≤ 0 means the entry "
             "band is breached.",
    )


def _render_position(decisions):
    st.subheader("Current position")

    latest = _latest_row(decisions)
    if latest is None:
        st.info("No position data yet.")
        return

    meta = utils.parse_json_dict(latest["meta"])
    snapshot = utils.parse_json_dict(latest["market_snapshot"])
    leg_y = meta.get("leg_y", "leg Y")
    leg_x = meta.get("leg_x", "leg X")

    c1, c2, c3 = st.columns(3)
    c1.metric("State", utils.position_label(latest["current_pos"]))
    c2.metric(f"{leg_y} qty", utils.fmt_qty(latest["held_qty_y"]))
    c3.metric(f"{leg_x} qty", utils.fmt_qty(latest["held_qty_x"]))

    # Last known leg prices from the decision's market snapshot.
    if snapshot:
        price_cols = st.columns(len(snapshot))
        for col, (sym, px) in zip(price_cols, snapshot.items()):
            col.metric(f"{sym} close", utils.fmt_price(px))

    # Surface a staged-but-not-committed transition, if any.
    pending_pos = meta.get("pending_current_pos")
    if pending_pos is not None:
        st.caption(
            f"⏳ Pending transition staged → target current_pos={pending_pos} "
            "(awaiting risk approval / commit)."
        )


def _render_pnl(fills):
    st.subheader("Cumulative P&L (from fills)")

    if fills.empty:
        st.info("No fills recorded for this session yet.")
        return

    realized = pd.to_numeric(fills["realized_pnl"], errors="coerce").fillna(0.0)
    commission = pd.to_numeric(fills["commission"], errors="coerce").fillna(0.0)

    total_realized = float(realized.sum())
    total_commission = float(commission.sum())
    net = total_realized - total_commission

    c1, c2, c3 = st.columns(3)
    c1.metric("Realized P&L", utils.fmt_signed(total_realized))
    c2.metric("Commissions", utils.fmt_money(total_commission))
    c3.metric("Net (realized − commissions)", utils.fmt_signed(net))

    # DO NOT trust Net blindly: IBKR's realizedPNL may already be net of
    # commissions. Flag it loudly rather than silently combining the two.
    st.caption(
        "⚠️ **Validate before trusting Net:** IBKR's `realized_pnl` may already "
        "be net of commissions. Reconcile one closed trade against an IBKR "
        "statement to confirm whether commissions are double-counted here."
    )

    # Cumulative realized curve over the session (from fills only).
    df = utils.add_local_column(fills)
    df = df.assign(cum_realized=realized.cumsum().values)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["timestamp_local"], y=df["cum_realized"],
        mode="lines+markers", name="Cumulative realized P&L", line=dict(width=2),
    ))
    fig.add_hline(y=0.0, line_dash="dot", line_color="gray")
    fig.update_layout(
        height=300,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis=dict(title=f"Time ({utils.LOCAL_TZ_LABEL})"),
        yaxis=dict(title="Cumulative realized P&L"),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)


def _time_columns(df):
    """Add display-friendly dual-time columns; newest first."""
    out = utils.add_local_column(df)
    out = out.sort_values("timestamp_utc", ascending=False).reset_index(drop=True)
    out["time_local"] = out["timestamp_local"].dt.strftime("%Y-%m-%d %H:%M:%S")
    out["time_utc"] = out["timestamp_utc"].dt.strftime("%H:%M:%S")
    return out


def _render_orders_table(orders):
    if orders.empty:
        st.info("No orders in this session.")
        return
    df = _time_columns(orders)
    view = df[[
        "time_local", "time_utc", "signal_type", "symbol", "action", "qty",
        "order_type", "limit_price", "estimated_price", "estimated_volatility",
    ]]
    st.dataframe(
        view, use_container_width=True, hide_index=True,
        column_config={
            "time_local": st.column_config.TextColumn(f"Time ({utils.LOCAL_TZ_LABEL})"),
            "time_utc": st.column_config.TextColumn("UTC"),
            "signal_type": st.column_config.TextColumn("Signal"),
            "symbol": st.column_config.TextColumn("Symbol"),
            "action": st.column_config.TextColumn("Action"),
            "qty": st.column_config.NumberColumn("Qty", format="%.0f"),
            "order_type": st.column_config.TextColumn("Type"),
            "limit_price": st.column_config.NumberColumn("Limit", format="%.5f"),
            "estimated_price": st.column_config.NumberColumn("Est. price", format="%.5f"),
            "estimated_volatility": st.column_config.NumberColumn("Est. vol", format="%.5f"),
        },
    )


def _render_fills_table(fills):
    if fills.empty:
        st.info("No fills in this session.")
        return
    df = _time_columns(fills)
    view = df[[
        "time_local", "time_utc", "symbol", "side", "shares", "price",
        "commission", "realized_pnl", "slippage_bps", "estimated_price",
    ]]
    st.dataframe(
        view, use_container_width=True, hide_index=True,
        column_config={
            "time_local": st.column_config.TextColumn(f"Time ({utils.LOCAL_TZ_LABEL})"),
            "time_utc": st.column_config.TextColumn("UTC"),
            "symbol": st.column_config.TextColumn("Symbol"),
            "side": st.column_config.TextColumn("Side"),
            "shares": st.column_config.NumberColumn("Shares", format="%.0f"),
            "price": st.column_config.NumberColumn("Fill price", format="%.5f"),
            "commission": st.column_config.NumberColumn("Commission", format="%.4f"),
            "realized_pnl": st.column_config.NumberColumn("Realized P&L", format="%.2f"),
            "slippage_bps": st.column_config.NumberColumn("Slippage (bps)", format="%.2f"),
            "estimated_price": st.column_config.NumberColumn("Est. price", format="%.5f"),
        },
    )


def _render_orders_and_fills(orders, fills):
    st.subheader("Orders & fills")
    tab_orders, tab_fills = st.tabs(["Orders", "Fills"])
    with tab_orders:
        _render_orders_table(orders)
    with tab_fills:
        _render_fills_table(fills)


# ---------------------------------------------------------------------------- #
# Live view (auto-refreshing fragment)
# ---------------------------------------------------------------------------- #
def _live_view(pinned_run_id, lookback):
    now_utc = pd.Timestamp.now(tz="UTC")

    # Resolve which session we're viewing. In 'Auto' mode we re-resolve every
    # cycle so the dashboard follows an engine restart without a full rerun.
    if pinned_run_id is None:
        active = reader.resolve_active_run()
    else:
        active = next(
            (s for s in reader.list_sessions() if s.get("run_id") == pinned_run_id),
            None,
        )

    if active is None:
        st.info(
            "Waiting for telemetry… No session manifests found yet under "
            f"`{TELEMETRY_DIR}/sessions/`. The dashboard will populate once the "
            "engine has booted and written its first session."
        )
        return

    run_id = active.get("run_id")
    started_dt = active.get("started_dt")
    run_start = started_dt if started_dt is not None else (now_utc - timedelta(days=7))

    # Heavy stream: window it. Light streams: whole run.
    chart_start = now_utc - lookback
    decisions = reader.read_stream("decisions", chart_start, now_utc, run_id=run_id)
    orders = reader.read_stream("orders", run_start, now_utc, run_id=run_id)
    fills = reader.read_stream("fills", run_start, now_utc, run_id=run_id)

    # Last-good handling for the DECISIONS stream only. Empty orders/fills is a
    # legitimate state (a flat strategy simply hasn't traded), so those are
    # shown as-is; only decisions going empty implies a possible torn read.
    decisions_stale = False
    if decisions.empty:
        cached = _recall_good(f"decisions_{run_id}")
        if cached is not None and not cached.empty:
            decisions = cached
            decisions_stale = True
    else:
        _remember_good(f"decisions_{run_id}", decisions)

    _render_session_status(active, decisions, now_utc, decisions_stale)
    st.divider()
    _render_signal_chart(decisions)
    st.divider()
    _render_position(decisions)
    st.divider()
    _render_pnl(fills)
    st.divider()
    _render_orders_and_fills(orders, fills)


def _auto_refresh(func):
    """
    Wrap the live view so it auto-reruns every REFRESH_SECONDS, using whichever
    fragment API this Streamlit version exposes. Degrades to a single render
    (manual F5 refresh) on very old versions without fragment support.
    """
    frag = getattr(st, "fragment", None) or getattr(st, "experimental_fragment", None)
    if frag is None:
        return func
    return frag(run_every=f"{REFRESH_SECONDS}s")(func)


# ---------------------------------------------------------------------------- #
# Render
# ---------------------------------------------------------------------------- #
st.title("📈 Bluegrey — live telemetry")
st.caption(
    "Read-only view of the engine's Parquet telemetry. Does not connect to IBKR "
    "and cannot affect the live market-data session or strategy."
)

_pinned_run_id, _lookback = _render_sidebar()

# Bind the current control values into the fragment and render it.
_live_view_bound = _auto_refresh(lambda: _live_view(_pinned_run_id, _lookback))
_live_view_bound()