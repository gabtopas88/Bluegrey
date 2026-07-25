"""
Bluegrey — read-only run / position diagnostic.

One-off inspector that answers two questions:
  1. In which run (and at what minute) did the position first leave FLAT?
  2. Were any orders / fills ever recorded — and do they match the decisions?

It reuses the dashboard's read-only TelemetryReader, so it NEVER instantiates
TelemetryStore, NEVER connects to IBKR, and only reads the :ro-mounted telemetry
tree. Safe to run while the engine is live.

Run it inside the dashboard container (which already has the read-only telemetry
mount, src on the path, and pandas):

    docker compose -f docker-compose.cloud.yml exec dashboard \
        python dashboard/diagnose_runs.py

All output is plain text to stdout. Nothing is written to disk.
"""
import os
import sys

# Make `from dashboard...` / `from src.telemetry...` importable regardless of CWD.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from datetime import timedelta

import pandas as pd

from dashboard import utils
from dashboard.telemetry_reader import TelemetryReader

POS_LABEL = {1: "LONG SPREAD", -1: "SHORT SPREAD", 0: "FLAT"}


def _z_of(meta_str):
    """Pull the z-score out of a decision's JSON meta, or None during warm-up."""
    z = utils.parse_json_dict(meta_str).get("z")
    try:
        return None if z is None else float(z)
    except (TypeError, ValueError):
        return None


def _fmt_utc(ts):
    t = utils.to_utc(ts)
    return "—" if t is None else t.strftime("%Y-%m-%d %H:%M:%S UTC")


def _z_str(z):
    # In a mixed pandas column, meta z=None is coerced to NaN — treat both as warm-up.
    return "warming up" if pd.isna(z) else f"{z:+.3f}"


def _plabel(pos):
    return POS_LABEL.get(int(pos), "?")


def _for_run(df, run_id):
    """Rows of df for a run_id (empty-safe)."""
    if df is None or df.empty:
        return df
    return df[df["run_id"] == run_id]


def _print_run(s, dec_all, ord_all, fil_all):
    """Print one run's block; return its earliest non-FLAT (ts, run_id, sig, z)."""
    run_id = s.get("run_id", "?")
    d = _for_run(dec_all, run_id)
    o = _for_run(ord_all, run_id)
    f = _for_run(fil_all, run_id)
    earliest_nonflat = None

    print("=" * 78)
    print(
        f"RUN {str(run_id)[:8]}  {s.get('strategy', '?')}  "
        f"started {_fmt_utc(s.get('started_dt'))}  "
        f"risk={s.get('risk_mode', '?')}  mkt_data={s.get('market_data_type', '?')}"
    )
    print(f"  full run_id: {run_id}")

    if d is None or d.empty:
        print("  decisions: (none)")
    else:
        d = d.sort_values("timestamp_utc").copy()
        d["z"] = d["meta"].apply(_z_of)
        # current_pos is int8 in the schema; fillna(0) guards a malformed row.
        d["pos"] = (
            pd.to_numeric(d["current_pos"], errors="coerce").fillna(0).astype(int)
        )

        first, last = d.iloc[0], d.iloc[-1]
        t0 = _fmt_utc(first["timestamp_utc"])
        t1 = _fmt_utc(last["timestamp_utc"])
        print(f"  decisions: {len(d)} rows  [{t0} .. {t1}]")
        print(
            f"    first: pos={_plabel(first['pos']):<12} "
            f"z={_z_str(first['z']):<11} signal={first['signal_type']}"
        )
        print(
            f"    last:  pos={_plabel(last['pos']):<12} "
            f"z={_z_str(last['z']):<11} signal={last['signal_type']}"
        )

        counts = d["signal_type"].value_counts().to_dict()
        joined = ", ".join(f"{k}={v}" for k, v in counts.items())
        print(f"    signal_type counts: {joined}")

        # Position transitions (skip row 0, which is the initial state).
        d["prev_pos"] = d["pos"].shift(1)
        changes = d[d["prev_pos"].notna() & (d["pos"] != d["prev_pos"])]
        if changes.empty:
            held = _plabel(first["pos"])
            print(f"    position changes: none (held {held} throughout)")
        else:
            print("    position changes:")
            for _, r in changes.iterrows():
                qy = utils.fmt_qty(r["held_qty_y"])
                qx = utils.fmt_qty(r["held_qty_x"])
                print(
                    f"      {_fmt_utc(r['timestamp_utc'])}  "
                    f"{_plabel(r['prev_pos'])} -> {_plabel(r['pos'])}  "
                    f"signal={r['signal_type']}  z={_z_str(r['z'])}  "
                    f"qty_y={qy} qty_x={qx}"
                )

        nf = d[d["pos"] != 0]
        if not nf.empty:
            r0 = nf.iloc[0]
            earliest_nonflat = (
                utils.to_utc(r0["timestamp_utc"]), run_id,
                r0["signal_type"], r0["z"],
            )

    n_ord = 0 if o is None or o.empty else len(o)
    print(f"  orders: {n_ord}")
    if n_ord:
        for _, r in o.sort_values("timestamp_utc").iterrows():
            print(
                f"      {_fmt_utc(r['timestamp_utc'])}  {r['signal_type']}  "
                f"{r['symbol']}  {r['action']}  qty={utils.fmt_qty(r['qty'])}  "
                f"type={r['order_type']}"
            )

    n_fil = 0 if f is None or f.empty else len(f)
    print(f"  fills:  {n_fil}")
    if n_fil:
        for _, r in f.sort_values("timestamp_utc").iterrows():
            print(
                f"      {_fmt_utc(r['timestamp_utc'])}  {r['symbol']}  {r['side']}  "
                f"shares={utils.fmt_qty(r['shares'])}  "
                f"price={utils.fmt_price(r['price'])}  "
                f"realized={utils.fmt_money(r['realized_pnl'])}"
            )

    return earliest_nonflat


def main():
    reader = TelemetryReader()
    sessions = reader.list_sessions()
    if not sessions:
        print("No sessions found under", reader.base_path)
        return

    # Bound the reads: from the earliest run start (-1d) to now (+1d).
    starts = [s["started_dt"] for s in sessions if s.get("started_dt") is not None]
    now = pd.Timestamp.now(tz="UTC")
    lo = (min(starts) - timedelta(days=1)) if starts else (now - timedelta(days=30))
    hi = now + timedelta(days=1)

    # Read each stream ONCE across the whole span, then group by run_id in pandas.
    dec_all = reader.read_stream("decisions", lo, hi)
    ord_all = reader.read_stream("orders", lo, hi)
    fil_all = reader.read_stream("fills", lo, hi)

    total_orders = len(ord_all)
    total_fills = len(fil_all)
    first_nonflat = None  # earliest across all runs

    _floor = pd.Timestamp.min.tz_localize("UTC")
    for s in sorted(sessions, key=lambda m: m.get("started_dt") or _floor):
        cand = _print_run(s, dec_all, ord_all, fil_all)
        if cand is not None and (first_nonflat is None or cand[0] < first_nonflat[0]):
            first_nonflat = cand

    print("=" * 78)
    print("SUMMARY")
    if first_nonflat is not None:
        ts, rid, sig, z = first_nonflat
        print(
            f"  Position FIRST became non-FLAT at {_fmt_utc(ts)} "
            f"in run {str(rid)[:8]} (signal={sig}, z={_z_str(z)})."
        )
    else:
        print("  Position never left FLAT in any run.")
    print(f"  Total ORDERS across all runs: {total_orders}")
    print(f"  Total FILLS  across all runs: {total_fills}")
    if first_nonflat is not None and total_fills == 0:
        print(
            "  >> A position was entered in the DECISIONS stream, but NO fills\n"
            "     were ever recorded. Decisions and execution telemetry disagree.\n"
            "     If the transition above carries an ENTER_* signal with no matching\n"
            "     order, look at the order/risk path; if current_pos flips with no\n"
            "     ENTER_* signal, it's a state-machine desync (commit_pending_\n"
            "     transition ran without a real fill)."
        )


if __name__ == "__main__":
    main()