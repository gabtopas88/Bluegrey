"""
strategies/tsmom_baseline.py
S3 TSMOM Study — the two pre-registered BASELINE definitions (charter §4).

These are the bar the ML layer must clear. They are deliberately textbook:

  B1  'sign'  — classic Moskowitz–Ooi–Pedersen time-series momentum:
                position = sign(trailing 12-month return), volatility-scaled.
  B2  'macd'  — the Baz et al. trend battery: three MACD speeds, each
                normalized twice (by price vol, then by its own history),
                pushed through the standard response curve and averaged.
                Produces a CONTINUOUS conviction in ~[-1, 1] rather than ±1.

Shared machinery (identical for both, so comparisons are apples-to-apples):
  * per-asset volatility scaling:  w_a = conviction_a * vol_target / (N_t * sigma_a)
  * ragged universe entry: an asset trades only once it has `min_history`
    valid bars (feature warmup); before that its weight is exactly 0
  * optional point-in-time liquidity eligibility mask (data['eligible'])
  * gross exposure cap
  * an EXECUTION POLICY (Phase 2 addition, charter amendment A4):
      'daily'   — re-target every bar (Phase 1 behaviour)
      'monthly' — re-target every `rebalance_bars` bars on a fixed grid and
                  HOLD weights in between; eligibility loss still flattens an
                  asset immediately (daily risk-only override). This is the
                  literature's rebalance frequency — Phase 1 measured ~10x
                  NAV/yr turnover under daily re-targeting, far above it.
  * a no-trade band on WEIGHTS to suppress churn (the cost model punishes
    small daily rebalances via per-share commission minimums)

SCOPE
  This class implements the RESEARCH interface (generate_signals) only.
  The event/live interface (on_bar) deliberately raises: porting a stateless
  daily-weights strategy to the event path is a Phase 4 deliverable and must
  not be silently "half-done" — a default no-op on_bar would let the event
  backtester run and print a tearsheet for a strategy that never trades.

No fitted model lives here, so every transform is a causal rolling statistic
and the CPCVOptimizer's single-pass generate_signals() call is leak-safe.
"""
from typing import Dict

import numpy as np
import pandas as pd

from strategies.base import BaseStrategy, StrategySignal


class TSMOMBaselineStrategy(BaseStrategy):

    DEFAULTS = {
        'signal': 'sign',            # 'sign' (B1) or 'macd' (B2)
        'lookback': 252,             # B1 momentum window (bars)
        'macd_pairs': ((8, 24), (16, 48), (32, 96)),   # B2 (short, long) EWMA spans
        'vol_span': 60,              # EWMA span for per-asset volatility (bars)
        'vol_target': 0.10,          # annualized target used in the per-asset scaler
        'vol_floor': 0.01,           # annualized floor so cash-like assets can't explode
        'min_history': 252,          # bars of valid data before an asset may trade
        'max_gross': 1.5,            # cap on sum(|w|) — Reg-T-safe design ceiling
        'no_trade_band': 0.0025,     # skip weight changes smaller than 0.25% of NAV
        'rebalance': 'daily',        # 'daily' or 'monthly' (see module docstring)
        'rebalance_bars': 21,        # grid step for 'monthly'
        'exclude': ('BIL',),         # held out of trading (cash reference only)
        'bars_per_year': 252,
    }

    def __init__(self, instruments: dict, params: dict):
        super().__init__(instruments, params)
        p = {**self.DEFAULTS, **(params or {})}
        if p['signal'] not in ('sign', 'macd'):
            raise ValueError(f"signal must be 'sign' or 'macd', got {p['signal']!r}")
        if p['rebalance'] not in ('daily', 'monthly'):
            raise ValueError(f"rebalance must be 'daily' or 'monthly', got {p['rebalance']!r}")
        self.p = p

    # ==========================================
    # ⚙️ LIFECYCLE
    # ==========================================
    def get_warmup_lookback(self) -> int:
        """Longest memory of any transform + one full year of feature warmup."""
        longest_macd = max(l for _, l in self.p['macd_pairs'])
        return int(max(self.p['lookback'], longest_macd, self.p['vol_span'], self.p['min_history']) + 252)

    # ==========================================
    # 🔬 RESEARCH INTERFACE (vector path)
    # ==========================================
    def generate_signals(self, data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        close = data.get('close')
        if close is None:
            raise ValueError("generate_signals requires a 'close' matrix in the data dictionary.")
        close = close.copy()

        # Excluded reference assets never receive a weight (but stay in the
        # column set so the engine's price/signal alignment is untouched).
        excluded = [c for c in self.p['exclude'] if c in close.columns]

        rets = close.pct_change(fill_method=None)
        valid = close.notna()

        # --- Ragged entry: count valid history per asset, gate on min_history ---
        history = valid.cumsum()
        warm = history >= self.p['min_history']

        # --- Per-asset annualized volatility (EWMA), floored ---
        vol = rets.ewm(span=self.p['vol_span'], min_periods=self.p['vol_span']).std()
        vol = vol * np.sqrt(self.p['bars_per_year'])
        vol = vol.clip(lower=self.p['vol_floor'])

        # --- Conviction ---
        if self.p['signal'] == 'sign':
            conviction = self._sign_conviction(close)
        else:
            conviction = self._macd_conviction(close)

        # --- Eligibility: warm, valid price, vol available, liquidity screen, not excluded ---
        eligible = warm & valid & vol.notna()
        if 'eligible' in data and data['eligible'] is not None:
            liq = data['eligible'].reindex(index=close.index, columns=close.columns).fillna(False).astype(bool)
            eligible = eligible & liq
        if excluded:
            eligible.loc[:, excluded] = False

        conviction = conviction.where(eligible, 0.0).fillna(0.0)

        # --- Volatility scaling: w = conviction * vol_target / (N_t * sigma) ---
        n_live = eligible.sum(axis=1).replace(0, np.nan)
        raw_w = conviction.multiply(self.p['vol_target'], axis=0)
        raw_w = raw_w.div(vol).div(n_live, axis=0)
        raw_w = raw_w.where(eligible, 0.0).fillna(0.0)

        # --- Gross cap ---
        gross = raw_w.abs().sum(axis=1)
        scale = np.where(gross > self.p['max_gross'], self.p['max_gross'] / gross, 1.0)
        raw_w = raw_w.multiply(scale, axis=0)

        # --- Execution policy: monthly grid holds targets between rebalance dates ---
        if self.p['rebalance'] == 'monthly':
            step = int(self.p['rebalance_bars'])
            on_grid = np.zeros(len(raw_w), dtype=bool)
            on_grid[::step] = True
            raw_w = raw_w.where(on_grid[:, None], np.nan).ffill().fillna(0.0)
            # Daily risk-only override: an asset that loses eligibility is flattened now.
            raw_w = raw_w.where(eligible, 0.0)

        # --- No-trade band (path-dependent -> small loop, T x N in numpy) ---
        weights = self._apply_no_trade_band(raw_w, eligible, self.p['no_trade_band'])
        return weights

    # ------------------------------------------
    # Conviction builders
    # ------------------------------------------
    def _sign_conviction(self, close: pd.DataFrame) -> pd.DataFrame:
        """B1: sign of the trailing `lookback` return. NaN until enough history."""
        mom = close / close.shift(self.p['lookback']) - 1.0
        return np.sign(mom)

    def _macd_conviction(self, close: pd.DataFrame) -> pd.DataFrame:
        """
        B2: Baz et al. (2015) trend battery.
          x_k = EWMA_S(P) - EWMA_L(P)
          q_k = x_k / rolling_std_63(P)          (scale by price volatility)
          y_k = q_k / rolling_std_252(q_k)       (scale by the score's own history)
          z_k = y_k * exp(-y_k^2 / 4) / 0.89     (response curve, peaks near |y|~1.4)
          conviction = mean_k z_k
        """
        scores = []
        for short, long in self.p['macd_pairs']:
            x = close.ewm(span=short, min_periods=short).mean() - close.ewm(span=long, min_periods=long).mean()
            q = x / close.rolling(63, min_periods=63).std()
            y = q / q.rolling(252, min_periods=252).std()
            z = y * np.exp(-(y ** 2) / 4.0) / 0.89
            scores.append(z)
        return sum(scores) / len(scores)

    # ------------------------------------------
    # No-trade band
    # ------------------------------------------
    @staticmethod
    def _apply_no_trade_band(target: pd.DataFrame, eligible: pd.DataFrame, band: float) -> pd.DataFrame:
        """
        Hold the previous weight unless the target moved by at least `band`
        (fraction of NAV). Ineligible assets are always forced to zero — the
        band must never keep us in a position the screen says we shouldn't hold.
        """
        tgt = target.values
        elig = eligible.values
        out = np.zeros_like(tgt)
        held = np.zeros(tgt.shape[1])
        for t in range(tgt.shape[0]):
            row = tgt[t]
            move = np.abs(row - held) >= band
            held = np.where(move, row, held)
            held = np.where(elig[t], held, 0.0)
            out[t] = held
        return pd.DataFrame(out, index=target.index, columns=target.columns)

    # ==========================================
    # 🏭 PRODUCTION INTERFACE (event path) — Phase 4
    # ==========================================
    def on_bar(self, latest_bars: pd.DataFrame) -> StrategySignal:
        raise NotImplementedError(
            "TSMOMBaselineStrategy.on_bar is a Phase 4 deliverable (event-path port "
            "after the K1-K4 gates). Refusing to run silently as a no-op strategy."
        )