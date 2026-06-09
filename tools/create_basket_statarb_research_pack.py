from pathlib import Path
import json
import textwrap

ROOT = Path.cwd()

BASE = ROOT / "research" / "strategies" / "cross_sectional_equity_dispersion"
REPORTS = BASE / "reports"
NOTEBOOKS = BASE / "notebooks"
OUTPUTS = BASE / "outputs"

for p in [REPORTS, NOTEBOOKS, OUTPUTS]:
    p.mkdir(parents=True, exist_ok=True)


PDF_TEXT = """
Cross-Sectional Equity Dispersion — Basket StatArb
Plan inicial de research para Bluegrey

Estado:
Research only. No live trading. No order submission. No edge validation yet.

Objetivo:
Investigar una estrategia intradía market-neutral sobre un microuniverso de acciones de semiconductores estadounidenses y un ETF sectorial, inicialmente SMH, con SOXX como alternativa.

Tesis:
Durante los primeros 30 minutos de la sesión regular de Nueva York, de 09:30 a 10:00 ET, pueden aparecer dislocaciones relativas dentro de un sector. Algunas acciones pueden moverse demasiado respecto al ETF sectorial y su beta; otras pueden quedarse rezagadas. La estrategia rankea los residuales a las 10:00, compra underperformers residuales y vende en corto outperformers residuales, construyendo una cesta dollar-neutral y beta-neutral que se cierra antes del cierre.

Fases de trabajo:
Se elimina Fase 0 por decisión del usuario. El research comienza directamente en dataset, notebook exploratorio y backtest reproducible.

Fase 1 — Dataset mínimo:
- SMH 1-minute OHLCV.
- SOXX 1-minute OHLCV opcional.
- 30–50 acciones líquidas de semiconductores.
- Barras de 1 minuto en sesión regular.
- Corporate actions.
- Universo semiconductor.
- Earnings calendar si está disponible.
- Borrow / short availability si está disponible.
- Spreads o proxies de liquidez si están disponibles.

Fase 2 — Notebook exploratorio:
Crear:
research/strategies/cross_sectional_equity_dispersion/notebooks/01_cross_sectional_dispersion_semis_research.ipynb

Orden del notebook:
1. Cargar configuración.
2. Cargar universo.
3. Cargar barras intradía.
4. Validar calidad de datos.
5. Calcular retornos 09:30–10:00.
6. Estimar beta rolling.
7. Calcular residual beta-adjusted.
8. Calcular z-score cross-sectional.
9. Seleccionar long bottom 20% y short top 20%.
10. Construir cesta dollar-neutral y beta-neutral.
11. Simular entrada y salida.
12. Aplicar costes.
13. Calcular métricas.
14. Guardar outputs.
15. Emitir decisión: reject, revise, expand o paper observation candidate.

Fase 3 — Señal inicial:
Para cada acción i y fecha t:

r_stock = P_i,10:00 / P_i,09:30 - 1
r_etf = P_ETF,10:00 / P_ETF,09:30 - 1
beta_i = Cov(r_stock, r_etf) / Var(r_etf), usando solo fechas anteriores.
residual_i = r_stock - beta_i * r_etf
z_i = z-score cross-sectional del residual.

Long leg:
bottom 20% por z-score residual.

Short leg:
top 20% por z-score residual.

Fase 4 — Backtest inicial:
Entrada:
VWAP 10:00–10:02 o siguiente barra.

Salida:
VWAP 15:53–15:55.

Restricciones:
- Sin posiciones overnight.
- Sin ejecución misma barra con lookahead.
- Dollar-neutral.
- Beta-neutral.
- Max 5% por nombre.
- ETF hedge opcional con SMH.
- Mínimo 25 nombres elegibles.
- Mínimo 5 longs y 5 shorts.

Costes:
Probar 1, 2, 5 y 10 bps por lado.

El backtest debe fallar si:
- Solo funciona antes de costes.
- Solo funciona a 1–2 bps.
- El PnL está dominado por earnings.
- El PnL está dominado por pocos días.
- La pata short no es borrowable.
- La beta realizada no es neutral.
- Hay lookahead, survivorship bias o same-bar execution bias.

Fase 5 — Reporte:
Guardar:
- signals.csv
- positions.csv
- trades.csv
- daily_pnl.csv
- metrics.yaml
- cost_sensitivity.csv
- research_memo.md

Métricas principales:
- Total return.
- Daily PnL.
- Sharpe.
- Sortino.
- Max drawdown.
- Hit ratio.
- Turnover.
- Average gross exposure.
- Average net exposure.
- Realized beta exposure.
- Long leg PnL.
- Short leg PnL.
- ETF hedge PnL.
- Cost sensitivity.
- Capacity estimate.
- Performance by volatility regime.
- Performance by liquidity decile.
- Performance excluding earnings days.

Uso del repo:
- Usar research/ para notebooks y memos.
- Usar universes/ para definir el universo semiconductor.
- Usar src/data/DataStore para cargar matrices OHLCV si los datos ya están en ArcticDB.
- Usar src/data/UniverseManager como punto de partida para universos.
- Usar src/backtest/vector_backtester.py para backtest rápido multi-activo.
- No usar src/engine/execution.py en esta fase.
- No usar IBKR ni order routing.
- No usar live engine.

Decisiones permitidas:
- Reject.
- Revise.
- Expand.
- Move to paper observation candidate.

Decisión no permitida:
- Approved for live trading.

Conclusión:
El objetivo inmediato no es demostrar un edge, sino construir un research loop reproducible:
tesis → datos → señal → cartera → simulación → costes → métricas → memo → decisión.
"""


def write_simple_pdf(path: Path, text: str):
    """
    Minimal dependency-free PDF writer.
    Produces a simple text PDF. Not fancy, but portable.
    """
    lines = []
    for paragraph in text.strip().split("\n"):
        if not paragraph.strip():
            lines.append("")
        else:
            lines.extend(textwrap.wrap(paragraph, width=88))

    pages = []
    lines_per_page = 46
    for i in range(0, len(lines), lines_per_page):
        pages.append(lines[i:i + lines_per_page])

    objects = []
    page_ids = []

    def esc(s):
        return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    # 1: catalog, 2: pages
    objects.append("<< /Type /Catalog /Pages 2 0 R >>")
    objects.append(None)

    font_id = 3
    objects.append("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    for page in pages:
        content = ["BT", "/F1 10 Tf", "50 780 Td", "14 TL"]
        for line in page:
            content.append(f"({esc(line)}) Tj")
            content.append("T*")
        content.append("ET")
        stream = "\n".join(content)
        content_obj_id = len(objects) + 1
        objects.append(f"<< /Length {len(stream.encode('latin-1', 'replace'))} >>\nstream\n{stream}\nendstream")

        page_obj_id = len(objects) + 1
        page_ids.append(page_obj_id)
        objects.append(
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            f"/Resources << /Font << /F1 {font_id} 0 R >> >> "
            f"/Contents {content_obj_id} 0 R >>"
        )

    kids = " ".join(f"{pid} 0 R" for pid in page_ids)
    objects[1] = f"<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>"

    pdf = ["%PDF-1.4\n"]
    offsets = [0]
    for idx, obj in enumerate(objects, start=1):
        offsets.append(sum(len(part.encode("latin-1", "replace")) for part in pdf))
        pdf.append(f"{idx} 0 obj\n{obj}\nendobj\n")

    xref_offset = sum(len(part.encode("latin-1", "replace")) for part in pdf)
    pdf.append(f"xref\n0 {len(objects)+1}\n")
    pdf.append("0000000000 65535 f \n")
    for off in offsets[1:]:
        pdf.append(f"{off:010d} 00000 n \n")
    pdf.append(
        f"trailer\n<< /Size {len(objects)+1} /Root 1 0 R >>\n"
        f"startxref\n{xref_offset}\n%%EOF"
    )

    path.write_bytes("".join(pdf).encode("latin-1", "replace"))


def make_notebook():
    cells = []

    def md(text):
        cells.append({
            "cell_type": "markdown",
            "metadata": {},
            "source": textwrap.dedent(text).strip().splitlines(True),
        })

    def code(text):
        cells.append({
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": textwrap.dedent(text).strip().splitlines(True),
        })

    md("""
    # 01 — Cross-Sectional Equity Dispersion Semis Research

    Strategy: **Basket StatArb**

    Status:
    - Research only.
    - No live trading.
    - No order submission.
    - No edge validation yet.

    Initial universe:
    - 30–50 liquid US-listed semiconductor stocks.
    - Primary ETF: SMH.
    - Alternative ETF: SOXX.

    Core idea:
    Compute first-window residual return from 09:30 to 10:00 New York time.
    Buy residual underperformers and short residual outperformers.
    Exit before the close.
    """)

    md("""
    ## 1. Notebook principles

    This notebook must avoid:

    - look-ahead bias,
    - same-bar execution bias,
    - survivorship bias,
    - unrealistic fills,
    - ignoring costs,
    - ignoring short constraints,
    - overfitting.

    Valid final research decisions:

    - reject,
    - revise,
    - expand,
    - paper observation candidate.

    Invalid decision:

    - approved for live trading.
    """)

    code("""
    from pathlib import Path
    import warnings
    import numpy as np
    import pandas as pd

    warnings.filterwarnings("ignore")

    ROOT = Path.cwd()
    STRATEGY_DIR = ROOT / "research" / "strategies" / "cross_sectional_equity_dispersion"
    OUTPUT_DIR = STRATEGY_DIR / "outputs"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("ROOT:", ROOT)
    print("OUTPUT_DIR:", OUTPUT_DIR)
    """)

    md("""
    ## 2. Configuration

    First-pass exploratory config.

    Important:
    - Fase 0 has intentionally been skipped.
    - This notebook assumes the repo already has or will receive intraday data.
    - Any unavailable field must be marked as `information missing`.
    """)

    code("""
    CONFIG = {
        "strategy_name": "cross_sectional_equity_dispersion",
        "config_name": "semis_v1_beta_adjusted_exploratory",
        "research_only": True,
        "live_trading_allowed": False,
        "order_submission_allowed": False,

        "sector": "semiconductors",
        "primary_etf": "SMH",
        "alternative_etf": "SOXX",

        "timezone": "America/New_York",
        "bar_size": "1min",

        "signal_start": "09:30",
        "signal_end": "10:00",
        "signal_time": "10:00",

        "entry_model": "vwap",
        "entry_start": "10:00",
        "entry_end": "10:02",

        "exit_model": "vwap",
        "exit_start": "15:53",
        "exit_end": "15:55",

        "beta_lookback_days": 60,
        "beta_min_obs": 40,

        "long_quantile": 0.20,
        "short_quantile": 0.80,

        "target_gross": 1.0,
        "max_weight_per_name": 0.05,

        "cost_bps_grid": [1, 2, 5, 10],

        "min_price": 5.0,
        "min_adv_usd": 25_000_000,
        "min_eligible_names": 25,
        "min_longs": 5,
        "min_shorts": 5,
    }

    assert CONFIG["research_only"] is True
    assert CONFIG["live_trading_allowed"] is False
    assert CONFIG["order_submission_allowed"] is False

    CONFIG
    """)

    md("""
    ## 3. Initial static semiconductor universe

    This is only an exploratory starter universe.

    Warning:
    If this list is used across history without point-in-time membership, the test is survivorship-biased.
    """)

    code("""
    SEMIS_STARTER_UNIVERSE = [
        "NVDA", "AMD", "AVGO", "INTC", "QCOM", "TXN", "MU", "MRVL",
        "AMAT", "LRCX", "KLAC", "MCHP", "ON", "ADI", "NXPI", "MPWR",
        "SWKS", "QRVO", "TER", "WDC", "STX", "COHR", "LSCC", "CRUS",
        "ACLS", "AEIS", "FORM"
    ]

    ETF = CONFIG["primary_etf"]
    SYMBOLS = sorted(set(SEMIS_STARTER_UNIVERSE + [ETF]))

    print("N symbols including ETF:", len(SYMBOLS))
    SYMBOLS[:10]
    """)

    md("""
    ## 4. Data loading placeholder

    Replace this section with the repo-specific data source.

    Preferred path:
    - Load from `src.data.store.DataStore` if ArcticDB has already been populated.
    - Required matrices: open, high, low, close, volume.
    - Index must be timezone-aware or consistently New York time.
    """)

    code("""
    # Example intended usage if DataStore is ready:
    #
    # from src.data.store import DataStore
    # store = DataStore("intraday_1min")
    # data = store.load(SYMBOLS, start_date="2021-01-01", end_date="2025-12-31")
    #
    # open_px = data["open"]
    # high_px = data["high"]
    # low_px = data["low"]
    # close_px = data["close"]
    # volume = data["volume"]

    data = None

    print("Data loading not executed in template.")
    print("Information missing: confirmed intraday data source and ArcticDB library name.")
    """)

    md("""
    ## 5. Helper functions
    """)

    code("""
    def require_data_loaded(data):
        if data is None:
            raise RuntimeError(
                "Data is not loaded. Connect this notebook to DataStore or another intraday source first."
            )

    def select_time(df: pd.DataFrame, time_str: str) -> pd.DataFrame:
        # Assumes DatetimeIndex.
        return df[df.index.strftime("%H:%M") == time_str]

    def vwap(price: pd.DataFrame, volume: pd.DataFrame, start_time: str, end_time: str) -> pd.DataFrame:
        mask = (
            (price.index.strftime("%H:%M") >= start_time)
            & (price.index.strftime("%H:%M") <= end_time)
        )
        p = price.loc[mask]
        v = volume.loc[mask]
        num = (p * v).groupby(p.index.date).sum()
        den = v.groupby(v.index.date).sum()
        out = num / den.replace(0, np.nan)
        out.index = pd.to_datetime(out.index)
        return out

    def daily_time_price(price: pd.DataFrame, time_str: str, field_name: str) -> pd.DataFrame:
        x = select_time(price, time_str).copy()
        x.index = pd.to_datetime(x.index.date)
        x = x[~x.index.duplicated(keep="last")]
        x.index.name = "date"
        return x
    """)

    md("""
    ## 6. Data quality checks

    Do not inspect PnL before this section is clean.
    """)

    code("""
    def data_quality_report(open_px, close_px, volume):
        report = {}

        report["open_shape"] = open_px.shape
        report["close_shape"] = close_px.shape
        report["volume_shape"] = volume.shape

        report["open_missing_pct"] = float(open_px.isna().mean().mean())
        report["close_missing_pct"] = float(close_px.isna().mean().mean())
        report["volume_missing_pct"] = float(volume.isna().mean().mean())

        report["zero_or_negative_close_count"] = int((close_px <= 0).sum().sum())
        report["zero_volume_count"] = int((volume == 0).sum().sum())

        return pd.Series(report)

    # require_data_loaded(data)
    # dq = data_quality_report(data["open"], data["close"], data["volume"])
    # dq
    """)

    md("""
    ## 7. Signal calculation

    Signal window:
    - 09:30 to 10:00 New York time.

    Signal:
    - stock return from 09:30 to 10:00,
    - ETF return from 09:30 to 10:00,
    - rolling beta using prior dates only,
    - beta-adjusted residual,
    - cross-sectional z-score.
    """)

    code("""
    def compute_signal_window_returns(open_px, close_px, signal_start="09:30", signal_end="10:00"):
        p0 = daily_time_price(open_px, signal_start, "open_0930")
        p1 = daily_time_price(close_px, signal_end, "close_1000")

        common_idx = p0.index.intersection(p1.index)
        common_cols = p0.columns.intersection(p1.columns)

        p0 = p0.loc[common_idx, common_cols]
        p1 = p1.loc[common_idx, common_cols]

        return p1 / p0 - 1

    def estimate_rolling_beta(stock_rets, etf_rets, etf_symbol, lookback=60, min_obs=40):
        etf = etf_rets[etf_symbol]
        betas = pd.DataFrame(index=stock_rets.index, columns=stock_rets.columns, dtype=float)

        for symbol in stock_rets.columns:
            if symbol == etf_symbol:
                betas[symbol] = 1.0
                continue

            cov = stock_rets[symbol].rolling(lookback, min_periods=min_obs).cov(etf)
            var = etf.rolling(lookback, min_periods=min_obs).var()
            beta = cov / var

            # Shift by 1 day to avoid using current-day information.
            betas[symbol] = beta.shift(1)

        return betas

    def robust_zscore_cross_sectional(x):
        med = x.median(axis=1)
        mad = (x.sub(med, axis=0)).abs().median(axis=1)
        denom = 1.4826 * mad.replace(0, np.nan)
        return x.sub(med, axis=0).div(denom, axis=0)
    """)

    code("""
    # require_data_loaded(data)
    #
    # signal_rets = compute_signal_window_returns(
    #     data["open"],
    #     data["close"],
    #     CONFIG["signal_start"],
    #     CONFIG["signal_end"],
    # )
    #
    # etf_rets = signal_rets[[ETF]]
    # stock_rets = signal_rets.drop(columns=[ETF], errors="ignore")
    #
    # betas = estimate_rolling_beta(
    #     signal_rets,
    #     signal_rets[[ETF]].reindex(columns=signal_rets.columns, fill_value=np.nan).assign(**{ETF: signal_rets[ETF]}),
    #     ETF,
    #     CONFIG["beta_lookback_days"],
    #     CONFIG["beta_min_obs"],
    # )
    #
    # residuals = stock_rets.sub(betas[stock_rets.columns].mul(signal_rets[ETF], axis=0))
    # zscores = robust_zscore_cross_sectional(residuals)
    #
    # zscores.tail()
    """)

    md("""
    ## 8. Portfolio construction
    """)

    code("""
    def construct_long_short_weights(zscores, betas, long_q=0.20, short_q=0.80, max_weight=0.05):
        weights = pd.DataFrame(0.0, index=zscores.index, columns=zscores.columns)

        for dt, row in zscores.iterrows():
            valid = row.dropna()
            if len(valid) < CONFIG["min_eligible_names"]:
                continue

            long_cut = valid.quantile(long_q)
            short_cut = valid.quantile(short_q)

            longs = valid[valid <= long_cut].index.tolist()
            shorts = valid[valid >= short_cut].index.tolist()

            if len(longs) < CONFIG["min_longs"] or len(shorts) < CONFIG["min_shorts"]:
                continue

            long_w = min(0.5 / len(longs), max_weight)
            short_w = min(0.5 / len(shorts), max_weight)

            weights.loc[dt, longs] = long_w
            weights.loc[dt, shorts] = -short_w

            # Re-normalize stock basket to dollar-neutral if possible.
            net = weights.loc[dt].sum()
            if abs(net) > 1e-8:
                pos = weights.loc[dt] > 0
                neg = weights.loc[dt] < 0

                long_gross = weights.loc[dt, pos].sum()
                short_gross = -weights.loc[dt, neg].sum()

                if long_gross > 0 and short_gross > 0:
                    weights.loc[dt, pos] *= 0.5 / long_gross
                    weights.loc[dt, neg] *= 0.5 / short_gross

        return weights

    def add_etf_beta_hedge(stock_weights, stock_betas, etf_symbol="SMH"):
        beta_exposure = (stock_weights * stock_betas[stock_weights.columns]).sum(axis=1)
        weights = stock_weights.copy()
        weights[etf_symbol] = -beta_exposure
        return weights
    """)

    md("""
    ## 9. Execution simulation

    Entry:
    - VWAP 10:00–10:02.

    Exit:
    - VWAP 15:53–15:55.

    No same-bar lookahead.
    """)

    code("""
    def simulate_intraday_trade_returns(close_px, volume, symbols, entry_start, entry_end, exit_start, exit_end):
        entry = vwap(close_px[symbols], volume[symbols], entry_start, entry_end)
        exit_ = vwap(close_px[symbols], volume[symbols], exit_start, exit_end)

        common_idx = entry.index.intersection(exit_.index)
        common_cols = entry.columns.intersection(exit_.columns)

        entry = entry.loc[common_idx, common_cols]
        exit_ = exit_.loc[common_idx, common_cols]

        return exit_ / entry - 1, entry, exit_
    """)

    md("""
    ## 10. Cost model
    """)

    code("""
    def apply_fixed_bps_costs(weights, bps_per_side):
        # Approximation:
        # Entry + exit cost based on absolute notional.
        round_trip_bps = 2 * bps_per_side
        gross = weights.abs().sum(axis=1)
        return gross * round_trip_bps / 10_000

    def calculate_daily_pnl(weights, trade_returns, bps_per_side):
        common_idx = weights.index.intersection(trade_returns.index)
        common_cols = weights.columns.intersection(trade_returns.columns)

        w = weights.loc[common_idx, common_cols]
        r = trade_returns.loc[common_idx, common_cols]

        gross_pnl = (w * r).sum(axis=1)
        costs = apply_fixed_bps_costs(w, bps_per_side)
        net_pnl = gross_pnl - costs

        return pd.DataFrame({
            "gross_pnl": gross_pnl,
            "costs": costs,
            "net_pnl": net_pnl,
        })
    """)

    md("""
    ## 11. Metrics
    """)

    code("""
    def max_drawdown(equity):
        peak = equity.cummax()
        dd = equity / peak - 1
        return dd.min()

    def summarize_pnl(pnl):
        daily = pnl["net_pnl"].dropna()
        equity = (1 + daily).cumprod()

        ann_factor = 252
        vol = daily.std()
        sharpe = np.nan if vol == 0 else daily.mean() / vol * np.sqrt(ann_factor)

        downside = daily[daily < 0].std()
        sortino = np.nan if downside == 0 or np.isnan(downside) else daily.mean() / downside * np.sqrt(ann_factor)

        return pd.Series({
            "total_return": equity.iloc[-1] - 1 if len(equity) else np.nan,
            "avg_daily_pnl": daily.mean(),
            "daily_vol": daily.std(),
            "sharpe": sharpe,
            "sortino": sortino,
            "max_drawdown": max_drawdown(equity) if len(equity) else np.nan,
            "hit_ratio": (daily > 0).mean(),
            "avg_cost": pnl["costs"].mean(),
        })
    """)

    md("""
    ## 12. Full exploratory run

    This cell is intentionally commented out until data loading is connected.
    """)

    code("""
    # require_data_loaded(data)
    #
    # signal_rets = compute_signal_window_returns(
    #     data["open"],
    #     data["close"],
    #     CONFIG["signal_start"],
    #     CONFIG["signal_end"],
    # )
    #
    # stock_cols = [c for c in signal_rets.columns if c != ETF]
    # stock_rets = signal_rets[stock_cols]
    #
    # betas_all = estimate_rolling_beta(
    #     signal_rets,
    #     signal_rets,
    #     ETF,
    #     CONFIG["beta_lookback_days"],
    #     CONFIG["beta_min_obs"],
    # )
    #
    # residuals = stock_rets.sub(betas_all[stock_cols].mul(signal_rets[ETF], axis=0))
    # zscores = robust_zscore_cross_sectional(residuals)
    #
    # stock_weights = construct_long_short_weights(
    #     zscores,
    #     betas_all,
    #     CONFIG["long_quantile"],
    #     CONFIG["short_quantile"],
    #     CONFIG["max_weight_per_name"],
    # )
    #
    # # v1: stock basket only
    # weights = stock_weights
    #
    # trade_returns, entry_px, exit_px = simulate_intraday_trade_returns(
    #     data["close"],
    #     data["volume"],
    #     weights.columns.tolist(),
    #     CONFIG["entry_start"],
    #     CONFIG["entry_end"],
    #     CONFIG["exit_start"],
    #     CONFIG["exit_end"],
    # )
    #
    # results = {}
    # for bps in CONFIG["cost_bps_grid"]:
    #     pnl = calculate_daily_pnl(weights, trade_returns, bps)
    #     results[bps] = {
    #         "pnl": pnl,
    #         "metrics": summarize_pnl(pnl),
    #     }
    #
    # pd.DataFrame({bps: results[bps]["metrics"] for bps in results}).T
    """)

    md("""
    ## 13. Save outputs

    Outputs to save once the run is connected:

    - signals.csv
    - positions.csv
    - daily_pnl_by_cost.csv
    - metrics_by_cost.csv
    - research_memo.md
    """)

    code("""
    # Example save pattern:
    #
    # zscores.to_csv(OUTPUT_DIR / "signals_zscores.csv")
    # weights.to_csv(OUTPUT_DIR / "positions_weights.csv")
    #
    # metrics_df = pd.DataFrame({bps: results[bps]["metrics"] for bps in results}).T
    # metrics_df.to_csv(OUTPUT_DIR / "metrics_by_cost.csv")
    #
    # for bps in results:
    #     results[bps]["pnl"].to_csv(OUTPUT_DIR / f"daily_pnl_{bps}bps.csv")
    """)

    md("""
    ## 14. Research decision

    Fill after reviewing metrics and bias checklist.

    Allowed decisions:

    - Reject.
    - Revise.
    - Expand.
    - Move to paper observation candidate.

    Not allowed:

    - Approved for live trading.
    """)

    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.x"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }

    return notebook


pdf_path = REPORTS / "basket_statarb_research_plan.pdf"
notebook_path = NOTEBOOKS / "01_cross_sectional_dispersion_semis_research.ipynb"

write_simple_pdf(pdf_path, PDF_TEXT)
notebook_path.write_text(json.dumps(make_notebook(), indent=2), encoding="utf-8")

print("Created:")
print(pdf_path)
print(notebook_path)