# Strategy 3 — ML-Conditioned Time-Series Momentum (ETF Implementation)
## Full Study Design & Pre-Registration Document

**Project:** Bluegrey_Alpha · **Status:** PRE-REGISTERED — DRAFT v1.0 · **Horizon:** Daily rebalance, multi-week holds · **Capital basis:** $1,000,000 via IBKR

---

## 0. Purpose and pre-registration statement

This document is the research charter for Strategy 3. Everything declared here — the hypotheses, the universe, the frozen feature inventory, the model candidates, the hyperparameter grid, the validation protocol, and above all the kill criteria — is fixed **before any model is fit**. Any deviation during the study must be logged in the Amendments section (§11) with a rationale, and any amendment made *after* seeing results is presumptively data snooping and taints the affected result. The point of this discipline: the ML layer multiplies researcher degrees of freedom, and the only defense that survives contact with a P5_Sharpe number you want to believe is a decision you wrote down before you saw it.

Nothing in this document assumes any particular data subscription. §3 states requirements first and sourcing options second, with the access caveats for each.

---

## 1. Hypotheses

**H1 (Baseline viability).** A classic vol-targeted time-series momentum strategy over a diversified US-listed ETF universe delivers positive net Sharpe (after the full `IBKRFeeModel` cost stack) across CPCV paths spanning 2007–2025, including the 2009–2019 trend drought.

**H2 (ML uplift).** An ML-conditioned position layer — trained walk-forward on a frozen feature inventory, ensembled with the baseline — improves *net* portfolio Sharpe versus the baseline on a per-path paired comparison, and specifically reduces whipsaw bleed in low-trend regimes.

**H3 (Crisis convexity preserved).** The ML-conditioned version retains the property that justifies this sleeve's existence in the book: non-negative (ideally positive) performance in the equity crisis windows present in the sample (2008H2, Feb–Mar 2020, calendar 2022). An ML layer that trades convexity for average Sharpe is a **failure** for this strategy's portfolio role, even if headline Sharpe rises.

Interpretation of nulls: H1 false → the strategy class is dead at this cost structure and universe; stop entirely (K1). H2 false with H1 true → ship the baseline or nothing; the ML layer is dead (K2). H3 false → reject the ML layer regardless of H2 (K3).

---

## 2. Universe specification

Thirty-two US-listed, unleveraged, long-only-construction ETFs across five asset blocks. Leveraged and inverse ETFs are excluded categorically (vol drag and path dependency corrupt the trend signal). Every instrument trades through the existing `STK` pipeline: IBKR SMART routing, `IBKRFeeModel` STK class, existing live engine contract.

| Block | Tickers | Inception (approx.) | Notes |
|---|---|---|---|
| US equity | SPY, QQQ, IWM, DIA, MDY | 1993–1999 | Core trend assets, deepest history |
| Intl equity | EFA, EEM, EWJ, VGK, EWZ, FXI | 1996–2005 | EM adds dispersion |
| Rates & credit | TLT, IEF, SHY, LQD, HYG, TIP, EMB | 2002–2007 | TR adjustment is critical here (§3.3) |
| Commodities | GLD, SLV, USO, UNG, DBC, DBA | 2004–2007 | USO/UNG carry structural roll decay — the model may legitimately learn to short them; that is signal, not bug |
| FX | UUP, FXE, FXY, FXB, FXA, FXC | 2005–2008 | Thinnest ADV of the universe; liquidity screen applies |
| Real assets / other | VNQ, PDBC (alt to DBC), BIL (cash proxy, excluded from trading, used as rate reference) | 2004–2015 | VNQ is equity-correlated; include but cap |

**Panel construction:** ragged entry. Each asset enters the panel at inception + 252 trading days (feature warmup); no backfilling, no index-proxy splicing in v1. Proxy extension (e.g., extending GLD with spot gold) is a declared Phase-2 amendment, not a silent patch. The common-coverage panel effectively begins ~2007–2008, which — usefully — places the GFC, the full 2009–2019 drought, 2020, and 2022 inside the sample.

**Liquidity screen:** minimum $25M median daily dollar volume trailing 63 days, evaluated point-in-time; an asset failing the screen is force-flattened and excluded until it re-qualifies for 21 consecutive days (hysteresis prevents churn at the boundary).

**Effective breadth warning (declared now):** thirty-two tickers in five correlated blocks is roughly 8–12 independent bets. Sharpe confidence intervals will be wide and P5_Sharpe will look humble relative to equity cross-sectional strategies. This sleeve is judged on §7's crisis and correlation metrics as much as on Sharpe. Do not let a modest absolute Sharpe trigger scope creep into exotic assets mid-study.

---

## 3. Data plan

### 3.1 Requirement (subscription-agnostic)

Daily OHLCV bars, **split- and dividend-adjusted (total-return basis)**, for the 32-ETF universe, from inception (or 2005-01-01, whichever is later) through present. Roughly 32 tickers × ~5,000 rows — a trivially small dataset (< 50 MB in ArcticDB). The problem is access and adjustment quality, not volume.

### 3.2 Sourcing options — verify before building

**Option A (default; zero incremental cost): IBKR historical API via `tools/download_history_IBKR.py`.** IBKR serves decades of daily bars for listed ETFs to account holders, and the repo already contains the loader. Required changes: request `whatToShow='ADJUSTED_LAST'` (split **and** dividend adjusted) instead of `'TRADES'` (split-only); daily granularity means pacing limits are a non-issue. Confirm the relevant market-data permissions on the account allow historical requests for US ETFs (typically bundled with US securities market data; verify, don't assume).

**Option B: Polygon Stocks aggregates via the existing `PolygonIngestor`.** Only valid **if** the account's Polygon plan (a) includes the *Stocks* asset class — a Currencies-tier plan does not — and (b) provides sufficient history depth, which is tier-dependent at Polygon. Verify both on the account dashboard before choosing this path. Polygon aggregates are split-adjusted; dividend adjustment must be applied separately from their dividends endpoint.

**Option C: purchase a cheap EOD vendor** (Tiingo, EODHD, Norgate). Only worth it if A fails QA — Norgate in particular becomes relevant later if/when this strategy migrates to futures.

**Decision rule:** attempt Option A first; run the QA battery in §3.4; fall back to B/C only on QA failure. Record the chosen source in the run metadata so every backtest is traceable to its data provenance.

### 3.3 The adjusted-series ingestion trap (infrastructure change required)

`PolygonIngestor._get_smart_start_date` implements append-style smart updates: find the last stored timestamp, download from there, append. **This is correct for unadjusted series and silently wrong for adjusted series**: every new dividend re-bases the *entire historical* series, so an append leaves the stored history stale relative to the newly downloaded tail — a discontinuity at the splice point that manufactures a fake return. Policy for the new library: **full-overwrite re-pull on every ingestion run** (`lib.write`, not `lib.update`). At this data volume a full re-pull costs seconds. Alternative for later: store unadjusted prices plus a separate adjustment-factor series and compose at load time — cleaner, but not needed for v1.

New ArcticDB library: **`etf_daily_adj`**, same OHLCV+volume schema as existing libraries, loaded through the existing `DataStore` contract so `generate_signals({'close': ...})` works unchanged.

**Known approximation (declared):** the backtester will use adjusted close for both signals and P&L, but `IBKRFeeModel` sizes per-share commissions from price, and adjusted prices in the deep past are lower than the prices that actually traded — inflating share counts and hence modeled commissions in early years. The bias is *conservative* (overstates costs) and small at daily turnover; it is accepted for v1 and quantified in the cost-sensitivity run (§7.4).

### 3.4 Data QA battery (runs before any research)

QA notebook checks, each with a hard pass/fail: (1) gap scan — no missing trading days vs. the NYSE calendar beyond known holidays; (2) split-spike detector — any |daily return| > 20% is flagged and manually resolved (unadjusted split leakage shows up here); (3) dividend sanity — bond-ETF adjusted vs. unadjusted cumulative return divergence must be material (if TLT total return ≈ price return, the adjustment silently failed — this is the canary); (4) cross-source spot-check — 3 tickers × 20 random dates compared against a second source, tolerance 10 bps; (5) volume plausibility — no zero-volume days on assets above the liquidity screen.

### 3.5 IBKR account/permission checklist (verify before Phase 4, not before research)

Long ETF positions: covered by the confirmed Stock/ETF permission. **Short ETF positions require a margin account and shortable inventory** — trend will want short TLT/UUP/USO at times; confirm margin permission and typical borrow availability (these are all easy-to-borrow in normal conditions, but `fees.py` models zero borrow cost — a flat borrow haircut sensitivity is included in §7.4). Gross exposure design ceiling: ≤ 150% under Reg-T assumptions; if the account has Portfolio Margin (available above $110K equity), that ceiling relaxes, but the study is run at the conservative ceiling so results don't depend on a permission upgrade.

---

## 4. Baseline definition (the admission gate)

The ML layer must beat the *best* simple baseline, not a strawman. Two baselines are declared; the stronger per-path result is the bar.

**B1 — Classic MOP (Moskowitz–Ooi–Pedersen):**

$$
w^{B1}_{a,t} = \operatorname{sign}\left(r_{a,\,t-252:t}\right)\cdot \frac{\sigma_{tgt}}{N_t\,\hat\sigma_{a,t}}
$$

**B2 — MACD trend battery (Baz et al.):** for speed pairs $(S,L) \in \{(8,24),(16,48),(32,96)\}$, compute $m_{a,t}^{(S,L)} = (\text{EWMA}_S - \text{EWMA}_L)/\text{std}_{63}(P)$, normalize each by its own trailing 252-day std, map through the standard response curve $\phi(x) = x\,e^{-x^2/4}/0.89$, average the three, and vol-scale as in B1 with the averaged score replacing the sign.

Common machinery for both: $\hat\sigma_{a,t}$ = EWMA volatility, span 60 days, annualized; portfolio vol target $\sigma_{tgt} = 10\%$; $N_t$ = number of live (screened-in) assets; daily rebalance through `PortfolioVectorEngine` with `execution_delay=1` (signal on today's close, execution at tomorrow's close — realistic and conservative at daily frequency); no-trade band: skip any weight change with $|\Delta w_{a,t}| < 0.25\%$ of NAV. Costs: `IBKRFeeModel` STK class at `initial_capital=1_000_000` (see §8 precondition), slippage sensitivity at {1, 2, 5} bps.

---

## 5. Frozen feature inventory

Four families, ~22 features per asset per day, all causal rolling transforms computable from the adjusted close series alone. **No features outside this list may be added without an amendment.** Deliberately excluded as overfit bait at this horizon: calendar features (day-of-week, month), asset-identity one-hots (v1 pools without ID to force generalization; asset-*class* ID is a declared variant), and macro data (a vendor dependency this study doesn't need).

**F1 — Normalized momentum (5):** $r_{a,\,t-k:t}\,/\,(\hat\sigma_{a,t}\sqrt{k/252})$ for $k \in \{5, 21, 63, 126, 252\}$.

**F2 — MACD battery (6):** the three normalized MACD scores from B2, both pre- and post-response-curve.

**F3 — Volatility structure (4):** $\hat\sigma_{21}/\hat\sigma_{63}$, $\hat\sigma_{63}/\hat\sigma_{252}$, vol-of-vol (std of 21d vol over 63d, normalized), and current vol percentile rank over the trailing 252 days.

**F4 — Changepoint detection (3):** following Wood et al. (2021): fit a Gaussian-process changepoint model over a trailing 63-day window; emit (a) changepoint *severity* (likelihood-ratio-based score in [0,1]), (b) changepoint *location* normalized to the window, (c) severity × sign of the pre/post trend flip. **Engineering fallback declared now:** if GP-CPD proves too slow for the walk-forward loop, substitute Bayesian online changepoint detection (BOCD) or a normalized CUSUM statistic — the *concept* (how recently and decisively did the trend regime break) is the feature; the estimator is an implementation detail and swapping it is not an amendment.

**F5 — Cross-sectional context (4, declared optional family):** the asset's F1(63d) rank across the live universe, the universe-median F1(63d), dispersion of F1 across the universe, and the fraction of assets in positive trend. This family encodes "is trend working *anywhere* right now" — the drought detector. It is toggled as a single unit in the ablation grid, never cherry-picked feature-by-feature.

Feature matrices are precomputed once and passed through the existing `data: Dict[str, pd.DataFrame]` contract as additional keys alongside `'close'` — the `BaseStrategy` interface already supports this, and it keeps the CPCV loop fast.

---

## 6. Model, training harness, and position mapping

### 6.1 The internal walk-forward contract (binding)

Per the Part-0 architecture finding: `CPCVOptimizer` calls `generate_signals()` once over the full frame, which is leak-safe only if the signal at bar $t$ derives exclusively from information at $t$ and earlier. Therefore the strategy class implements **walk-forward fitting inside `generate_signals()`**: refit every $R = 21$ trading days on an expanding window ending at $t - g$, with purge gap $g = 21$ days ( = label horizon $h$, so no training label overlaps the scoring period). The model scoring any bar was trained only on data that predates it by at least one full label horizon. This is simultaneously the leak defense and the exact production behavior, so the research path and the live path share one training loop — the same philosophy as the parity harness.

### 6.2 Label

Vol-normalized forward return: $y_{a,t} = r_{a,\,t \to t+21}\,/\,(\hat\sigma_{a,t}\sqrt{21/252})$, winsorized at ±5. Regression on this (rather than sign classification) preserves magnitude information for position sizing; vol-normalization makes the label comparable across a universe whose raw vols span an order of magnitude, which is what makes pooling valid.

### 6.3 Model candidates and admission ladder

**M1 — LightGBM regressor** on the frozen features, pooled across assets. Grid (declared, total 8 combinations — deliberately tiny): `max_depth ∈ {2, 3}`, `num_leaves ∈ {7, 15}`, `learning_rate ∈ {0.03, 0.1}`, fixed `feature_fraction=0.7`, `min_child_samples=200`, 400 trees with early stopping on a purged tail of the training window.

**M2 — direct Sharpe-optimizing MLP** (Lim–Zohren DMN-style: 2 hidden layers, ~32 units, dropout 0.3) trained on the cost-aware Sharpe objective:

$$
\theta^{*} = \arg\max_\theta\; \frac{\mathbb{E}[R_p]}{\sqrt{\operatorname{Var}(R_p)}},\qquad
R_{p,t} = \sum_a u_{a,t-1}\frac{\sigma_{tgt}}{N\hat\sigma_{a,t-1}}\, r_{a,t} \;-\; c\sum_a |\Delta u_{a,t}|
$$

with $c$ set from the fee model's measured bps-per-unit-turnover.

**Admission ladder (binding):** M1 runs first. M2 is trained **only if** M1 passes K2 — the more flexible model is a refinement of a demonstrated effect, never a rescue of an absent one. If M1 fails and M2 were then to "succeed," the correct inference is overfitting, not discovery.

### 6.4 Position mapping and the baseline ensemble

Model output maps to a bounded conviction $u_{a,t} = \operatorname{clip}(\hat y_{a,t}/q^{95}_{train},\,-1,\,1)$, where $q^{95}_{train}$ is the 95th percentile of $|\hat y|$ on the training window (no test-period statistics touch the mapping). Final production-candidate weights are the **ensemble**:

$$
w_{a,t} = \left[\alpha\, u_{a,t} + (1-\alpha)\, u^{B}_{a,t}\right]\cdot \frac{\sigma_{tgt}}{N_t\,\hat\sigma_{a,t}}, \qquad \alpha = 0.5
$$

with $u^B$ the best baseline's conviction. $\alpha = 1$ (pure ML) is computed as a *diagnostic only* and is not eligible for deployment in v1 — this is the pre-committed defense against the model learning to fade its own strategy through the drought (§7.3 red-team). Same no-trade band and vol-targeting as the baselines, so every comparison is apples-to-apples.

---

## 7. Validation protocol

### 7.1 CPCV configuration — with a required optimizer fix

`CPCVOptimizer._calibrate_cpcv` sets `n_splits = max(4, total_days/180)`. On an ~18-year daily panel that yields ~36 splits and 12 test blocks — $\binom{36}{12} \approx 1.25\times10^9$ combinatorial paths. The auto-calibration was designed for 2–4-year minute-bar datasets and **must be overridable** for long daily panels. Required change (Phase 0): accept explicit `n_splits` / `n_test_splits` constructor overrides. Study configuration: **`n_splits = 12` (~18-month blocks), `n_test_splits = 2` → 66 paths** — every block pairing tested, each block long enough to contain a full regime. `max_lookback_bars = 273` (252-day feature memory + 21-day label horizon), embargo per the existing 10% rule.

### 7.2 Terminal holdout (hard precondition)

The final 12 months of data are physically excluded from every DataFrame the optimizer, the trainer, and the researcher's notebooks can load — enforced in code (Phase 0 build item), not by discipline. Opened exactly once, on the final production candidate, after all other gates pass. Result recorded regardless of outcome.

### 7.3 Metrics and paired comparison

Because ML and baseline run on identical CPCV paths, H2 is tested as a **paired per-path comparison**: the distribution of $\Delta\text{Sharpe}_{path} = \text{Sharpe}^{ML}_{path} - \text{Sharpe}^{best\,baseline}_{path}$, reporting median, P5, and the fraction of paths with $\Delta > 0$. This is far more powerful than comparing two marginal P5_Sharpe numbers at this breadth. Additional metrics per variant: net P5/median Sharpe, max drawdown distribution, **whipsaw bleed** (annualized return conditioned on the bottom tercile of universe trend-dispersion — the regime where classic TSMOM dies), crisis-window returns (2008H2, 2020-02→2020-04, calendar 2022, computed on paths whose test blocks cover them), annualized turnover, and cost drag as a fraction of gross returns. Deflated Sharpe ratio reported for the winning configuration given the (small, declared) search space: 8 M1 combos × 2 feature-family toggles (F5 on/off) × 2 baselines = 32 effective trials.

Monte Carlo risk-of-ruin runs on the final candidate via `monte_carlo.py` — with the caveat, declared now, that its IID bootstrap destroys the return autocorrelation that *is* a trend strategy's risk profile. A block-bootstrap mode (21-day blocks) is a Phase-0 infrastructure item; both versions are reported.

### 7.4 Sensitivity battery

Final candidate re-run under: slippage {1, 2, 5} bps; a flat 50 bps/yr borrow haircut applied to all short exposure (crude stand-in for the missing borrow model); commission floor doubled; no-trade band halved and doubled; and the early-period adjusted-price commission bias quantified by re-running one path on unadjusted prices for sizing. A candidate whose edge survives only the friendliest corner of this battery is rejected.

---

## 8. Pre-registered kill criteria

**K1 — Baseline viability.** Best baseline net P5_Sharpe ≤ 0 across the 66 paths → the strategy class is dead at this cost structure and universe. Stop. No ML resurrection attempts.

**K2 — ML uplift.** Ensemble beats best baseline (net Sharpe) on < 60% of paths, **or** median $\Delta$Sharpe ≤ 0 → ML layer dead. Decision reverts to shipping the baseline alone (if K1 passed) or nothing.

**K3 — Crisis convexity.** Ensemble materially underperforms the baseline in the crisis windows (aggregate crisis-window return more than 25% worse than baseline's) → reject the ML layer even if K2 passed. The sleeve's job is convexity.

**K4 — Cost discipline.** Cost drag > 35% of gross returns on the median path → one redesign iteration of the no-trade band / rebalance frequency is permitted; a second failure kills the candidate.

**K5 — Holdout.** Holdout net Sharpe < 50% of the CPCV median, or negative → no deployment. No re-opening the holdout with a "fixed" model.

---

## 9. Phased workplan

**Phase 0 — infrastructure preconditions (blocking).** (a) Terminal holdout enforcement in the optimizer/data loader; (b) explicit `n_splits`/`n_test_splits` overrides in `CPCVOptimizer`; (c) `initial_capital` parameterized through optimizer and vector engine, study runs at $1M; (d) `etf_daily_adj` ArcticDB library + full-overwrite ingestion path (Option A loader modification: `ADJUSTED_LAST`); (e) block-bootstrap mode in `monte_carlo.py`. Estimated: 3–5 sessions.

**Phase 1 — data + baselines (`research/nb_s3_01_baseline.ipynb`).** Ingest, run the QA battery (§3.4), implement B1/B2 as a `TSMOMBaselineStrategy(BaseStrategy)` via `generate_signals`, full CPCV at $1M, cost sensitivity. **Gate: K1.** Estimated: 3–4 sessions.

**Phase 2 — features + label study (`nb_s3_02_features.ipynb`).** Build and persist the frozen feature matrices; information-coefficient analysis of each family vs. the label (rank IC, by regime tercile); CPD implementation with the declared fallback path. No portfolio construction in this phase — this is measurement, not selection (the inventory is frozen; IC analysis informs *interpretation*, not feature exclusion). Estimated: 3–4 sessions.

**Phase 3 — ML harness + CPCV (`nb_s3_03_ml.ipynb`).** Walk-forward trainer as a reusable module (this is the artifact Strategies 1–2 inherit); M1 grid across 66 paths; paired ΔSharpe analysis; ablation (F5 on/off); M2 only if K2 passes for M1. **Gates: K2, K3, K4.** Estimated: 4–6 sessions.

**Phase 4 — holdout, Monte Carlo, deployment prep.** Open holdout once (**K5**); block-bootstrap risk-of-ruin; port the final candidate to the event path (`on_bar` implementation, `sync_positions`, pending-transition protocol — largely trivial for a stateless daily-weights strategy); paper deployment in Risk SHADOW mode; parity harness Tiers 1–2 on live paper fills. Broker checklist from §3.5 verified here.

---

## 10. Known approximations & threat log

(1) ETF proxies decouple the strategy from true futures carry — carry features are deliberately absent from v1 rather than badly approximated; futures migration (Databento/Norgate + `FUT` fee class + roll logic) is earned by results, not assumed. (2) Expense ratios are embedded in ETF total returns, so management-fee drag is automatically and correctly accounted for — one of the quiet advantages of the ETF route. (3) Borrow cost on shorts is unmodeled in `fees.py`; §7.4's haircut is a stand-in, and a real borrow term remains on the shared fee-model backlog. (4) Single-vendor price risk mitigated only by the §3.4 cross-check — acceptable at daily granularity. (5) The 2009–2019 drought is one historical instance of the regime that kills trend; CPCV cannot manufacture more of them, and no P5 number here should be read as protection against a longer drought. (6) Structural crowding in trend is real and accepted: this premium is bought for its covariance with the rest of the book, and a crowded risk premium erodes rather than inverts.

## 11. Amendments log

*(empty at pre-registration)*
