# Cross-Sectional Equity Dispersion — Basket StatArb

## Plan inicial de research para Bluegrey

### Estado

**Research only.**
No live trading. No order submission. No edge validation yet.

---

## Objetivo

Investigar una estrategia intradía **market-neutral** sobre un microuniverso de acciones de semiconductores estadounidenses y un ETF sectorial, inicialmente **SMH**, con **SOXX** como alternativa.

---

## Tesis

Durante los primeros 30 minutos de la sesión regular de Nueva York, de **09:30 a 10:00 ET**, pueden aparecer dislocaciones relativas dentro de un sector.

Algunas acciones pueden moverse demasiado respecto al ETF sectorial y su beta; otras pueden quedarse rezagadas.

La estrategia rankea los residuales a las **10:00**, compra *underperformers* residuales y vende en corto *outperformers* residuales, construyendo una cesta **dollar-neutral** y **beta-neutral** que se cierra antes del cierre.

---

## Fases de trabajo

Se elimina **Fase 0** por decisión del usuario.

El research comienza directamente en:

* Dataset.
* Notebook exploratorio.
* Backtest reproducible.

---

# Fase 1 — Dataset mínimo

El dataset mínimo deberá incluir:

* SMH 1-minute OHLCV.
* SOXX 1-minute OHLCV opcional.
* 30–50 acciones líquidas de semiconductores.
* Barras de 1 minuto en sesión regular.
* Corporate actions.
* Universo semiconductor.
* Earnings calendar, si está disponible.
* Borrow / short availability, si está disponible.
* Spreads o proxies de liquidez, si están disponibles.

---

# Fase 2 — Notebook exploratorio

Crear el siguiente notebook:

```text
research/strategies/cross_sectional_equity_dispersion/notebooks/01_cross_sectional_dispersion_semis_research.ipynb
```

## Orden del notebook

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
15. Emitir decisión: `reject`, `revise`, `expand` o `paper observation candidate`.

---

# Fase 3 — Señal inicial

Para cada acción `i` y fecha `t`:

```text
r_stock = P_i,10:00 / P_i,09:30 - 1

r_etf = P_ETF,10:00 / P_ETF,09:30 - 1

beta_i = Cov(r_stock, r_etf) / Var(r_etf)
```

La beta debe estimarse usando solo fechas anteriores.

```text
residual_i = r_stock - beta_i * r_etf

z_i = z-score cross-sectional del residual
```

## Long leg

Bottom 20% por z-score residual.

## Short leg

Top 20% por z-score residual.

---

# Fase 4 — Backtest inicial

## Entrada

VWAP **10:00–10:02** o siguiente barra.

## Salida

VWAP **15:53–15:55**.

## Restricciones

* Sin posiciones overnight.
* Sin ejecución misma barra con lookahead.
* Dollar-neutral.
* Beta-neutral.
* Max 5% por nombre.
* ETF hedge opcional con SMH.
* Mínimo 25 nombres elegibles.
* Mínimo 5 longs y 5 shorts.

## Costes

Probar:

* 1 bps por lado.
* 2 bps por lado.
* 5 bps por lado.
* 10 bps por lado.

## El backtest debe fallar si

* Solo funciona antes de costes.
* Solo funciona a 1–2 bps.
* El PnL está dominado por earnings.
* El PnL está dominado por pocos días.
* La pata short no es borrowable.
* La beta realizada no es neutral.
* Hay lookahead, survivorship bias o same-bar execution bias.

---

# Fase 5 — Reporte

Guardar:

```text
signals.csv
positions.csv
trades.csv
daily_pnl.csv
metrics.yaml
cost_sensitivity.csv
research_memo.md
```

## Métricas principales

* Total return.
* Daily PnL.
* Sharpe.
* Sortino.
* Max drawdown.
* Hit ratio.
* Turnover.
* Average gross exposure.
* Average net exposure.
* Realized beta exposure.
* Long leg PnL.
* Short leg PnL.
* ETF hedge PnL.
* Cost sensitivity.
* Capacity estimate.
* Performance by volatility regime.
* Performance by liquidity decile.
* Performance excluding earnings days.

---

# Uso del repo

* Usar `research/` para notebooks y memos.
* Usar `universes/` para definir el universo semiconductor.
* Usar `src/data/DataStore` para cargar matrices OHLCV si los datos ya están en ArcticDB.
* Usar `src/data/UniverseManager` como punto de partida para universos.
* Usar `src/backtest/vector_backtester.py` para backtest rápido multi-activo.
* No usar `src/engine/execution.py` en esta fase.
* No usar IBKR ni order routing.
* No usar live engine.

---

# Decisiones permitidas

* `Reject`.
* `Revise`.
* `Expand`.
* `Move to paper observation candidate`.

## Decisión no permitida

* `Approved for live trading`.

---

# Conclusión

El objetivo inmediato no es demostrar un edge, sino construir un research loop reproducible:

```text
tesis → datos → señal → cartera → simulación → costes → métricas → memo → decisión
```
