# Bluegrey — Parked Items / ToDo

A holding pen for good ideas that aren't today's work. One line each, dated,
with just enough context that future-us knows why it's here. Check items off
rather than deleting them, so the trail stays visible.

## Infrastructure

- [ ] 2026-08-14 — **Move `initial_capital` into config as the single source of truth.**
      Currently set to 1M by hand in four files (optimizer, monte_carlo,
      vector_backtester, event_backtester). Works fine, but one config value
      would mean one place to change and zero risk of the files drifting apart.
      Deferred deliberately — do during a quiet moment, not mid-study.

- [ ] 2026-08-14 — **Risk module placeholder account balance.**
      The live risk module carries a placeholder equity number that gets
      overwritten with the real account balance at runtime. Harmless for
      research; verify the wiring properly at deployment (S3 Phase 4).

- [ ] 2026-08-14 — **Borrow-cost modeling for short positions.**
      The fee model currently assumes shorting is free. It isn't. The S3 study
      papers over this with a flat sensitivity haircut; a real borrow-cost term
      is needed before any strategy that leans heavily on shorts (S1/S2 small-caps
      especially).

- [ ] 2026-08-14 — **Futures upgrade path.**
      If S3 earns real capital, the "proper" version trades futures instead of
      ETFs: needs a futures fee class, contract-roll handling, and a futures
      data vendor. Explicitly out of scope for v1 — results first.

## Data

- [X] 2026-08-14 — **Resolve short adjusted history on IBKR (TLT and possibly others).**
      In progress: v2 diagnostic distinguishes whether the raw history or only
      the adjusted history is short. Outcome decides free-vs-paid data sourcing
      for the S3 universe.

## Deployment (S3 Phase 4 checklist — not before)

- [ ] 2026-08-14 — **Verify margin permission for shorting ETFs** (long positions
      are covered by the confirmed Stock/ETF permission; shorts are not).