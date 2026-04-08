# Refactor guide: Productive (`live`) vs Experimental (`backtest`)

This guide is the practical playbook for migrating structure without behavior changes.

## Objective

Split code by responsibility while preserving compatibility:

- `src/live/*` for runtime trading flow
- `src/backtest/*` for simulation/research flow
- `src/infra/*` for shared config and persistence

## Branching model (important)

- Integration branch: `Structure_change_ProdExp`
- Working branches: `Structure_change_ProdExp_*`
- Never target `main` during this migration

### Exact git workflow

```bash
# from latest integration branch
git checkout Structure_change_ProdExp
git pull

# create working branch
git checkout -b Structure_change_ProdExp_changes

# commit normally
git add .
git commit -m "Refactor: <scope>"

# push branch
git push -u origin Structure_change_ProdExp_changes

# open PR with:
# base: Structure_change_ProdExp
# compare: Structure_change_ProdExp_changes
```

## Module ownership

### `src/live/`

- `main.py`: runtime orchestration loop
- `data.py`: real-time bars/gap handling
- `risk.py`: runtime risk checks and sizing
- `execution.py`: order placement/fill lifecycle

### `src/backtest/`

- `event_backtester.py`: event-driven backtest engine
- `vector_backtester.py`: vectorized backtest utility
- `optimizer.py`: parameter exploration/optimization

### `src/infra/`

- `config.py`: central config constants/settings
- `store.py`: ArcticDB persistence operations

## Compatibility policy during migration

Keep wrappers in root `src/` temporarily (re-export new module paths) to avoid breaking notebooks and scripts.

Required wrappers:

- `src/main.py` -> `src.live.main`
- `src/data.py` -> `src.live.data`
- `src/risk.py` -> `src.live.risk`
- `src/execution.py` -> `src.live.execution`
- `src/event_backtester.py` -> `src.backtest.event_backtester`
- `src/vector_backtester.py` -> `src.backtest.vector_backtester`
- `src/optimizer.py` -> `src.backtest.optimizer`
- `src/config.py` -> `src.infra.config`
- `src/store.py` -> `src.infra.store`

## PR slicing strategy

Use small PRs to reduce risk:

1. Infra move + wrappers
2. Live move + import updates
3. Backtest move + import updates
4. Tools/notebooks import updates
5. Cleanup PR: remove wrappers after downstream migration

## Validation checklist (must pass)

- Import smoke checks for old and new paths
- Backtest smoke run on short date range
- Live startup smoke run in safe/no-order mode
- Tool script import smoke (`tools/*.py`)

## Definition of done for `Structure_change_ProdExp`

- No behavior changes intended
- Old imports still functional during migration
- New imports documented and used in touched files
- Smoke checks executed and recorded in PR
