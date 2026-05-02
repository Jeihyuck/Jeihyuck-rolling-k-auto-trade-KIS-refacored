# CLAUDE.md — dual-agent branch

You are working on the **dual-agent** branch.

dual-agent contains:
- Korean stock trading code copied from nullim
- US stock trading code imported from us-agent

---

## Hard rules

- Do not modify nullim.
- Do not modify Korean trading files unless explicitly instructed.
- US trading code must live under `trader/us/**`.
- US tests must live under `tests/us/**`.
- US workflows must be named `.github/workflows/us-*.yml`.
- Korean workflows must not receive US env variables (`TRADING_REGION=US`, `US_AGENT_ENABLED`).
- Do not merge us-agent wholesale.
- KIS secrets are shared. Use existing `KIS_APP_KEY`, `KIS_APP_SECRET`, `KIS_REST_URL`, `CANO`, `ACNT_PRDT_CD`.
- Do not create `KIS_US_*` secrets.
- US DB tables must use `us_` prefix only.
- US code must not access KR `orders`/`positions`/`signals` tables directly.
- KR code must not access `us_` tables directly.

---

## Branch structure

| Branch | Purpose | Modifiable |
|--------|---------|------------|
| `dual-agent` | KR + US integrated trading candidate | ✅ |
| `nullim` | KR stable baseline / rollback | ❌ Never |
| `us-agent` | US experimental origin | Read-only reference |

---

## Forbidden files (KR area — do not touch during US work)

- `trader/pb1_runner.py`
- `trader/kis_wrapper.py`
- `trader/prep_runner.py`
- `trader/trade_am_runner.py`
- `trader/trade_afternoon_runner.py`
- `trader/trade_close_runner.py`
- `settings.py`
- Any existing KR workflow yml
- Any existing KR DB migration

---

## US trading area

```
trader/us/
├── config.py
├── symbols.py
├── market_calendar.py
├── universe.py
├── data_provider.py
├── strategy/
├── execution/
├── runner/
│   ├── dispatcher.py
│   ├── mode_resolver.py
│   ├── prep_runner.py
│   ├── trade_open_runner.py    (legacy alias)
│   ├── trade_mid_runner.py     (legacy alias)
│   ├── trade_close_runner.py
│   └── daily_report_runner.py
├── agents/
└── harness/
```

Workflow modes (production):
- `prep` → `trade-am` → `trade-pm` → `trade-close` → `report`

---

## DB boundary

KR tables: existing domestic tables (orders, positions, signals, pb1_*, ...)
US tables: `us_` prefix only (us_universe, us_watchlist, us_order_intents, us_orders,
           us_fills, us_positions, us_reconcile_logs, us_agent_runs, us_harness_runs, us_failure_events)

Both KR and US share the same `PBCORE_DB_URL`. Table prefix enforces separation.

---

## Shared secrets (no US-specific secrets)

```
KIS_APP_KEY, KIS_APP_SECRET, KIS_REST_URL
CANO, ACNT_PRDT_CD, KIS_ENV
PBCORE_DB_URL
```

---

## Safety constraints

```
ALLOW_REAL_ORDER=0
US_PAPER_TRADING_ENABLED=1
DRY_RUN=1  (default; override via GitHub variable US_AGENT_SCHEDULE_DRY_RUN)
KIS_ENV=practice
DISABLE_REAL_TRADING=1
```

---

## Essential commands

```bash
# US tests
pytest -q tests/us

# US harness (offline all scenarios)
python -m trader.us.harness.runner --scenario all --offline

# US dispatcher dry-run
python -m trader.us.runner.dispatcher --mode prep --env practice --offline

# Trade-AM tick test
US_TRADE_AM_TICK_INTERVAL_SEC=1 US_TRADE_AM_MAX_TICKS=2 \
  python -m trader.us.runner.dispatcher --mode trade-am --env practice --offline \
  --force-now 2026-05-01T10:00:00-04:00
```

---

## Before completion checklist

- [ ] `git diff --name-only` — no forbidden KR files changed
- [ ] `pytest -q tests/us` passes
- [ ] `python -m trader.us.harness.runner --scenario all --offline` passes
