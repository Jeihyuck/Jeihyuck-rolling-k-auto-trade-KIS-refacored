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
- 어떤 종목명도 코드에 하드코딩하지 말 것.

  절대 금지:
  - AMD, QCOM, MU, DELL, AMZN 등 특정 종목명을 코드 조건문에 직접 넣지 말 것.
  - 특정 종목을 예외 허용하거나 예외 차단하는 방식으로 수정하지 말 것.
  - 테스트를 통과시키기 위해 특정 ticker symbol list를 코드 내부에 박아 넣지 말 것.
  - PNL, risk gate, entry engine, exit engine, reconcile, sold_today, pending_order 처리에서 특정 symbol을 특별취급하지 말 것.

  허용되는 방식:
  - BUY 허용 universe는 prep에서 생성·저장된 locked watchlist를 기준으로 동적으로 판단할 것.
  - SELL 허용 여부는 KIS balance 또는 us_positions의 현재 보유 포지션을 기준으로 동적으로 판단할 것.
  - sold_today_symbols는 us_fills 또는 KIS 체결조회에서 당일 실제 SELL 체결된 symbol을 기준으로 동적으로 산출할 것.
  - PNL positions는 KIS balance 또는 DB snapshot에서 조회된 symbol을 기준으로 동적으로 계산할 것.
  - 테스트에서도 특정 종목명 자체가 아니라 "locked watchlist에 포함된 임의 symbol", "현재 보유 중인 임의 symbol"이라는 계약을 검증할 것.

  오늘 로그에 등장한 AMD/QCOM/MU/DELL/AMZN 등은 원인 설명용 예시일 뿐이며, 구현 코드의 예외 조건으로 사용하면 안 된다.

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

## Completion criteria (US trading contracts)

16. 어떤 BUY symbol이든 locked watchlist에 있으면 symbol_not_in_universe 또는 symbol_not_in_locked_watchlist로 막히지 않는다.
17. 어떤 BUY symbol이든 locked watchlist에 없으면 entry intent 자체가 생성되지 않거나, route 전 contract check에서 BUY intent에서 제거된다.
18. 어떤 SELL symbol이든 현재 KIS balance/us_positions 보유 포지션에 있으면 static universe 밖이라는 이유만으로 차단되지 않는다.
19. 어떤 ACK SELL 주문이든 특정 종목명 하드코딩 없이 KIS fill 또는 balance 변화 기준으로 pending/fill/balance 상태가 reconcile된다.
20. sold_today_symbols는 특정 종목명 조건이 아니라 당일 실제 SELL fill 데이터 기준으로만 산출된다.

## Test contract guidelines

테스트에서 AMD/QCOM/MU/DELL 같은 실제 ticker를 사용하더라도, 이는 fixture 이름일 뿐이어야 한다.
테스트 assertion은 특정 종목명 예외가 아니라 다음 계약을 검증해야 한다.

1. locked_watchlist_symbols 안에 있는 임의 BUY symbol은 risk gate를 통과한다.
2. locked_watchlist_symbols 밖에 있는 임의 BUY symbol은 차단된다.
3. current_position_symbols 안에 있는 임의 SELL symbol은 risk gate를 통과한다.
4. current_position_symbols 밖에 있는 SELL symbol은 보유수량 부족 또는 포지션 없음으로 차단된다.
5. reconcile은 특정 ticker가 아니라 order_no, client_order_key, side, qty, balance 변화 기준으로 동작한다.
