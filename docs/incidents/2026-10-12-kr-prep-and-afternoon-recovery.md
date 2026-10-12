# KR PREP recovery after 2026-10-09 KRX holiday (2026-10-12)

## Evidence and root causes

- Supabase `universe_runs`: 2026-10-12 06:30:31 KST, `practice:best_k_meta`, requested `as_of=2026-10-09`, `FAILED`, `provider_empty:emergency_seed_missing`, members 0.
- The KRX was closed on 2026-10-09; the previous **completed** trading date for Monday 2026-10-12 is 2026-10-08.
- `trader.kr.calendar.resolve_kr_expected_as_of` used weekday-only fallback and disagreed with the canonical `trader.time_utils.resolve_prev_krx_trading_day` already used in afternoon verification.
- `data/universe_seed.csv` is ticker-only. The venue-safety change intentionally rejects it without independent market verification; **do not restore the old positional KOSPI/KOSDAQ guess**.
- DB `universe_runs` already contains a 2026-10-08 `SUCCESS` snapshot from `kis_marketcap_top` with 195 **unique** six-digit tickers and 195 valid KOSPI/KOSDAQ venues. This is the strictly dated fallback source, not a fabricated seed.
- A previously successful `as_of=2026-10-08` PREP may belong to trade date **2026-10-09**, not **2026-10-12**. Reusing it without comparing `trade_date` is unsafe.
- The afternoon GitHub Actions workflow used an `OR final30_rows == 30` expression that could bypass a failed PREP provenance check. This PR removes that bypass.

## Changes and invariants

1. KR PREP and PM share the canonical KRX previous-trading-session resolver.
2. KIS and the existing seed providers retain their priority. Only when PREP has **no usable members**, allow an exact-as-of previously successful DB snapshot as an emergency fallback, with all venues explicit and verified, no duplicates, at least 100 members, and only a `kis_marketcap_top` or `seed_static` source.
3. Cache the committed successful snapshot **before** beginning a retry: `start_universe_run` may reuse and temporarily mark that same historical as-of run `RUNNING`. After successful verified recovery, write back a `SUCCESS` snapshot and audit `build_reason=exact_asof_verified_db_recovery`.
4. Duplicate PREP guard requires a completed event for the **same trade date** as well as the same as-of and 30-row quality.
5. The GitHub afternoon path permits new entries **only with same-trade-date PREP evidence** and 30 scored DB Final30 rows. If PREP is not ready, keep the pre-existing **exit-only** path. Never bypass broker reconciliation, risk gates or order idempotency.
6. No strategy scoring, owner, TP/SL rules, Windows Task Scheduler settings or trading hours are changed.

## Remote afternoon recovery procedure (no direct Windows/WSL access)

**Do not treat a merged PR as deployed to the already-running home PC or as proof that PM will auto-start.** This PR does not deploy or start WSL processes.

1. Review PR checks; merge only when regression tests are green. Confirm the `dual-agent` commit and whether the home machine has updated its checkout before PM (requires remote scheduler/runner evidence when WSL cannot be reached).
2. The existing GitHub **Trade Prep (KST 06:20)** workflow supports `workflow_dispatch` on `dual-agent`. Run it **once** only after the duplicate guard confirms no successful PREP for **trade date 2026-10-12**, using `practice`; do not set `ALLOW_DUPLICATE_PREP=1` and do not override `AS_OF_OVERRIDE` to the holiday.
3. Require `expected_as_of=2026-10-08`, `PREP_CORE_OK`, score/quality contract `trade_can_proceed=1`, exactly 30 locked Final30 rows in Supabase for `as_of=2026-10-08`, and a `PREP_DONE` event with `trade_date=2026-10-12`. Inspect source branch/commit, owner and risk evidence.
4. Check that **Trade Afternoon** starts in its normal authorized environment and that `prep_ready=1`. `Final30 rows=30` alone is insufficient. At 13:00–15:10, entries only occur when all existing strategy and risk conditions are satisfied. A missing PREP permits only existing-holdings exit management (if session itself starts).
5. If WSL deployment cannot be verified, do **not** assume the new code is in use by the home scheduler. Do not turn on manual broker orders to compensate for a missing automatic session. GitHub manual afternoon defaults to `compute_only` and is not an automatic live-order substitute.
6. If the exact-session snapshot is unavailable/invalid, or PREP/ledger quality is not verified, **stop recovery and leave BUY blocked**; retain normal SELL management and collect runtime logs.

## Repro / tests

```bash
python -m pytest -q tests/kr/test_kr_prep_holiday_verified_db_recovery_20261012.py
python -m pytest -q tests/kr
```

Safety: no setting of live order flags, no database delete/reset, no stop/restart of an in-flight home process, and no changes to strategy family selection.
