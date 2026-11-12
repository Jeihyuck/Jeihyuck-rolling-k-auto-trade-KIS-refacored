# KR BUY→SELL contract integrity: premerge / deployment / unresolved-evidence plan

Reference: 2026-11-12 operator directive `KR_Buy_to_Sell_Integrity_PR_Directive_20261112.md`.
Base: `dual-agent@00f182ae9ff86a5615763f05f7a7e184a2e87277`.

## Scope of this Draft PR (not a completed historical recovery)

- PB1 signed BUY: keep original KIS intent and frozen entry plan intact. The
  PostgreSQL `0063` guards protect original BUY identity/plan/hash and its
  matching signed SYSTEM position against direct UPDATE/DELETE. ACK, fill
  progression and position dynamic state still change.
- Application: PositionsRepo rejects source-plan/root-hash overwrites.
- After CLOSE: a bounded read-only account/epoch-scoped audit writes
  `runtime/health/kr-contract-audit-YYYY-MM-DD.json`. A DB audit failure is
  an explicit warning; it never blocks a legitimate protective SELL or
  triggers a historical repair or brokerage call.
- The 2026-11-11 ACTIVE_PRACTICE Samsung E&A amendment remains a **separate**
  stored contract, not a rewrite of the v1 BUY plan. Real accounts and
  `KR_INFINITE_V1` execution policy are not changed.

## Initial observed DB baseline — NOT independent KIS execution proof

- Active practice account (redacted): active epoch
  `09772c87-27ab-412d-b1c7-0a4a55196462`.
- Signed PB1 original BUY orders: 18; matching original SYSTEM/imported
  position lifecycle/plan/hash: 13; orphan original lifecycle: 5.
- Excluding both KakaoGames BUY orders, 16 signed PB1 originals remain;
  12 have original-cycle/plan/hash match, and 11 have matching cycle SELLs
  and quantities in the current DB snapshot.
- Independent original KIS proof is required for any historical repair.
  Read-only counts do not demonstrate that no field was ever mutated.

## Per-order historical debt — DO NOT automatically relink

| Trade date | Code / name | Original BUY ODNO / qty | Current finding |
| --- | --- | --- | --- |
| 2026-10-27 | 028050 Samsung E&A | 0000011241 / 33 | SELL33 belongs to IMPORTED cycle; original KIS fill row absent |
| 2026-10-27 | 012750 에스원 | 0000011653 / 21 | SELL21 belongs to IMPORTED cycle; original fill row absent |
| 2026-10-27 | 455890 RISE Money Market Active | 0000011685 / 35 | SELL35 belongs to IMPORTED cycle; original fill row absent |
| 2026-10-28 | 253590 Neosem | 0000029434 / 110 | SELL110 belongs to IMPORTED cycle; original fill row absent |
| 2026-10-29 | 059090 Mico | 0000011865 / 79 | Original SYSTEM cycle retained contract; imported SELL79 is distinct |
| 2026-10-27 | 293490 Kakao Games | 0000011280 / 174 | Only ACK / 0 original priced fills in DB; imported OPEN174; claim UNCERTAIN |

These are historical evidence gaps, not permission to invent fills, change
an entry contract, duplicate P&L, or make a new SELL. Other same-symbol
orders in the epoch belong to **different** cycles. For original KIS evidence:
`python -m scripts.kr_20261109_original_kis_proof` and
`python -m scripts.kr_20261027_293490_readonly_proof`, on the credentialed
practice workstation with `FORCE_HTTP=1 NO_TRADE=1 DRY_RUN=1`.
If KIS originals are inaccessible, report `RECOVERY_UNRESOLVED_ORIGINAL_KIS_PROOF`.

## Premerge checks / migration safety

1. Verify branch HEAD and original `dual-agent` base, open PRs (#288 is old
   owner-specific Draft; #332/#336/#344 already merged).
2. In an **isolated PostgreSQL test DB**: install `0063` through the normal
   SQL splitter and run the test suite; verify signed BUY/position source
   rewrites are rejected, but KIS ACK number, order status, position quantity,
   trailing stop and stage state update normally.
3. Before installing on the actual account, perform read-only audit snapshots
   and compare 18/13/5 and 16/12/11 with explicitly stated scopes/dates.
   Review all existing signed BUY/position rows for any pending legacy repair
   requiring initial backfill, since the trigger blocks an existing signed
   contract from silently changing.
4. Install migration only via approved deployment procedure; migration creates
   functions/triggers but does **not** update/delete a single trade record.
   Check `pg_trigger` and run non-mutating proofs before enabling trade.
5. CLOSE's new audit is best-effort, produces a JSON file and log summary.
   `KR_CONTRACT_AUDIT_AFTER_CLOSE=0` disables only this audit if the DB is
   unavailable, **not** any protective BUY/SELL integrity gate.
6. Backup pre-install schema/trigger definitions. If a reviewed rollback is
   required: DROP TRIGGER `kr_signed_buy_order_guard_0063` on `orders`,
   DROP TRIGGER `kr_signed_position_guard_0063` on `positions`; DROP
   corresponding `public.kr_signed_*_0063()` functions only after the triggers.
   Rollback is a supervised DB change, never done by the trading runner.

## Tests / evidence requirements

- `python -m compileall -q scripts/kr_buy_sell_contract_audit.py trader/db/repos.py tests/kr/test_kr_buy_sell_integrity_20261112.py`
- `pytest -q tests/kr/test_kr_buy_sell_integrity_20261112.py`
- Real PostgreSQL optional integration requires
  `PBCORE_TEST_POSTGRES_URL=<isolated_test_db>`; no production/test URL reuse.
- Existing suites: `pytest -q tests/kr/test_kr_entry_exit_contract_p0_20260918.py tests/kr/test_kr_entry_exit_contract_postgres.py tests/kr/test_pr260_frozen_contract_recovery.py`.
- Full settlement fault-injection, market-session timing, KIS original-day proof,
  notification delivery validation and database migration rehearsal are
  **NOT VERIFIED** by simply creating this PR.

## Not completed by this PR

This is a bounded P0 immutability + detection increment, not a claim that
all instructions are complete. Still required before `MERGE READY`:
- evidence-backed, transactional, owner-scoped historical cycle repair with
  explicit operator approval and rollback; no fabricated historical fills;
- KIS/DB idempotent cross-crash settlement tests for all hard boundaries;
- explicit, append-only and policy-authoritative revision store for every
  approved amendment (current Samsung overlay stays ACTIVE, untouched);
- actual notification-delivery/AM+Close integration and KODEX Infinite
  original policy provenance with versioned adaptive evaluation evidence;
- real PostgreSQL suite, complete CI, and independently verified broker proof.

No automatic merge, deploy, historical Supabase mutation or KIS order.

## 6.1 review amendment (2026-11-12)

1. Account-epoch and owner: active `trading_epochs(account_id, env, trading_epoch_id)`
   must bind uniquely to `portfolio_epochs(account_id, strategy)`; empty identities
   are never matched. Signed PB1 and unsigned/legacy/foreign-owned transactions
   are reported separately, and KR Infinite uses its own order-intent lifecycle.
2. KIS daily-ccld `tot_ccld_qty`/`tot_ccld_amt` is per-order cumulative
   economic evidence, not an independent execution. `filled_at_source=
   ORDER_TIME_PROXY` is never treated as actual execution time. A canceled
   order can have positive filled quantity; orders.qty is a submission limit,
   not an execution. Mixed cumulative and individual evidence is unresolved.
3. BUYs sharing a cycle require cycle-level accounting and explicit allocation
   proof; never allocate the full cycle's sells to every BUY. Current active
   epoch has no repeated signed PB1 BUY cycle, but that historical property
   cannot be coded as an invariant for future pyramid entries.
4. Current KIS practice symbol 012750 is **에스원**, corroborated by
   `public.universe_members` lookup (no proof of a historical specific KIS
   execution merely from this name source). The previous "E1" name was wrong.
5. An after-CLOSE audit gets an attempt-specific run ID, KST trade date, and
   status marker. A timeout/crash must not leave yesterday/today's earlier
   successful audit masquerading as the latest. Existing Mail missing-audit
   cases remain deliverable, not degraded session-execution evidence.
6. Appendix tests must cover cross-account/epoch fill identity, order-number
   absence, native cumulative notional conflict, mixed execution types,
   canceled partial SELL, same-cycle multiple BUY and original contract
   immutability in a PostgreSQL disposable schema.

### Not yet proven by this code change
- Stored KIS daily cumulative economic proof is **not** live requery for
  every historic order, nor proof of individual execution timestamp.
- One DB transaction for both fresh `fills` write and `positions` apply
  in every legacy settlement path is **not** established. The existing
  protected atomic settlement cutover is subject to the current, unchanged
  guarded release gate and not automatically activated by this PR.
- Broker POST cannot be made transactional with PostgreSQL. The original
  durable intent and unresolved-ACK recovery gate must remain authoritative.
- Current KR Infinite adaptive policy remains unchanged; the read-only audit
  enumerates intent and owner state but has not proven all historic evaluations.
- No account changes, no KIS order, no operator DB migration, no deployment.

## Real KR execution path and owner-of-record (6.1 review)

| Boundary | PB1 Pullback/Momentum/Breakout/Swing | KR Infinite 122630 | Preservation rule |
| --- | --- | --- | --- |
| Session owner | `trader/kr/runner/trade_session_runner.py::run_session` → `trader/pb1_runner.py::run_once` | `trader/kr/infinite/runner.py::run_kr_infinite_sleeve_tick` | Intraday owner is not the selected entry style |
| Entry policy | Final30 selected entry style → `trader/trade_plan.py::build_entry_exit_plan` | `trader/kr/infinite/strategy.py` + `InfiniteConfig` | Preserve each owner's policy mathematics |
| Durable intent | `trader/db/repos.py::OrdersRepo`, canonical `_assert_kr_buy_entry_contract` | `trader/kr/infinite/repository.py` dedicated idempotency intent | DB intent before any KIS POST, account/epoch/cycle scoped |
| Broker submit | `trader/kis_wrapper.py` through PB1 risk/submit gates | `trader/kr/infinite/executor.py::submit` | ACK never equals economic fill |
| Broker execution | `trader/reconcile_kis.py::reconcile_today`, `FillsRepo.upsert_fill` | Infinite repository + `trader/kr/infinite/reconciliation.py` | KIS native qty/amount/order/time evidence, same owner |
| Position | `trader/db/repos.py::PositionsRepo.apply_fill` | `kr_infinite_state` per cycle | PB1 root plan freezes; Infinite evaluation state adapts |
| Exit evaluation | `trader/exit_policy/router.py::resolve_exit_policy_for_position`, `trader/kr/us_standard_profit_parity.py` | `trader/kr/infinite/runner.py::evaluate_exit_only` | Preserve original BUY and approved ACTIVE overlay separately |
| SELL and reconciliation | PB1 durable SELL stage, KIS ACK, `reconcile_today` fill and PnL | Dedicated Infinite SELL intent/executor/reconciliation | Avoid duplicate order and double profit attribution |
| Boundary uncertainty | `trader/settlement/kr_cutover.py::load_kr_runtime_release_for_scope` guarded release | Independent Infinite claim and broker observation | The guarded atomic writer is NOT assumed active |

### Native proof and timeline classification

The native practice KIS `daily_ccld` evidence shown in the active
2026-11-12 DB records includes:
- `tot_ccld_qty` and `tot_ccld_amt`, with `KIS_ORDER_CUMULATIVE_EXECUTION`
  and stable `KR_KIS_CUM` watermark identity;
- `filled_at_source=ORDER_TIME_PROXY`, which is **not** the time of any
  particular native trade execution. This limits exact time proof even
  when order cumulative economics are consistent;
- recorded `orders.qty` is requested quantity, NOT reliably filled quantity.

Thus an actual cumulative economic fill may be verified while
`audit.status=REVIEW_REQUIRED` for missing independent execution time
and historical evaluation snapshots. That is not by itself an
IMPLEMENTATION BUG in the active execution path.

### Six-case dry run

Run only on the credentialed local **practice** environment:

```bash
NO_TRADE=1 DRY_RUN=1 DISABLE_REAL_TRADING=1 \
PYTHONPATH=. python -m scripts.kr_buy_sell_historical_recovery_plan_20261112 \
  --env practice --epoch 09772c87-27ab-412d-b1c7-0a4a55196462
```

The six-case plan **does not** use KIS or mutate the DB. All cases remain
`NONE_PROOF_REQUIRED` until exact original-date all-page KIS evidence,
authoritative account, order, amount, time, and current broker holdings
are independently acquired and reviewed. The existing exact KIS proof
utility should be used from the same verified account for that separate
step, but **never** as permission to backfill synthetic fills.
