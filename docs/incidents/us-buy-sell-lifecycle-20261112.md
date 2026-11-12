# US BUY → SELL lifecycle integrity — P0 #353

**Source**: 2026-11-12 6.1 integrated directive. Baseline
`dual-agent@00f182ae9ff86a5615763f05f7a7e184a2e87277`
(the merge of #352). Scope is US trading only; KR policy, TQQQ capital
rules, current V2/V3 thresholds and operator-requested ACTIVE flags are
out of scope. This PR is **not** proof of operational KIS readiness.

## Read-only incident evidence (2026-11-12 KST)

- Current active practice trading epoch: uniquely identified in PostgreSQL.
  Do **not** paste account IDs or SHA256(account) into PR logs.
- 2026-11-11 stored positive `us_positions`: ten US_STANDARD symbols
  ABBV, AMGN, DHR, HCA, KO, KR, MCK, PFE, PG, TMO have NULL
  `account_id/portfolio_epoch_id/position_cycle_id` columns; the
  individual V2 policy JSON copies alone are not proof of owner/cycle.
- AMGN on 2026-11-10: 6-share original current-epoch `BUY`, unique
  matching intent/order/KIS_ORDER_CUMULATIVE_ACTUAL fill and immutable
  signed contract. Its daily snapshot retained the cycle and portfolio
  but had NULL account hash column; the 2026-11-11 snapshot lost the
  cycle/portfolio. The carry guard had required the missing column.
- Current TQQQ dedicated epoch cycle: 2-share
  KIS_ORDER_CUMULATIVE_ACTUAL BUY in database; daily KIS balance
  snapshot reports 2, but stored Infinite status is READY. The actual
  current broker GET has **not** been performed in this session.
- Historical unresolved US SELL records: KO/KR older epoch, TMO
  current epoch, plus unscoped legacy KO ACKs. They cannot be declared
  terminal merely because they are old. No manual rewrite or
  force-close was performed.

## Source-of-truth / fields / transaction boundaries

| Stage | Actual code path | Durable source / required guard | On failure |
|---|---|---|---|
| Entry family selected | `us_entry_engine`, `entry_exit_contract.build_us_entry_exit_contract` | Frozen original V2/V3 contract contains original entry provenance, selected style and explicit version | Never reconstruct past contract from today's market settings |
| Pre-POST contract | `order_router.route_order` → `repos.save_order_intent` | `us_order_intents` first durable signed full contract; strategy owner/name/version, client key | No broker POST if persistence/readback fails |
| Claim + attempt | `repos.claim_execution_action`, `append_us_order_event` | Scope-bound execution claim, attempt, SUBMIT_STARTED | Fail closed; UNKNOWN POST is not retry permission |
| Broker | `order_router._mark_broker_submit_boundary` → native KIS | Stage4/lease/risk/time authority at HTTP boundary | PRE_IO vs possibly-submitted must not be conflated |
| Actual economic event | `repos.save_fills` and actual reconcile | `us_fills`, KIS cumulative or execution evidence, exact order identity | No synthetic fill from ACK or empty balance |
| Position snapshot | `repos.save_position_snapshot` | `us_positions` observed quantity, frozen contract *copy* only | A raw balance meta copy may not overwrite proven immutable policy |
| First original/overnight | `original_buy_lineage.verified_new_buy_snapshot_identity`, `verified_carried_us_snapshot_identity` | exact account/epoch/portfolio/cycle + signed source & scoped risk lifecycle | UNVERIFIED; never adopt from symbol or price |
| Current exit | `us_exit_position_resolver.enrich_us_positions_for_exit`, `us_exit_engine`, canonical router | frozen V2/V3, real TP SELL fill quantity, current KIS orderability / pending SELL | Contract conflict and unresolved SELL fail closed by symbol |
| TQQQ | `infinite.repository` + `infinite.integration.run_sleeve` | dedicated cycle/owner/book, actual same-cycle buy fill and authoritative balance, version CAS | READY stays READY without proof; stale state write rejected |

The operational canonical US_STANDARD **BUY-time contract** is the
immutable signed contract stored before POST in the original
`us_order_intents` row, identified by `client_order_key`; order/fill
and `us_positions` JSON are **copies**, not independent contracts.
This is the retained pre-existing durable source, not a new
unreviewed table. Signed SHA256 here is a data-integrity checksum,
**not** a secret-key authorization/signature. Legacy holdings without
a proven original are not declared repaired by this convention.

`trading_epochs.account_id` and lifecycle state store the raw
internal account scope; broker order/fill/snapshot `account_id`
columns use SHA256 of that scope. A column NULL is not the same as a
mismatching hash. Do not bulk rehash historical rows.

## Changes in the PR

1. Allow conservative overnight carrying from the original signed
   prior-day metadata when the *added account hash column alone* is
   NULL, still requiring active account+epoch, exact cycle/portfolio,
   signed original contract, scoped risk state, quantity and no
   ambiguous intervening order.
2. Replace position identity SQL string substitutions with a
   separately authored, scoped UPSERT; reject account/cycle/contract
   conflicts and prevent later unverified broker snapshot metadata
   from discarding a proven same-day original contract.
3. Preserve selected Pullback/Momentum/Breakout/Minervini family
   separately from generic `us_pb1`; freeze provided strategy version
   and never modify an existing holding's signed contract. Reject
   unregistered future owners and conflict ADDs without proven lot
   attribution. No change to TP1/2/3, loss, stop or trailing math.
4. TQQQ READY→ACTIVE requires same-cycle KIS actual BUY and exact net
   shares consistent with authoritative KIS position, not ACK or
   broker-average fallback. Store transitions with real version CAS;
   reject stale READY writes and preserve completed→new cycle rules.
5. Add all-symbol, **read-only** AM/Close PostgreSQL diagnostics:
   `python -m scripts.us_all_symbols_lifecycle_readonly_audit_20261112`
   on the authorized US practice WSL host. This emits no account
   identifiers, performs no repairs, never makes KIS POST and never
   claims fresh KIS origin certification.

## Residual P0 gates — not yet completed

- Dedicated real PostgreSQL cross-day first-fill → partial SELL → TP2,
  10+ simultaneous positions, cross-cycle late-fill tests with real
  migration/catalog and concurrent processes beyond CAS fixture.
- Exact full KIS original-day and current balance/order book GET for
  all live positive positions, including stale KO/KR/TMO SELL
  confirmation. Existing `us_legacy_nine_originals_readonly_certify`
  is available but was **not executed** against the WSL KIS session.
- Full 6.1 sections 2–8 operational single-contract/lot E2E,
  submit-boundary fault injections, current/day-late KIS fills and
  real TP/stop SELL proof. There is no generic future-owner SELL
  adapter in this patch.
- WSL local code pin, scheduler, KIS practice BUY/FILL, next-day
  continuity and condition-triggered SELL/FILL are NOT_VERIFIED.
  CI green is not operational GO.
- No production DB UPDATE/repair, broker order/cancel, history
  backfill or epoch mutation was done by this PR creation.

## Operator-only read-only triage

```sh
KIS_ENV=practice WSL_RUN_MARKET=US NO_TRADE=1 DRY_RUN=1 \
US_ORDER_ARMED=0 DISABLE_REAL_TRADING=1 \
.venv/bin/python -m scripts.us_all_symbols_lifecycle_readonly_audit_20261112 \
  --as-of 2026-11-11 --confirm-practice-readonly
```

The audit intentionally emits `UNVERIFIED` even when DB records are
self-consistent because **fresh broker evidence** has not been
obtained. Follow original-day GET and current orderable balance proof,
then a separately reviewed minimum-scope transactional repair. Never
backfill unsupported historical portfolio/cycle IDs.

## Review decision

- **IMPLEMENTATION BUG**: dropped next-day identity and TQQQ
  READY+actual-fill transition (source code/DB evidence).
- **POLICY REVIEW**: inter-strategy ADD lot allocation absent;
  do not merge unrelated BUY contract families into one cycle.
- **EVIDENCE INSUFFICIENT**: stale KO/KR/TMO originals and any
  broker-time race without live original KIS GET.
- **Merge: HOLD** until full mandated tests pass. No automatic
  WSL rollout or real/observed broker fills were claimed.
