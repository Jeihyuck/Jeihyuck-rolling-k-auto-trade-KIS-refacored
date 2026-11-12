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
| 2026-10-27 | 012750 E1 | 0000011653 / 21 | SELL21 belongs to IMPORTED cycle; original fill row absent |
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
