# US PR #285 consolidated rollout — PR #255 coverage and remaining proofs

## Scope of one merge candidate

The PR #285 integrated branch contains US corrective changes #268–#276
(consolidated by #277), #278–#281 (consolidated by #282), #284, practice
entry-timing ENFORCE configuration, and the **nonduplicative** #255 follow-on
changes enumerated here. #277, #282 and #285 are overlapping *integration*
branches, not three additional changesets to merge. Never merge them all.

Older historical draft PRs (#231, #237 and earlier) are NOT blanket
cherry-picked over the current dual-agent code; each old patch needs an
evidence-based supersession/diff review before inclusion. No KR strategy
or owner policy is implicitly changed by this US integration.

## #255 integration against the current Stage4 design

1. The original #255 bounded first-Tick wait is already superseded by
   `trader/us/execution/stage4_lifecycle.py::wait_for_stage4_ready` and the
   supervised PAUSED -> recovery or verified child restart paths in the
   current `trade_session_runner`. Retain the current generation-scoped
   handoff; do not install the old competing wait loop.
2. An actual missing startup marker was integrated. The Stage4 worker now
   publishes a NON-AUTHORIZING `STARTING` generation before initial KIS
   provider construction. This is NOT a successful fill/reconcile.
3. The original 0059 production PostgreSQL CHECK forbids `STARTING`.
   `migrations/0060_us_stage4_starting_health.sql` widens only that
   state CHECK. **Apply migration 0060 before enabling WORKER**; never
   enable new Stage4 code against an unupgraded schema.
4. The original #255 mail patch guessed log paths; #285's older mail
   collection likewise guessed a session-label prefix which may differ from
   `safe_scope_token(session_run_id)`. The corrected script follows
   exactly referenced and basename-validated same-day child log paths.
   Missing files cause DEGRADED archive evidence, not lost mail.
5. Both the primary archive redactor and its fallback mask embedded
   SQL/HTTP userinfo in Stage4 traceback logs.
6. Full code regression: actual PostgreSQL migration and STARTING ->
   read_worker_health NOT_READY -> READY, exact log archive evidence,
   credential redaction, WSL shell and US strategy-owner regression.

## Not automatic or accomplished by the PR

- No KIS practice native POST, ACK or filled order is executed by CI.
- No WSL local deployment, Windows schedule update, secrets or active
  epoch migration has been performed.
- No production Supabase migration has been applied by this PR.
- No historical Claim rows are removed or relabeled.
- 2026-10-28 Close failure is not automatically resolved by a later
  read-only `us_reconcile_logs.status=OK`.
- The Stage4 writer's 20 real consecutive READY intervals and protective
  SELL p95/p99 are still operational evidence blockers.
- The price-aware ENTRY policy is trial ON in practice; experimental
  thresholds are POLICY REVIEW, not proven alpha. TQQQ Infinite, VCP,
  SWING_CARRY management and protection SELL remain owner-isolated.

## Merge gate

Latest exact #285 HEAD full CI + real PostgreSQL migration test PASS
are required for **CODE PASS**. Operational authorization also requires
schema 0060 deployed, KIS account+epoch+generation original evidence,
matching Stage4 READY continuity, genuine protected SELL timing, KIS
ACK/fill/reconcile, original-date Close proof, WSL scheduler/pin and
report-email completeness. CI alone never qualifies as OPERATION PASS.

**No merge.** Keep #285 Draft until the outstanding evidence is obtained.
