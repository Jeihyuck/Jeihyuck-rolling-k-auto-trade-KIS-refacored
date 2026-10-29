# 2026-10-29 US dual-agent PR rollout / safety gate

Target base: `dual-agent` at `b1936bad6bf8ae6c7f789f6414f93d89298c4a1d`.

## Existing draft PR stack
- #268 Stage4 duplicate verified fill persistence avoidance
- #269 completed PREP recovery
- #270 entry style/trigger identity evidence
- #271 actual new-position count from KIS fills
- #272 frozen entry/exit policy consistency
- #273 Claim mismatch reporting and history audit
- #274 Stage4 phase latency (stacked on #268)
- #275 per-Tick available position limit diagnostics, NOT daily cap
- #276 finite realized PnL reporting
- #277 cross-PR integration draft, **not** operation-verified.

## New independent draft PRs
- #278 5W1H immutable BUY/SELL decision evidence foundation. NO DB, runtime or email wiring yet.
- #279 owner-isolated entry timing shadow evaluator foundation. NO live order permission.
- #280 generic order latency audit aggregator foundation. Existing #274 Stage4 metrics are authoritative until reconciled.
- Current PR: operations acceptance evidence schema and tests.

## Merge order
1. Verify #277 contains the latest approved individual changes; reconcile #274 base stack; do not merge all overlapping branches blindly.
2. Reproduce Oct 28 Close FAILED_RECONCILE and 4 KIS fills / 0 balance-delta confirmations; verify exact account, date, generation and original KIS evidence without inventing balance changes.
3. Wire #278 into decision intent/journal/position/DB/report with schema migration, compatibility and strategy owner tests.
4. Wire #279 into read-only Tick evidence, no new BUY or SELL block. Compare shadow vs original at matching timestamps.
5. Reconcile #280 with #274 before adding instrumentation to protected SELL path. Monotonic phase timings only.
6. Rebase, run complete CI/Postgres/KR regression, then Windows scheduler→WSL→PREP→AM→Stage4→broker GET→Fill→Close→email and protective SELL performance acceptance.

## Forbidden shortcuts
- Never treat a CI PASS as WSL/KIS operations PASS.
- Never mutate old signed v2 contracts, permit TQQQ into US_STANDARD, infer missing evidence as positive, or relax Stage4 stale gates.
- No daily cap policy change: current five available-slot semantics are per-Tick per #277 and must remain without separate policy approval.
- Protect SELL execution if report rendering, explanatory text or analytics fail.
- Maintain separate EXPECTED / IMPLEMENTATION BUG / POLICY REVIEW conclusions.
