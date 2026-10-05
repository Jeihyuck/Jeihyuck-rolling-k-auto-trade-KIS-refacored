# Stabilization 4/5 — KR/US BUY→SELL contract, lifecycle, and TP-state recovery

Base branch: `stabilize/03-kr-broker-recovery-20261004`.

## Purpose
Ensure every position presented to a SELL engine has the same authoritative lifecycle/entry-contract interpretation regardless of whether it came from DB, KIS balance recovery, reconcile-skip cache, restart, or a later trade date.

This PR must preserve frozen BUY contracts and strategy-owner separation. It is not permission to redefine TP thresholds, stop rules, holding periods, or entry philosophy.

## Context to inspect
Review the actual current paths introduced/changed by PR135–154, especially:
- BUY reason / immutable entry-contract propagation
- trading epoch propagation/recovery
- KR/US position recovery from KIS
- PR154 US `opened_at` / min-hold correction
- TP/profit-capture stage persistence and reconciliation
- runtime-integrity modules/patches that may alter the same fields

Do not assume a prior MERGE READY statement means all runtime paths use the fixed resolver.

## Canonical position-lifecycle input
Before entering any SELL engine, normalize position evidence into an explicit common view containing at least:
- env/account identity
- market
- trading_epoch_id
- strategy_owner / sleeve_id
- position lifecycle or cycle ID
- original BUY strategy/reason/immutable entry-exit contract + version/hash
- authoritative opened_at / opened_trade_date + source
- entry/average-price provenance
- current holding/orderable quantity + broker snapshot source/as-of/completeness
- cumulative actual BUY fills and SELL fills relevant to the current lifecycle
- TP stage actual filled quantities and pending/unresolved quantities
- lifecycle closed/re-entry state

KR/US market-specific adapters may populate the common view differently, but SELL engines must not interpret raw DB/KIS rows differently by path.

## Required invariants
1. DB row `created_at` must never be used as economic first-buy/opened time unless independently proven to represent the confirmed BUY fill.
2. Same lifecycle queried on normal tick, reconcile-skip tick, KIS balance recovery, restart, and next trade date must preserve the same owner/opened_at/frozen contract.
3. Do not attach a historical BUY contract by symbol alone. Require account/env/epoch + owner + lifecycle/cycle evidence.
4. TP stage completion is based on actual broker-confirmed fill quantity, not ACK, requested quantity, or cancel ACK.
5. Partial TP fill must keep the stage partially satisfied/pending for the remaining target as dictated by the execution contract.
6. Add-to-existing BUY must preserve/extend the current lifecycle according to existing policy without silently replacing its frozen exit contract with today's ENV.
7. Full liquidation closes the lifecycle; a later re-entry starts a distinct lifecycle/cycle and must not inherit completed TP flags from the old lifecycle.
8. Evidence-poor imported holdings must retain existing `POLICY_MISSING`/protective behavior; do not fabricate a strategy contract to make tests pass.
9. Epoch filters must not hide broker-held positions that need SELL management; recovered positions must receive the correct current epoch only when evidence/approved recovery policy supports it.
10. TP1 completion must not imply TP2/TP3 completion; each stage tracks actual satisfied quantity independently.
11. Dedicated sleeves (TQQQ Infinite, KR Infinite) use their owner-specific state and must not consume PB1 frozen contracts.

## Required regressions
### US PR154 cross-day min-hold
Reproduce both:
- DB/source-equipped fast path where row timestamps look recent but lifecycle `opened_at` is old.
- reconcile-skip/cached position path.
Expected: SELL soft/trend guard uses authoritative lifecycle timing immediately, without waiting for a later authoritative reconcile.

### TP progression
For both markets where applicable:
- BUY -> next trade day -> TP1 actual partial fill -> TP2 evaluation.
- TP1 ACK but no fill -> TP1 must not be done.
- TP1 partial fill -> partial stage state preserved and no duplicate accounting.
- TP1 done -> TP2 can proceed; TP3 remains independent.

### Add / partial sell / liquidation / re-entry
- Initial BUY creates lifecycle A.
- Add BUY preserves lifecycle A contract according to existing policy.
- Partial SELL reduces qty without losing lifecycle A entry contract.
- Full SELL closes lifecycle A.
- Later BUY creates lifecycle B; no TP/hold state from A leaks into B.

### KIS/imported holding recovery
- High-confidence known lifecycle: recover exact contract/owner/epoch/opened_at.
- Insufficient evidence: preserve protective missing-policy state; do not guess.

## Required production-path tests
Connect actual position loaders/resolvers, SELL engine entry, repository and reconciliation functions; do not test only a new normalization helper.

Test normal DB path, cached/reconcile-skip path, KIS-recovery path, restart path and next-day path for both KR and US as relevant.

## Runtime-integrity cleanup
Inventory date-named `runtime_integrity_*` modules and monkeypatch/install ordering touching lifecycle/contract/position data. Only move a responsibility into a canonical module after proving old and new entry paths in tests. Do not delete patches merely for cleanliness.

## Health/integrity visibility
Expose at least:
- position with missing/ambiguous lifecycle identity
- broker-held position not visible to current epoch SELL management
- frozen-contract hash/version mismatch
- TP stage quantity inconsistency
- closed lifecycle still carrying open/pending state

## Non-goals
- No strategy threshold changes.
- No guessed repair of historical production rows.
- No live trading or session restart.
- No DB reset/delete.
- No PR merge.

## Completion evidence
Report each position source path before/after, exact authoritative field source, failing-before/passing-after tests, and any historical holdings that still require separate evidence-based operational repair.
