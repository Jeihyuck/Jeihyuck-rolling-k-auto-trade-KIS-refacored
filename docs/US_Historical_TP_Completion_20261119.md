# Original-day resolution of claimless historical TP stages

Baseline: merged PR #369, `225deb4999b99f2b76c4aa918e608d7bc96f33d5`.
The PR also preserves the later KR #370 changes in base `b12a37f28c9864eec6858d0ccfddda8e171eb4c2`.

## Unfinished problem

The historical journal replay enumerates active execution claims. KO's original
2026-11-11 TP2 and KR's original 2026-11-04 TP2 have no execution attempt, event,
order or fill under their original client keys. They therefore never entered the
original-day KIS query path. PR #369 added future pre-claim abort certificates,
but cannot retrospectively produce those certificates for existing records.

This change enumerates the original TP stage ledger directly. It does not invent
execution claims, submit attempts, BUY provenance, broker orders, fills or cycles.

## Complete proof required for a derived TP release

`historical_tp_resolution.py` runs only at `AM_START`/`AM` and `CLOSE`, with a
bounded source-query budget. All general symbols use the same implementation;
TQQQ is excluded as a repair target. Original dates are memoized only within one
boundary. Source-date rotation prevents one unavailable old day from monopolizing
every AM/Close budget. No historical recovery is added to tick/afternoon workers.

A release requires all of the following:

1. The exact original source stage, key, cycle, account's ACTIVE practice epoch,
   zero filled quantity, no broker number, and a recorded early-router failure.
2. A fresh authenticated original-day KIS query with **all symbols, sides,
   statuses and exchanges**, completed pagination and source-date validation.
   Previous successful query metadata is cleared before every new query.
3. At least one independently recorded **native DB execution from that original
   day** also appears in the KIS result with matching order number, symbol, side,
   original requested quantity and positive cumulative fills. This is a positive
   witness that the historical day is accessible. A blank result, a retained
   authentication object, or a requested date alone is never absence proof.
   Legacy witnesses with NULL account columns require the independently verified
   account/ACTIVE-epoch relation and authenticated broker match. A witness is not
   retroactively assigned new ownership. A TQQQ row may witness account history;
   its strategy/state is never changed or adopted by a standard-symbol repair.
4. **No unattributed SELL for the target symbol exists in that complete source-day
   book.** The only exclusion is an exactly identified **completed prior TP**:
   same native broker number, original day/symbol/cycle/ACTIVE epoch, exact prior
   stage key (including retry suffix), DONE stage with matching filled quantity,
   FILLED local order and KIS order, plus actual nonsynthetic DB fill provenance.
   Both raw and canonical stage broker numbers must agree. Conflicting native
   order ownership, current/later-stage keys, unknown, partial, open, cancelled or
   rejected SELLs remain fenced. Symbol+quantity alone never attributes a SELL.
5. No native attempt/event/order/fill under the original key and no related
   active execution claim or pending SELL. An unrelated TMO claim/order does not
   globally suppress KO/KR. Read-only preview and apply use the same guards.
6. Revalidation after the GET under row locks: unchanged original TP state,
   ACTIVE scope, original intent identity and native witness. Stage/intent
   changes and an immutable **observation**, dated now with the original source
   trade date, commit together. An audit/state failure rolls back that symbol.

The evidence is named `KIS_ORIGINAL_DAY_NO_ACCEPTED_SELL` when the target has no
SELLs, or `KIS_ORIGINAL_DAY_ONLY_SETTLED_PRIOR_TP_SELLS` when every target SELL is
exactly attributed to completed earlier stages. Neither proves that an HTTP call
was never attempted. Neither is labelled `BROKER_SUBMIT_ABORTED_PRE_IO`.
Only the old derived TP reservation/intent can be closed. No existing order,
fill, execution claim, position quantity, original contract or cycle is modified.

The ordinary TP engine then reevaluates its current policy and may create a fresh
retry key. Existing source-contract, risk, execution-claim, broker balance and
native-submit fences still run. This repair itself has no order/cancel authority.

## Operational evidence and verification

`get_original_day_order_snapshot` binds actual per-query completeness metadata to
an immutable normalized book digest. The report separates claim recovery debt,
claimless TP debt, source-day failures and current-day integrity. Unknown old TP
stages stay warnings/debts, never fake successful reconciliation or executions.

`tests/us/test_us_historical_tp_completion_20261119.py` covers every general symbol,
KO/KR source dates, empty/partial/stale/wrong-account/foreign-epoch queries,
positive historical witnesses, Infinite isolation, actual retry-intent generation,
atomic rollback/restart, concurrent native evidence and unrelated-symbol isolation.
PostgreSQL cases use production migrations in a disposable service.

The read-only operator/CI command is:

```bash
PYTHONPATH=. python scripts/us_historical_tp_resolution.py --all-source-days --budget-sec 30
```

It forces order permissions OFF, exposes a GET-only provider facade, and sets the
PostgreSQL session default to read-only before engine construction. It also
forces `SET TRANSACTION READ ONLY` and verifies `SHOW transaction_read_only`
on every transaction, independently of engine/pooler option overrides. Output omits
credentials/raw accounts/broker order numbers and explicitly says
`operational_database_repaired=false`. KIS-positive target SELL observations
include status/quantities and a number digest for diagnostic comparison.

The dedicated workflow first runs offline/PostgreSQL contracts without secrets.
A **single ready-for-review transition** on this repository owner's exact trusted
branch can then run the authenticated read-only audit. It checks out the exact
head SHA, has only `contents: read`, does not run on forks/schedules or every push,
forces the practice endpoint and all order gates OFF, and never applies repairs.
The old PR243 manual audit workflow/security gates are unchanged.

The read-only audit enumerates actual source dates and grants each date its own
bounded evidence budget (maximum ten source days; excess is explicitly unresolved).
This prevents an operator audit from omitting KO after other old days consume a
production boundary budget. The production AM/Close total budget is unchanged.

The first authenticated audit on 2026-11-19 confirmed KR and PG eligible, but KO
was budget-deferred. HCA had two real FILLED SELLs. Read-only DB comparison tied
their native order numbers and keys to DONE TP1/TP2, while the candidate is TP3.
The final implementation verifies this exact prior-stage distinction generically;
the first audit's failure is retained as evidence, not relabelled as a pass.

**Code CI success and original KIS evidence success are separate.** Until the real
original-day audit succeeds, KO/KR's operational resolution remains unverified.
An eligible read-only result is also not a claim that the WSL process deployed
this code or that the operational DB was already repaired. Merge/deployment is
not reported as complete merely because deterministic tests pass.
