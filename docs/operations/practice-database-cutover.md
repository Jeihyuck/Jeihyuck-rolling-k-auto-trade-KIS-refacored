# Practice database generation and cutover

PR137 creates a separate empty PostgreSQL database with the source database's
public schema. It leaves the old database in place. It does **not** create an
independent backup of the old database: the generated JSON manifest contains
identities, counts and schema metadata, not historical rows. An independent
provider snapshot or database backup, with a checked restore procedure, must be
arranged separately if recovery from loss of the source database is required.

## What each result proves

- `READY`: schema-only copy completed; source preserved by this tool and target
  trading state empty. This is not broker approval or a successful trading run.
- `CUTOVER_DB_READY`: current required tables and entry-contract columns exist,
  migration stamps and source/target structure match, target trading state empty.
- `CUTOVER_READY`: the DB checks above and the observed KR/US flat-account and
  pending-order checks passed. This is a point-in-time check, not an atomic lock
  against new orders. Keep all KR/US schedules and other order-producing clients
  stopped until the cutover has completed.
- Live verification requires inspecting the first KR/US practice sessions,
  including BUY intent, broker acknowledgement/fill, persisted entry contract,
  reconciliation and the SELL policy selected from that contract.

## Approval conditions

US balance parsing alone is insufficient. Approval requires complete and
authoritative balance flags, no failed exchanges, coverage and result counts for
NASD/NYSE/AMEX, valid holdings quantities, no holdings and no pending orders.

KR orders are fetched through the final page, forwarding the continuation
cursors and `tr_cont=N`. Continuation errors, repeated cursors, invalid payloads
and the page limit block approval. A final D/E header ends pagination even if
KIS echoes the last cursor. Without that header, cursors must be exhausted.
Explicit remaining quantity takes precedence. Otherwise, filled and confirmed
cancelled quantities are subtracted from ordered quantity. A cancellation request
or revision quantity alone does not prove the order has been cancelled.
Malformed quantity evidence fails closed rather than becoming zero holdings.

Missing KR/US Infinite, profit lifecycle, order-event or other required state
tables fail validation even if migration stamps match. Required entry-contract
columns are checked on both source and target, before declaring either ready.

## CI scope and schema provenance

The PostgreSQL fixture is **current-model based**, not a replay of all historical
migrations and not a dump of the user's actual database. It creates the core
schema from `trader/db/schema.py`; versions 0001–0019 are explicitly recorded as
the fixture baseline. Historical 0019's global UUID conversion conflicts with the
current TEXT-backed identifier/universe model, so it is not re-executed.

Every migration from 0020 onwards is actually executed by the production
migrator. This includes 0033 entry metadata, 0038 US tables, 0047 profit lifecycle,
0048/0049 Infinite tables and 0050 position lifecycle. Versions are stamped only
after successful application by that migrator. No catch-and-stamp fallback is
used. The baseline supplies the column used by the 0032 index before 0033.
Before replay, the empty fixture's ledger payload column is set to TEXT, the
documented input to the one-way 0025 TEXT-to-JSONB migration. 0025 then performs
the real conversion; it is not skipped or stamped without execution.
The US repository's production `_ensure_us_position_risk_state_table` initializer
also runs: that table is created lazily by runtime code, not by a SQL migration.

CI clones this real PostgreSQL source into a separate database, verifies the
sentinel stays only in the source, checks every required trading table is empty,
and verifies required columns. Negative cases remove each required table and
contract column in PostgreSQL; committed source/target defects also exercise the
public verifier. Broker failure scenarios exercise the actual `main()` approval
path with network forbidden. These tests do not certify the user's existing DB,
broker account or first live practice run.

## Cutover and recovery

1. Stop every KR/US scheduler, manual trader and other order producer. Confirm
   there are no in-flight submissions before account reset or DB cutover.
2. Preserve the old DB and record the runtime SHA and connection identity.
   Arrange a separate backup if required; the generation manifest is not one.
3. Prepare the target; reset the KIS practice account separately under operator
   control. Run the cutover verifier while order producers remain stopped.
4. Only after approval, update all runtime DB connection settings consistently.
   Perform a no-order preflight and then inspect the first KR/US sessions.
5. If recovery is needed, stop all order producers again and preserve **both**
   databases and logs. Compare current broker holdings, cash, outstanding orders,
   and fills since the cutover against the candidate recovery DB. Reconcile
   identities, entry contracts and lifecycle state before resuming trading.

**Restoring the old DB URL alone is not a safe rollback after a broker account
reset or any new trade.** The old DB can describe positions the broker no longer
owns and omit orders made on the new DB. Even before new trading, connection
rollback requires confirmation that broker state still agrees with the old DB.
This PR does not automate account reset, connection switching or recovery.
