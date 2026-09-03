# Execution lifecycle rollout

This change has no destructive migration. Existing orders, fills, and client
keys remain immutable. Rollback is therefore a code rollback; generated repair
plans may be discarded.

## KR provenance

Export the authoritative KIS positions and persisted fill history into a JSON
object containing `trade_date`, `positions`, and `fills`. Audit first:

```bash
python scripts/repair_kr_position_provenance.py --dry-run --input /secure/kr-provenance.json
```

Generate a non-destructive update plan containing **CONFIRMED rows only**:

```bash
python scripts/repair_kr_position_provenance.py --apply --input /secure/kr-provenance.json \
  --output /secure/kr-provenance-confirmed.json
```

The script never connects to the live database. An operator must review the
plan before applying the listed fields in a transaction. `122630`, mismatched
quantities/cost bases, and missing original entry metadata are excluded.

## Infinite pending audit

Before rollout, export KR `122630` and US `TQQQ` broker orders alongside their
intent dates and cycle identifiers. Classify every row as `FILLED`, `OPEN`,
`CANCELLED`, `REJECTED`, or `UNKNOWN`. Do not delete an `UNKNOWN` row. KR broker
lookup must use the intent's own `trade_date`; an unknown result requires manual
reconciliation. For TQQQ, cancel an expired live BUY at the broker and record it
terminal only after cancellation evidence is returned.

## Partial-profit rollover semantics

An Infinite partial-profit fill does not create a new macro cycle. The existing
macro `cycle_id` is preserved, while the completed profit sleeve starts a new
nested buy round with `buy_round_units_used=0` and the existing 40-unit policy.
That reset is performed exactly once, only after terminal fill evidence and a
fresh authoritative broker residual quantity and average are both available.
Until the residual is authoritative, the rollover remains pending and the
SELL/TP lane stays available; a cached or fallback position must never seed the
round or be labeled `KIS_BALANCE_AUTHORITATIVE`.
