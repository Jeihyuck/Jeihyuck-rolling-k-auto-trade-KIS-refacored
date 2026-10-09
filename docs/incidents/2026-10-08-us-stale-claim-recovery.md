# US October 8 stale-claim recovery gate (MRVL / AMD)

## Verified incident facts

Live Supabase snapshot on 2026-10-09:

| Symbol | Source date | US_STANDARD stage | Broker order number | Qty | Claimed state | Attempt state | blocked conflicts |
|---|---|---|---|---:|---|---|---:|
| MRVL | 2026-10-06 | TP2 | 0000034237 | 2 | IN_FLIGHT | ACKED, non-authoritative | 111 |
| AMD | 2026-10-06 | TP1 | 0000035281 | 1 | IN_FLIGHT | ACKED, non-authoritative | 30 |

Both local `us_orders` rows still show `OPEN` and 0 confirmed fills, and no matching `us_fills` rows when queried. These are the **entire** 141 blocked claim conflicts and 2 unresolved execution actions. The user states the orders were resolved manually through WSL; authoritative KIS evidence has not been independently observed by this PR.

2026-10-08 separately has 13 FILLED SELL rows totalling 42 shares in `us_orders` and 42 shares in `us_fills`; those are not the two legacy unresolved orders.

## Read-only diagnosis

```sql
SELECT o.symbol, o.order_no, o.trade_date, o.status AS local_order_status,
       o.qty_requested, o.qty_filled, c.strategy_owner, c.action,
       c.action_state, c.claim_conflicts, a.attempt_state,
       a.authoritative, a.cumulative_filled_qty
FROM public.us_execution_claims c
JOIN public.us_execution_attempts a ON a.action_key=c.action_key
JOIN public.us_orders o ON o.client_order_key=a.client_order_key
WHERE o.trade_date='2026-10-06' AND o.symbol IN ('MRVL','AMD')
ORDER BY o.symbol;
```

## Recovery requirements

1. Query **practice KIS** by original broker order number and trade date, not symbol alone.
2. Verify order identity, side, status, cumulative filled quantity, remaining open quantity, and cancel/reject evidence (if any).
3. If KIS order query lacks quantities, check execution inquiry/history and conservatively retain the unresolved claim.
4. Pass only authoritative evidence through existing order observation / execution-claim reconciliation APIs; preserve filled portion and residual target.
5. Assert KIS broker order state, `us_orders`, `us_fills`, `us_execution_claims`, `us_execution_attempts` all agree. Do not force an `OPEN` row to `CANCELLED` or `FILLED` with ad-hoc SQL or an elapsed-time heuristic.
6. Recompute recovery health. Require zero unresolved actions in scope and no additional conflicting submits; historical `claim_conflicts=141` may remain as an audit counter and must **not** be reset to 0 as a cosmetic workaround.
7. Do **not** change TP1/TP2/TP3 or TQQQ Infinite policy, or release any claim until broker truth is established.

**No merge-ready declaration until this gate is met or the PR is explicitly narrowed to diagnostic-only.**
