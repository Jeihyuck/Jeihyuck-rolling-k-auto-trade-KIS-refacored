-- Exact one-time evidence repair for the TQQQ Infinite stale BUY incident.
--
-- Archived 2026-09-14/15 broker evidence queried the ORIGINAL trade date
-- (2026-09-02) and repeatedly identified the exact TQQQ order as:
--   requested_qty=3 / filled_qty=0 / remaining_qty=3.
-- The KIS cancel endpoint also reported that the original order number no
-- longer exists.  Cancel error text alone is never terminal evidence; this
-- migration additionally requires the exact immutable identity, zero DB fill,
-- and absence of any positive us_fills row.  No fill price is fabricated.

BEGIN;

UPDATE us_orders o
SET status = 'EXPIRED',
    qty_filled = 0,
    meta = COALESCE(o.meta, '{}'::jsonb) || jsonb_build_object(
        'strategy_owner', 'TQQQ_INFINITE',
        'sleeve_id', 'TQQQ_INFINITE',
        'incident_repair_version', 'TQQQ_INF_20260902_ZERO_FILL_V2',
        'incident_evidence', 'ARCHIVED_KIS_ORIGINAL_DATE_QUERY_ZERO_FILL_PLUS_ORIGINAL_ORDER_NOT_FOUND',
        'incident_proven_requested_qty', 3,
        'incident_proven_filled_qty', 0,
        'incident_proven_remaining_qty', 3,
        'incident_repair_terminal_status', 'EXPIRED',
        'incident_fill_price_status', 'NOT_APPLICABLE_ZERO_FILL'
    ),
    updated_at = NOW()
WHERE o.trade_date = DATE '2026-09-02'
  AND o.client_order_key = 'TQQQ_INF_V3:a2e52345-37b9-46de-af62-596ee706a50b:2026-09-02:BUY'
  AND o.symbol = 'TQQQ'
  AND o.side = 'BUY'
  AND o.qty_requested = 3
  AND COALESCE(o.qty_filled, 0) = 0
  AND NULLIF(BTRIM(COALESCE(o.order_no, '')), '') IS NOT NULL
  AND o.status IN (
      'INTENT', 'SUBMITTED', 'ACK', 'OPEN', 'PENDING',
      'PARTIALLY_FILLED', 'RECONCILE_PENDING', 'ACK_DB_FAILED'
  )
  AND NOT EXISTS (
      SELECT 1
      FROM us_fills f
      WHERE f.trade_date = o.trade_date
        AND f.client_order_key = o.client_order_key
        AND f.symbol = 'TQQQ'
        AND f.side = 'BUY'
        AND COALESCE(f.qty, 0) > 0
        AND COALESCE((f.meta->>'accounting_active')::boolean, TRUE)
  );

COMMIT;
