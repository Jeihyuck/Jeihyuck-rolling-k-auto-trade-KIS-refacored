-- Exact one-time evidence repair for the TQQQ Infinite stale BUY proven by
-- archived 2026-09-14/15 production logs.
--
-- Archived broker evidence repeatedly showed, for original trade date 2026-09-02:
--   * client key TQQQ_INF_V3:a2e52345-37b9-46de-af62-596ee706a50b:2026-09-02:BUY
--   * symbol TQQQ / BUY
--   * requested_qty=3
--   * filled_qty=0
--   * remaining_qty=3
-- The later cancel endpoint returned "original order does not exist" (and at
-- times HTTP 500), while a fresh authoritative KIS balance showed the existing
-- 27-share cycle holding.  The cancel error alone is NOT used as terminal
-- evidence.  This repair requires the exact immutable identity plus zero fill
-- and refuses to run if any positive fill row exists for the order.

BEGIN;

UPDATE us_orders o
SET status = 'EXPIRED',
    qty_filled = 0,
    meta = COALESCE(o.meta, '{}'::jsonb) || jsonb_build_object(
        'strategy_owner', 'TQQQ_INFINITE',
        'sleeve_id', 'TQQQ_INFINITE',
        'incident_repair_version', 'TQQQ_INF_20260902_ZERO_FILL_V1',
        'incident_evidence', 'ARCHIVED_KIS_ORIGINAL_DAY_QUERY_20260914_ZERO_FILL_PLUS_CANCEL_NOT_FOUND',
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
