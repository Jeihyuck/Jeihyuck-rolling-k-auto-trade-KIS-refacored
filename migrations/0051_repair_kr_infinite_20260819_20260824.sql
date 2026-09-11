-- One-time evidence repair for the exact KR_INFINITE incident proven by the
-- archived 2026-08-19 and 2026-08-24 runtime logs.
--
-- Evidence:
--  * 2026-08-19 NEW_CYCLE_BUY requested 6 and KIS accepted the order.
--  * 2026-08-24 before TP1, the KR_INFINITE decision log recorded broker_qty=6.
--    With no intervening KR_INFINITE intent, that later broker snapshot proves
--    the 8/19 BUY reached 6 shares.
--  * 2026-08-24 TP1 submitted SELL 3 from broker_qty=6; KIS accepted it.
--  * the same afternoon KIS holdings repeatedly showed broker_qty=3 and close
--    reconciliation showed DB=6/KIS=3, proving the SELL 3 filled.
--
-- Exact fill prices are NOT inferred from limit prices. This repair only
-- terminalizes quantities that the archived broker-holding evidence proves.
-- Scope is intentionally exact and idempotent; unrelated cycles are untouched.

UPDATE kr_infinite_order_intents
SET status = 'FILLED',
    filled_qty = 6,
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
        'strategy_owner', 'KR_INFINITE',
        'pre_order_holding_qty', 0,
        'incident_proven_post_order_holding_qty', 6,
        'incident_evidence', 'ARCHIVED_LOGS_20260819_20260824',
        'incident_repair_version', 'KR_INF_INCIDENT_REPAIR_V1',
        'fill_price_status', 'UNRESOLVED_NOT_INFERRED_FROM_LIMIT_PRICE'
    ),
    updated_at = NOW()
WHERE strategy_id = 'KR_INFINITE_V1'
  AND symbol = '122630'
  AND cycle_id = 'KRINF-20260819-1d1a9a3d'
  AND trade_date = DATE '2026-08-19'
  AND side = 'BUY'
  AND reason = 'NEW_CYCLE_BUY'
  AND idempotency_key = 'KR_INFINITE_V1:KRINF-20260819-1d1a9a3d:2026-08-19:BUY:1'
  AND requested_qty = 6
  AND broker_order_id IS NOT NULL
  AND status IN ('INTENT_CREATED', 'SUBMITTED', 'ACK', 'PENDING', 'PARTIALLY_FILLED', 'RECONCILE_PENDING');

UPDATE kr_infinite_order_intents
SET status = 'FILLED',
    filled_qty = 3,
    metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
        'strategy_owner', 'KR_INFINITE',
        'pre_order_holding_qty', 6,
        'incident_proven_post_order_holding_qty', 3,
        'historical_side_semantics', 'SELL_PARTIAL',
        'incident_evidence', 'ARCHIVED_LOGS_20260824',
        'incident_repair_version', 'KR_INF_INCIDENT_REPAIR_V1',
        'fill_price_status', 'UNRESOLVED_NOT_REQUIRED_FOR_QTY_TERMINALIZATION'
    ),
    updated_at = NOW()
WHERE strategy_id = 'KR_INFINITE_V1'
  AND symbol = '122630'
  AND cycle_id = 'KRINF-20260819-1d1a9a3d'
  AND trade_date = DATE '2026-08-24'
  AND side IN ('SELL_ALL', 'SELL_PARTIAL')
  AND reason = 'TAKE_PROFIT_TP1'
  AND idempotency_key = 'KR_INFINITE_V1:KRINF-20260819-1d1a9a3d:2026-08-24:SELL_TP1'
  AND requested_qty = 3
  AND broker_order_id IS NOT NULL
  AND status IN ('INTENT_CREATED', 'SUBMITTED', 'ACK', 'PENDING', 'PARTIALLY_FILLED', 'RECONCILE_PENDING');

-- The TP1 SELL was a three-share partial exit from six, with three shares
-- remaining. Clear the stale submitted fence only if the state row is still the
-- exact unresolved TP1 state from the incident. If an operator already repaired
-- or advanced the cycle, do not regress state.
UPDATE kr_infinite_state
SET status = 'ACTIVE',
    metadata = (COALESCE(metadata, '{}'::jsonb) - 'pending_profit_stage') || jsonb_build_object(
        'pending_profit_stage', NULL,
        'profit_stage', 'TP1_FILLED',
        'last_profit_fill_qty', 3,
        'last_profit_fill_date', '2026-08-24',
        'last_terminal_sell_status', 'FILLED',
        'last_terminal_sell_trade_date', '2026-08-24',
        'incident_repair_version', 'KR_INF_INCIDENT_REPAIR_V1'
    ),
    updated_at = NOW()
WHERE strategy_id = 'KR_INFINITE_V1'
  AND symbol = '122630'
  AND cycle_id = 'KRINF-20260819-1d1a9a3d'
  AND status = 'EXIT_PENDING'
  AND COALESCE(metadata->>'pending_profit_stage', '') = 'TP1_SUBMITTED'
  AND EXISTS (
      SELECT 1
      FROM kr_infinite_order_intents i
      WHERE i.strategy_id = 'KR_INFINITE_V1'
        AND i.symbol = '122630'
        AND i.cycle_id = 'KRINF-20260819-1d1a9a3d'
        AND i.trade_date = DATE '2026-08-24'
        AND i.reason = 'TAKE_PROFIT_TP1'
        AND i.idempotency_key = 'KR_INFINITE_V1:KRINF-20260819-1d1a9a3d:2026-08-24:SELL_TP1'
        AND i.requested_qty = 3
        AND i.status = 'FILLED'
        AND (i.metadata->>'incident_repair_version') = 'KR_INF_INCIDENT_REPAIR_V1'
  );
