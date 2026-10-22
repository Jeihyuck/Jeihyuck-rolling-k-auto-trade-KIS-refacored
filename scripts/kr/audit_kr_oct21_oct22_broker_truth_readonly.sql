-- Read-only operational evidence audit for KR 2026-10-21 and 2026-10-22.
-- No UPDATE/DELETE/INSERT and NO mutation of production positions or fills.
-- Validate with KIS order-number/date, account/epoch, original raw responses,
-- position owner + source contract before any separately approved backfill.
--
-- 1. Requested, broker-cumulative and legacy stored fill quantities are
--    deliberately kept as separate columns, not merged or "fixed" here.
WITH filled AS (
    SELECT order_id, COALESCE(SUM(qty),0) AS stored_fill_qty,
           COUNT(*) AS stored_fill_rows,
           SUM(qty * price) AS derived_stored_notional
    FROM fills
    GROUP BY order_id
), broker AS (
    SELECT o.env, o.order_id, o.code, o.side, o.kis_odno, o.status,
           o.qty AS ordered_qty, o.created_at, o.position_cycle_id,
           NULLIF(o.response_json::jsonb->>'tot_ccld_qty','')::numeric AS broker_cumulative_qty,
           NULLIF(o.response_json::jsonb->>'tot_ccld_amt','')::numeric AS broker_cumulative_notional
    FROM orders o
    WHERE o.created_at >= timestamptz '2026-10-21 00:00+09'
      AND o.created_at < timestamptz '2026-10-23 00:00+09'
)
SELECT b.code, b.side, b.kis_odno, b.status, b.ordered_qty,
       b.broker_cumulative_qty, b.broker_cumulative_notional,
       COALESCE(f.stored_fill_qty,0) AS stored_fill_qty,
       COALESCE(f.stored_fill_rows,0) AS stored_fill_rows,
       COALESCE(f.derived_stored_notional,0) AS derived_stored_notional,
       b.position_cycle_id, b.created_at AT TIME ZONE 'Asia/Seoul' AS order_kst,
       CASE WHEN b.broker_cumulative_qty IS NULL THEN 'BROKER_QTY_NOT_PROVEN'
            WHEN b.broker_cumulative_qty < b.ordered_qty THEN 'BROKER_PARTIAL'
            WHEN COALESCE(f.stored_fill_qty,0) > b.broker_cumulative_qty THEN 'STORED_CUMULATIVE_OVERCOUNT'
            WHEN COALESCE(f.stored_fill_qty,0) < b.broker_cumulative_qty THEN 'BROKER_OBSERVED_DB_UNAPPLIED'
            ELSE 'QTY_MATCH_ONLY_NOT_COMPLETE_SETTLEMENT'
       END AS audit_classification
FROM broker b
LEFT JOIN filled f ON f.order_id = b.order_id
ORDER BY b.created_at, b.side;

-- 2. Trading-day source timestamps: observed legacy +9-hour drift is not
--    repaired until original order/fill timestamps are reverified.
SELECT code, side, kis_odno, position_cycle_id, qty, price,
       filled_at AT TIME ZONE 'Asia/Seoul' AS stored_filled_kst,
       created_at::timestamptz AT TIME ZONE 'Asia/Seoul' AS ingested_kst,
       EXTRACT(EPOCH FROM (filled_at - created_at::timestamptz))/3600
           AS stored_fill_minus_ingestion_hours
FROM fills
WHERE created_at::timestamptz >= timestamptz '2026-10-21 00:00+09'
  AND created_at::timestamptz < timestamptz '2026-10-23 00:00+09'
ORDER BY created_at::timestamptz;

-- 3. Mixed lifecycle and CLOSE runs: aggregate by exact cycle and code.
SELECT p.env,p.code,p.position_cycle_id,p.status,p.qty,
       p.entry_style_selected,p.exit_policy_family,
       p.entry_meta_json::jsonb->>'source_buy_order_id' AS source_buy_order_id,
       p.realized_pnl,
       p.position_meta::jsonb->>'realized_pnl_status' AS pnl_status
FROM positions p
WHERE p.code IN ('053800','251970','253590','086450','144960')
ORDER BY p.code,p.opened_at::timestamptz DESC;

SELECT strategy, run_window, status, COUNT(*) AS count
FROM runs
WHERE started_at >= timestamptz '2026-10-21 00:00+09'
  AND started_at < timestamptz '2026-10-23 00:00+09'
GROUP BY strategy, run_window, status ORDER BY strategy,run_window,status;
