-- Read-only target capture for original KIS GET comparison, 2026-10-21/22.
-- Exactly ten KR practice orders. Includes neither account secrets nor modifications.
-- psql -XAt -f scripts/kr/pr249_db_snapshot_readonly.sql > /tmp/pr249-db.json
WITH targets(trade_date,code,side,kis_odno,ordered_qty) AS (
 VALUES
 ('2026-10-21','251970','BUY','0000013196',17),
 ('2026-10-21','251970','SELL','0000040226',17),
 ('2026-10-21','086450','BUY','0000013221',48),
 ('2026-10-21','086450','SELL','0000040213',48),
 ('2026-10-22','053800','BUY','0000013264',12),
 ('2026-10-22','053800','SELL','0000014123',12),
 ('2026-10-22','144960','BUY','0000013218',81),
 ('2026-10-22','144960','SELL','0000040070',81),
 ('2026-10-22','253590','BUY','0000014280',79),
 ('2026-10-22','253590','SELL','0000040083',79)
), scoped AS (
 SELECT t.trade_date,t.code,t.side,t.kis_odno,t.ordered_qty,
        o.env,o.strategy,o.order_id,o.position_cycle_id,o.portfolio_epoch_id,
        p.status AS position_status,
        COALESCE(SUM(f.qty),0)::int AS stored_fill_qty,
        COALESCE(ROUND(SUM(f.qty::numeric*f.price::numeric),2),0)::text AS stored_fill_notional
 FROM targets t
 JOIN public.orders o ON o.code=t.code AND o.side=t.side AND o.kis_odno=t.kis_odno
   AND o.qty=t.ordered_qty AND o.env='practice'
   AND (o.created_at AT TIME ZONE 'Asia/Seoul')::date=t.trade_date::date
 LEFT JOIN public.fills f ON f.order_id=o.order_id
 LEFT JOIN public.positions p ON p.position_cycle_id=o.position_cycle_id AND p.env=o.env
 GROUP BY t.trade_date,t.code,t.side,t.kis_odno,t.ordered_qty,
          o.env,o.strategy,o.order_id,o.position_cycle_id,o.portfolio_epoch_id,p.status
)
SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY s.trade_date,s.code,s.side),'[]'::jsonb)::text
FROM scoped s;
