-- PR #311 / migration 0062: READ ONLY evidence. Run before any DDL.
-- The preflight must be reviewed with the active KIS account and epoch.
BEGIN READ ONLY;
SELECT tablename,indexname,indexdef FROM pg_indexes
WHERE schemaname='public' AND tablename IN ('us_orders','us_fills','portfolio_epochs')
  AND (indexname LIKE 'uq_%' OR indexname LIKE '%idemp%')
ORDER BY tablename,indexname;
SELECT 'us_orders' AS source, count(*) AS total_rows FROM public.us_orders
UNION ALL SELECT 'us_fills', count(*) FROM public.us_fills;
SELECT trade_date,order_no,count(*) AS n FROM public.us_orders
WHERE order_no IS NOT NULL AND btrim(order_no)<>''
GROUP BY trade_date,order_no HAVING count(*)>1;
SELECT trade_date,symbol,side,COALESCE(order_no,'') AS order_no,
 COALESCE(client_order_key,'') AS client_order_key,qty,price_usd,count(*) AS n
FROM public.us_fills GROUP BY 1,2,3,4,5,6,7 HAVING count(*)>1;
-- Any result from the duplicate queries is a hard migration blocker.
-- Also verify that application writers and historical readers use the same
-- account/epoch scope before approving replacement of legacy indexes.
ROLLBACK;
