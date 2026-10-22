-- PR249 original KIS practice readback sourced from GitHub Actions:
-- orders https://github.com/Jeihyuck/Jeihyuck-rolling-k-auto-trade-KIS-refacored/actions/runs/39793300328
-- holdings https://github.com/Jeihyuck/Jeihyuck-rolling-k-auto-trade-KIS-refacored/actions/runs/39794211043
-- Execute inside BEGIN / COMMIT only after BEGIN / ROLLBACK dry run passes.
-- Never run for a live brokerage account; no broker orders are sent by this SQL.
DO $pr249$
DECLARE
  n integer;
  bad integer;
  account_digest text;
  preimage_id text := gen_random_uuid()::text;
BEGIN
  PERFORM pg_advisory_xact_lock(2491022);
  IF EXISTS(SELECT 1 FROM public.ledger_events WHERE event_type='KR_PR249_VERIFIED_BROKER_REPAIR' AND payload_json->>'source_run'='39793300328') THEN
    RAISE EXCEPTION 'PR249_ALREADY_APPLIED';
  END IF;
  SELECT encode(digest(account_id,'sha256'),'hex') INTO account_digest FROM public.portfolio_epochs
   WHERE portfolio_epoch_id='8105b903-405b-471c-9d9a-06764a5e2311'
     AND trading_epoch_id='3dc51d8e-61ce-414c-80a9-73f9b4911110'
     AND env='practice' AND status='ACTIVE';
  IF LEFT(COALESCE(account_digest,''),12) <> 'fef5d08b17e8' THEN RAISE EXCEPTION 'PR249_ACCOUNT_SCOPE_MISMATCH'; END IF;
  SELECT COUNT(*) INTO n FROM public.orders o WHERE o.env='practice'
    AND o.portfolio_epoch_id='8105b903-405b-471c-9d9a-06764a5e2311'
    AND o.trading_epoch_id='3dc51d8e-61ce-414c-80a9-73f9b4911110'
    AND o.kis_odno IN('0000013196','0000040226','0000013221','0000040213','0000013264','0000014123',
                      '0000013218','0000040070','0000014280','0000040083');
  IF n<>10 THEN RAISE EXCEPTION 'PR249_TARGET_ORDER_COUNT:%', n; END IF;
  SELECT COUNT(*) INTO bad FROM (VALUES
   ('2026-10-21'::date,'251970','BUY','0000013196',17,17,1162800::numeric),
   ('2026-10-21','251970','SELL','0000040226',17,10,685000),
   ('2026-10-21','086450','BUY','0000013221',48,48,1176000),
   ('2026-10-21','086450','SELL','0000040213',48,48,1180800),
   ('2026-10-22','053800','BUY','0000013264',12,12,1082400),
   ('2026-10-22','053800','SELL','0000014123',12,12,1084800),
   ('2026-10-22','144960','BUY','0000013218',81,81,1137680),
   ('2026-10-22','144960','SELL','0000040070',81,81,1091430),
   ('2026-10-22','253590','BUY','0000014280',79,79,1274270),
   ('2026-10-22','253590','SELL','0000040083',79,52,836180)
  ) x(trade_date,code,side,odno,requested,old_cumulative_qty,old_cumulative_amt)
  LEFT JOIN public.orders o ON o.env='practice' AND o.strategy='pb1_pullback_close'
    AND o.code=x.code AND o.side=x.side AND o.kis_odno=x.odno AND o.qty=x.requested
    AND (o.created_at AT TIME ZONE 'Asia/Seoul')::date=x.trade_date
    AND (o.response_json::jsonb->>'tot_ccld_qty')::integer=x.old_cumulative_qty
    AND (o.response_json::jsonb->>'tot_ccld_amt')::numeric=x.old_cumulative_amt
  WHERE o.order_id IS NULL;
  IF bad<>0 THEN RAISE EXCEPTION 'PR249_STALE_KIS_ORDER_PREIMAGE:%',bad; END IF;
  SELECT COUNT(*) INTO bad FROM (VALUES
   ('1c34540b-86d6-421c-ae12-c84d7469b75b','251970','BUY',15),
   ('74f3140a-f810-4be6-b49a-de5f871d61ad','086450','BUY',22),
   ('7cb19072-92fd-4b86-8c18-b5d24b427969','251970','BUY',17),
   ('e8731a9e-70b0-40eb-9d5f-e789fa041913','086450','BUY',48)
  ) x(fill_id,code,side,qty) LEFT JOIN public.fills f
    ON f.fill_id=x.fill_id AND f.code=x.code AND f.side=x.side AND f.qty=x.qty AND f.env='practice'
  WHERE f.fill_id IS NULL;
  IF bad<>0 THEN RAISE EXCEPTION 'PR249_STALE_BUY_FILL_PREIMAGE:%',bad; END IF;
  IF EXISTS(SELECT 1 FROM public.fills WHERE env='practice' AND kis_odno IN('0000040226','0000040083')) THEN
    RAISE EXCEPTION 'PR249_SELL_ALREADY_PERSISTED';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM public.positions WHERE position_cycle_id='bb24815a-f713-49ab-8a05-bde33c9d8a53'
    AND code='053800' AND qty=12 AND status='OPEN' AND avg_buy_price=90200 AND exit_policy_family='SWING_STAGED_EXIT')
  OR NOT EXISTS(SELECT 1 FROM public.positions WHERE position_cycle_id='4b7235d8-7921-491d-9053-a5451197c2e7'
    AND code='251970' AND qty=17 AND status='OPEN' AND avg_buy_price=68400)
  OR NOT EXISTS(SELECT 1 FROM public.positions WHERE position_cycle_id='758b23ad-0625-4c96-b4aa-4020298910be'
    AND code='253590' AND qty=79 AND status='OPEN' AND avg_buy_price=16130)
  OR NOT EXISTS(SELECT 1 FROM public.positions WHERE position_cycle_id='ec1e3ac5-c9f4-49b7-8119-802dba8cde6f'
    AND code='144960' AND qty=0 AND status='CLOSED' AND realized_pnl=-46215)
  THEN RAISE EXCEPTION 'PR249_STALE_POSITIONS_PREIMAGE'; END IF;

  -- Immutable complete backup before any UPDATE/DELETE, in this SAME transaction.
  INSERT INTO public.ledger_events(ledger_event_id,env,event_type,ts,ok,reasons,payload_json)
  VALUES(preimage_id,'practice','KR_PR249_BROKER_REPAIR_PREIMAGE',
    to_char(now() AT TIME ZONE 'Asia/Seoul','YYYY-MM-DD"T"HH24:MI:SS'),true,'[]',
    jsonb_build_object(
      'source_run','39793300328','balance_run','39794211043',
      'before_orders',(SELECT jsonb_agg(to_jsonb(o)) FROM public.orders o WHERE o.env='practice' AND o.kis_odno IN
         ('0000013196','0000040226','0000013221','0000040213','0000013264','0000014123','0000013218','0000040070','0000014280','0000040083')),
      'before_fills',(SELECT jsonb_agg(to_jsonb(f)) FROM public.fills f WHERE f.order_id IN
         (SELECT o.order_id FROM public.orders o WHERE o.env='practice' AND o.kis_odno IN
         ('0000013196','0000040226','0000013221','0000040213','0000013264','0000014123','0000013218','0000040070','0000014280','0000040083'))),
      'before_positions',(SELECT jsonb_agg(to_jsonb(p)) FROM public.positions p WHERE p.env='practice' AND p.code IN('053800','251970','253590','086450','144960')),
      'before_claims',(SELECT jsonb_agg(to_jsonb(c)) FROM public.execution_claims c WHERE c.env='practice' AND c.lifecycle_id IN
      ('3f8e25dc-56f5-4e1a-a6f7-961b4a412d0d','bb24815a-f713-49ab-8a05-bde33c9d8a53',
       '4b7235d8-7921-491d-9053-a5451197c2e7','758b23ad-0625-4c96-b4aa-4020298910be'))));

  DELETE FROM public.fills f WHERE
    (f.fill_id='1c34540b-86d6-421c-ae12-c84d7469b75b' AND f.code='251970' AND f.side='BUY' AND f.qty=15)
    OR(f.fill_id='74f3140a-f810-4be6-b49a-de5f871d61ad' AND f.code='086450' AND f.side='BUY' AND f.qty=22);
  GET DIAGNOSTICS n=ROW_COUNT; IF n<>2 THEN RAISE EXCEPTION 'PR249_DOUBLE_BUY_ROWS:%',n; END IF;

  UPDATE public.fills f SET filled_at=f.filled_at-interval '9 hours',
    fill_meta_json=COALESCE(f.fill_meta_json,'{}'::jsonb)||
      jsonb_build_object('time_source','KIS_ORDER_TIME_PROXY_NOT_EXECUTION','pr249_source_run','39793300328')
   WHERE f.fill_id IN('416d7005-6c09-4aca-86ce-9c0d3f69638d','275a016e-5d27-45bc-aee8-ed974fa56673',
    'e8731a9e-70b0-40eb-9d5f-e789fa041913','b568b95b-78cc-492e-87db-4e633c74648c',
    'a27b148f-df06-4c11-8958-a9b71096e0ff','0ba7e674-bef2-49c8-9f4d-e0227b70e09d',
    '7cb19072-92fd-4b86-8c18-b5d24b427969','feb152a3-4807-4c30-81e4-b60d2c2e141c');
  GET DIAGNOSTICS n=ROW_COUNT; IF n<>8 THEN RAISE EXCEPTION 'PR249_BAD_FILLED_AT_ROWS:%',n; END IF;

  -- REAL price is approximate. Source-accurate notional goes in JSON metadata.
  UPDATE public.fills f SET price=CASE WHEN f.kis_odno='0000013218' THEN (1137680.0/81)::real ELSE (1091430.0/81)::real END,
    fill_meta_json=COALESCE(f.fill_meta_json,'{}'::jsonb)||
     jsonb_build_object('broker_original_notional_krw',CASE WHEN f.kis_odno='0000013218' THEN 1137680 ELSE 1091430 END,
       'broker_amount_authoritative',true,'pr249_source_run','39793300328')
   WHERE f.fill_id IN('a27b148f-df06-4c11-8958-a9b71096e0ff','0ba7e674-bef2-49c8-9f4d-e0227b70e09d');
  GET DIAGNOSTICS n=ROW_COUNT; IF n<>2 THEN RAISE EXCEPTION 'PR249_144960_NOTIONAL:%',n; END IF;

  -- 053800 new 10/22 SWING BUY 12 was sold at 09:42; prior DAY cycle was already closed 10/20.
  -- Preserve original owner/claim as review evidence; adjust economic attribution only.
  UPDATE public.orders o SET position_cycle_id='bb24815a-f713-49ab-8a05-bde33c9d8a53',
   response_json=(o.response_json::jsonb||jsonb_build_object(
     '_pr249_original_cycle','3f8e25dc-56f5-4e1a-a6f7-961b4a412d0d','_pr249_source_run','39793300328'))::text,
   updated_at=now()
   WHERE o.order_id='31318d8b-d2c1-477f-bdb5-ba1f464f88c0' AND o.side='SELL'
     AND o.position_cycle_id='3f8e25dc-56f5-4e1a-a6f7-961b4a412d0d';
  GET DIAGNOSTICS n=ROW_COUNT; IF n<>1 THEN RAISE EXCEPTION 'PR249_053800_ORDER_CYCLE:%',n; END IF;
  UPDATE public.fills f SET position_cycle_id='bb24815a-f713-49ab-8a05-bde33c9d8a53',
   fill_meta_json=COALESCE(f.fill_meta_json,'{}'::jsonb)||jsonb_build_object(
     'pr249_original_cycle','3f8e25dc-56f5-4e1a-a6f7-961b4a412d0d','pr249_source_run','39793300328')
   WHERE f.fill_id='275a016e-5d27-45bc-aee8-ed974fa56673'
     AND f.position_cycle_id='3f8e25dc-56f5-4e1a-a6f7-961b4a412d0d';
  GET DIAGNOSTICS n=ROW_COUNT; IF n<>1 THEN RAISE EXCEPTION 'PR249_053800_FILL_CYCLE:%',n; END IF;

  UPDATE public.orders o SET status='FILLED',
   response_json=(o.response_json::jsonb||jsonb_build_object(
     '_pr249_previous_cumulative_qty',o.response_json::jsonb->>'tot_ccld_qty',
     '_pr249_previous_cumulative_amt',o.response_json::jsonb->>'tot_ccld_amt',
     'tot_ccld_qty',CASE WHEN o.code='251970' THEN '17' ELSE '79' END,
     'tot_ccld_amt',CASE WHEN o.code='251970' THEN '1163800' ELSE '1270340' END,
     '_pr249_source_run','39793300328'))::text,
    updated_at=now()
   WHERE o.order_id IN('3f8314d8-4342-4528-ab93-44002b2e536a','29a6eb20-d03f-4037-a6cc-60d5900fe20c') AND o.status='RECONCILED';
  GET DIAGNOSTICS n=ROW_COUNT; IF n<>2 THEN RAISE EXCEPTION 'PR249_CUMULATIVE_SELL_ORDERS:%',n; END IF;

  INSERT INTO public.fills(fill_id,env,run_id,order_id,kis_odno,trade_id,broker_fill_id,code,market,side,qty,price,fee,tax,
     filled_at,raw_json,fill_meta_json,position_cycle_id,portfolio_epoch_id,trading_epoch_id)
   SELECT gen_random_uuid()::text,o.env,o.run_id,o.order_id,o.kis_odno,
     'KR_KIS_CUM:'||account_digest||':20261021:0000040226:251970:SELL',
     'KR_KIS_CUM:'||account_digest||':20261021:0000040226:251970:SELL',
      o.code,o.market,o.side,17,(1163800.0/17)::real,0,0,timestamptz '2026-10-21 15:16:27+09',
      jsonb_build_object('source','PR249_AUTHENTICATED_KIS_GET','source_run','39793300328',
                        'source_cumulative_qty',17,'source_cumulative_amt',1163800)::text,
      jsonb_build_object('evidence_kind','KIS_ORDER_CUMULATIVE_EXECUTION','fill_source','pr249_original_kis_get',
        'broker_original_notional_krw',1163800,'time_source','KIS_ORDER_TIME_PROXY_NOT_EXECUTION','pr249_source_run','39793300328'),
      o.position_cycle_id,o.portfolio_epoch_id,o.trading_epoch_id
   FROM public.orders o WHERE o.order_id='3f8314d8-4342-4528-ab93-44002b2e536a';
  GET DIAGNOSTICS n=ROW_COUNT; IF n<>1 THEN RAISE EXCEPTION 'PR249_INSERT_SELL_251970:%',n; END IF;
  INSERT INTO public.fills(fill_id,env,run_id,order_id,kis_odno,trade_id,broker_fill_id,code,market,side,qty,price,fee,tax,
     filled_at,raw_json,fill_meta_json,position_cycle_id,portfolio_epoch_id,trading_epoch_id)
   SELECT gen_random_uuid()::text,o.env,o.run_id,o.order_id,o.kis_odno,
     'KR_KIS_CUM:'||account_digest||':20261022:0000040083:253590:SELL',
     'KR_KIS_CUM:'||account_digest||':20261022:0000040083:253590:SELL',
      o.code,o.market,o.side,79,(1270340.0/79)::real,0,0,timestamptz '2026-10-22 15:18:15+09',
      jsonb_build_object('source','PR249_AUTHENTICATED_KIS_GET','source_run','39793300328',
                        'source_cumulative_qty',79,'source_cumulative_amt',1270340)::text,
      jsonb_build_object('evidence_kind','KIS_ORDER_CUMULATIVE_EXECUTION','fill_source','pr249_original_kis_get',
        'broker_original_notional_krw',1270340,'time_source','KIS_ORDER_TIME_PROXY_NOT_EXECUTION','pr249_source_run','39793300328'),
      o.position_cycle_id,o.portfolio_epoch_id,o.trading_epoch_id
   FROM public.orders o WHERE o.order_id='29a6eb20-d03f-4037-a6cc-60d5900fe20c';
  GET DIAGNOSTICS n=ROW_COUNT; IF n<>1 THEN RAISE EXCEPTION 'PR249_INSERT_SELL_253590:%',n; END IF;

  UPDATE public.positions p SET qty=0,total_cost=0,avg_buy_price=NULL,realized_pnl=2400,
   status='CLOSED',closed_reason='FULL_SELL',closed_ts=timestamp '2026-10-22 09:42:06',
   last_trade_at='2026-10-22T09:42:06+09:00',last_reconciled_at='2026-10-22T09:42:06+09:00',
   position_meta=(COALESCE(p.position_meta,'{}'::jsonb)-'sell_execution_reconcile')||
      jsonb_build_object('sell_execution_reconcile',jsonb_build_object('31318d8b-d2c1-477f-bdb5-ba1f464f88c0',
        jsonb_build_object('confirmed_cumulative_qty',12,'cost_basis_at_sell',90200,'qty_accounted',12,
          'pnl_accounted_qty',12,'broker_holding_qty',0,'fill_price',90400,'pnl_accounted_notional',1084800,'realized_pnl_status','CONFIRMED')),
      'realized_pnl_status','CONFIRMED','pr249_repair',jsonb_build_object('source_run','39793300328',
        'balance_run','39794211043','old_cycle_metadata_removed',true,'entry_policy_unchanged','SWING_STAGED_EXIT')),
   updated_at=now()::text
   WHERE p.position_cycle_id='bb24815a-f713-49ab-8a05-bde33c9d8a53' AND p.env='practice' AND p.qty=12 AND p.status='OPEN';
  GET DIAGNOSTICS n=ROW_COUNT; IF n<>1 THEN RAISE EXCEPTION 'PR249_CLOSE_053800:%',n; END IF;
  UPDATE public.positions p SET qty=0,total_cost=0,avg_buy_price=NULL,realized_pnl=1000,
   status='CLOSED',closed_reason='FULL_SELL',closed_ts=timestamp '2026-10-21 15:16:27',
   last_trade_at='2026-10-21T15:16:27+09:00',last_reconciled_at='2026-10-21T15:16:27+09:00',
   position_meta=COALESCE(p.position_meta,'{}'::jsonb)||
     jsonb_build_object('sell_execution_reconcile',jsonb_build_object('3f8314d8-4342-4528-ab93-44002b2e536a',
       jsonb_build_object('confirmed_cumulative_qty',17,'cost_basis_at_sell',68400,'qty_accounted',17,
        'pnl_accounted_qty',17,'broker_holding_qty',0,'fill_price',1163800.0/17,
        'pnl_accounted_notional',1163800,'realized_pnl_status','CONFIRMED')),
     'realized_pnl_status','CONFIRMED','pr249_repair',
       jsonb_build_object('source_run','39793300328','balance_run','39794211043')),
   updated_at=now()::text
   WHERE p.position_cycle_id='4b7235d8-7921-491d-9053-a5451197c2e7' AND p.env='practice' AND p.qty=17 AND p.status='OPEN';
  GET DIAGNOSTICS n=ROW_COUNT; IF n<>1 THEN RAISE EXCEPTION 'PR249_CLOSE_251970:%',n; END IF;
  UPDATE public.positions p SET qty=0,total_cost=0,avg_buy_price=NULL,realized_pnl=-3930,
   status='CLOSED',closed_reason='FULL_SELL',closed_ts=timestamp '2026-10-22 15:18:15',
   last_trade_at='2026-10-22T15:18:15+09:00',last_reconciled_at='2026-10-22T15:18:15+09:00',
   position_meta=COALESCE(p.position_meta,'{}'::jsonb)||
     jsonb_build_object('sell_execution_reconcile',jsonb_build_object('29a6eb20-d03f-4037-a6cc-60d5900fe20c',
       jsonb_build_object('confirmed_cumulative_qty',79,'cost_basis_at_sell',16130,'qty_accounted',79,
        'pnl_accounted_qty',79,'broker_holding_qty',0,'fill_price',1270340.0/79,
        'pnl_accounted_notional',1270340,'realized_pnl_status','CONFIRMED')),
     'realized_pnl_status','CONFIRMED','pr249_repair',
       jsonb_build_object('source_run','39793300328','balance_run','39794211043')),
   updated_at=now()::text
   WHERE p.position_cycle_id='758b23ad-0625-4c96-b4aa-4020298910be' AND p.env='practice' AND p.qty=79 AND p.status='OPEN';
  GET DIAGNOSTICS n=ROW_COUNT; IF n<>1 THEN RAISE EXCEPTION 'PR249_CLOSE_253590:%',n; END IF;
  UPDATE public.positions p SET realized_pnl=-46250,
   position_meta=jsonb_set(COALESCE(p.position_meta,'{}'::jsonb),
    '{sell_execution_reconcile,a99b358d-d115-4bce-ad29-29f79e4ce63d,cost_basis_at_sell}',
     to_jsonb(1137680.0/81),true)||
     jsonb_build_object('pr249_repair',jsonb_build_object('source_run','39793300328',
       'exact_buy_notional',1137680,'exact_sell_notional',1091430,'net_pnl_before_fees',-46250)),
   updated_at=now()::text
   WHERE p.position_cycle_id='ec1e3ac5-c9f4-49b7-8119-802dba8cde6f' AND p.env='practice'
    AND p.status='CLOSED' AND p.qty=0 AND p.realized_pnl=-46215;
  GET DIAGNOSTICS n=ROW_COUNT; IF n<>1 THEN RAISE EXCEPTION 'PR249_CORRECT_144960_PNL:%',n; END IF;

  -- Each order is exactly matched against real KIS original quantities and authoritative NOTIONAL.
  SELECT COUNT(*) INTO bad FROM (VALUES
   ('251970','BUY','0000013196',17,1162800::numeric),('251970','SELL','0000040226',17,1163800),
   ('086450','BUY','0000013221',48,1176000),('086450','SELL','0000040213',48,1180800),
   ('053800','BUY','0000013264',12,1082400),('053800','SELL','0000014123',12,1084800),
   ('144960','BUY','0000013218',81,1137680),('144960','SELL','0000040070',81,1091430),
   ('253590','BUY','0000014280',79,1274270),('253590','SELL','0000040083',79,1270340)
  ) x(code,side,odno,expected_qty,expected_notional)
  LEFT JOIN public.orders o ON o.env='practice' AND o.code=x.code AND o.side=x.side AND o.kis_odno=x.odno
  LEFT JOIN LATERAL(
    SELECT COALESCE(SUM(f.qty),0)::integer qty,
     COALESCE(SUM(COALESCE((f.fill_meta_json->>'broker_original_notional_krw')::numeric,
        f.qty::double precision*f.price::double precision)),0)::numeric exact_notional,
     COALESCE(BOOL_AND(f.position_cycle_id IS NOT DISTINCT FROM o.position_cycle_id),false) cycles_match
    FROM public.fills f WHERE f.order_id=o.order_id
  ) f ON TRUE
  WHERE o.order_id IS NULL OR f.qty<>x.expected_qty OR ABS(f.exact_notional-x.expected_notional)>0.25
    OR f.cycles_match IS NOT TRUE;
  IF bad<>0 THEN RAISE EXCEPTION 'PR249_POSTCOMMIT_KIS_FILL_RECONCILE:%',bad; END IF;
  SELECT COUNT(*) INTO n FROM public.positions WHERE env='practice' AND status='CLOSED' AND qty=0
   AND position_meta->>'realized_pnl_status'='CONFIRMED'
   AND position_cycle_id IN('bb24815a-f713-49ab-8a05-bde33c9d8a53',
                           '4b7235d8-7921-491d-9053-a5451197c2e7',
                           '758b23ad-0625-4c96-b4aa-4020298910be');
  IF n<>3 THEN RAISE EXCEPTION 'PR249_POSTCOMMIT_OPEN_BROKER_ZERO:%',n; END IF;
  INSERT INTO public.ledger_events(ledger_event_id,env,event_type,ts,ok,reasons,payload_json)
    VALUES(gen_random_uuid()::text,'practice','KR_PR249_VERIFIED_BROKER_REPAIR',
    to_char(now() AT TIME ZONE 'Asia/Seoul','YYYY-MM-DD"T"HH24:MI:SS'),true,'[]',
    jsonb_build_object('source_run','39793300328','balance_run','39794211043',
      'preimage_ledger_event_id',preimage_id,'reconciled_orders',10,'closed_reconciled_positions',3,
      'no_broker_orders_placed',true,'timestamp_kind','KIS_ORDER_TIME_PROXY',
      'policy_review','053800 old DAY claim misattributed to new SWING buy; archived claim remains'));
END;
$pr249$;
