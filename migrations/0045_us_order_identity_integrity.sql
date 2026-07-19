-- Quarantine contaminated rows before enabling fail-closed identity constraints.
CREATE TABLE IF NOT EXISTS us_trade_integrity_quarantine (
    id bigserial PRIMARY KEY, quarantined_at timestamptz NOT NULL DEFAULT now(),
    source_table text NOT NULL, reason text NOT NULL, original_row jsonb NOT NULL
);
ALTER TABLE us_trade_integrity_quarantine ADD COLUMN IF NOT EXISTS quarantine_batch_id text;
ALTER TABLE us_trade_integrity_quarantine ADD COLUMN IF NOT EXISTS trade_date date;
ALTER TABLE us_trade_integrity_quarantine ADD COLUMN IF NOT EXISTS client_order_key text;
ALTER TABLE us_trade_integrity_quarantine ADD COLUMN IF NOT EXISTS order_no text;

DO $$ DECLARE blanks bigint; dup_keys bigint; dup_orders bigint; conflicts bigint;
BEGIN
  SELECT count(*) INTO blanks FROM us_orders WHERE client_order_key IS NULL OR btrim(client_order_key)='';
  SELECT count(*) INTO dup_keys FROM (SELECT client_order_key FROM us_orders GROUP BY client_order_key HAVING count(*)>1) x;
  SELECT count(*) INTO dup_orders FROM (SELECT trade_date,order_no FROM us_orders WHERE order_no IS NOT NULL AND btrim(order_no)<>'' GROUP BY trade_date,order_no HAVING count(*)>1) x;
  SELECT count(*) INTO conflicts FROM (SELECT client_order_key FROM us_orders GROUP BY client_order_key HAVING count(DISTINCT (trade_date,symbol,side,exchange))>1) x;
  RAISE NOTICE 'us identity cleanup blank_keys=% duplicate_client_keys=% duplicate_order_numbers=% identity_conflicts=%', blanks,dup_keys,dup_orders,conflicts;
END $$;

INSERT INTO us_trade_integrity_quarantine(source_table, reason, original_row)
SELECT 'us_order_intents', 'BLANK_CLIENT_ORDER_KEY', to_jsonb(t) FROM us_order_intents t
 WHERE client_order_key IS NULL OR btrim(client_order_key) = '' OR lower(btrim(client_order_key)) IN ('none','null');
DELETE FROM us_order_intents WHERE client_order_key IS NULL OR btrim(client_order_key) = '' OR lower(btrim(client_order_key)) IN ('none','null');
INSERT INTO us_trade_integrity_quarantine(source_table, reason, original_row)
SELECT 'us_orders', 'BLANK_CLIENT_ORDER_KEY', to_jsonb(t) FROM us_orders t
 WHERE client_order_key IS NULL OR btrim(client_order_key) = '' OR lower(btrim(client_order_key)) IN ('none','null');
DELETE FROM us_orders WHERE client_order_key IS NULL OR btrim(client_order_key) = '' OR lower(btrim(client_order_key)) IN ('none','null');
INSERT INTO us_trade_integrity_quarantine(source_table, reason, original_row)
SELECT 'us_fills', 'BLANK_CLIENT_ORDER_KEY', to_jsonb(t) FROM us_fills t
 WHERE client_order_key IS NULL OR btrim(client_order_key) = '' OR lower(btrim(client_order_key)) IN ('none','null');
DELETE FROM us_fills WHERE client_order_key IS NULL OR btrim(client_order_key) = '' OR lower(btrim(client_order_key)) IN ('none','null');

-- Ambiguous client identities and broker order numbers have no safe canonical
-- row without KIS evidence. Quarantine every dependent row in one batch,
-- then remove the full group so no orphan intents/fills survive.
INSERT INTO us_trade_integrity_quarantine
 (source_table,reason,original_row,quarantine_batch_id,trade_date,client_order_key,order_no)
SELECT 'us_order_intents','DUPLICATE_CLIENT_KEY_IDENTITY_RELATED_INTENT',to_jsonb(i),
       md5('client|'||i.client_order_key),i.trade_date,i.client_order_key,NULL
FROM us_order_intents i JOIN (SELECT client_order_key FROM us_orders GROUP BY client_order_key
      HAVING count(DISTINCT (trade_date,symbol,side,exchange)) > 1) d USING(client_order_key);
INSERT INTO us_trade_integrity_quarantine
 (source_table,reason,original_row,quarantine_batch_id,trade_date,client_order_key,order_no)
SELECT 'us_fills','DUPLICATE_CLIENT_KEY_IDENTITY_RELATED_FILL',to_jsonb(f),
       md5('client|'||f.client_order_key),f.trade_date,f.client_order_key,f.order_no
FROM us_fills f JOIN (SELECT client_order_key FROM us_orders GROUP BY client_order_key
      HAVING count(DISTINCT (trade_date,symbol,side,exchange)) > 1) d USING(client_order_key);
INSERT INTO us_trade_integrity_quarantine
 (source_table,reason,original_row,quarantine_batch_id,trade_date,client_order_key,order_no)
SELECT 'us_orders','DUPLICATE_CLIENT_KEY_IDENTITY',to_jsonb(o),
       md5('client|'||o.client_order_key),o.trade_date,o.client_order_key,o.order_no
FROM us_orders o JOIN (SELECT client_order_key FROM us_orders GROUP BY client_order_key
      HAVING count(DISTINCT (trade_date,symbol,side,exchange)) > 1) d USING(client_order_key);
DELETE FROM us_order_intents i USING (SELECT client_order_key FROM us_orders GROUP BY client_order_key
  HAVING count(DISTINCT (trade_date,symbol,side,exchange)) > 1) d WHERE i.client_order_key=d.client_order_key;
DELETE FROM us_fills f USING (SELECT client_order_key FROM us_orders GROUP BY client_order_key
  HAVING count(DISTINCT (trade_date,symbol,side,exchange)) > 1) d WHERE f.client_order_key=d.client_order_key;
DELETE FROM us_orders o USING (
  SELECT client_order_key FROM us_orders GROUP BY client_order_key
  HAVING count(DISTINCT (trade_date,symbol,side,exchange)) > 1
) d WHERE o.client_order_key=d.client_order_key;

-- Quarantine all dependent intent/fill evidence in the same deterministic batch
-- before removing ambiguous broker order groups.
INSERT INTO us_trade_integrity_quarantine
 (source_table,reason,original_row,quarantine_batch_id,trade_date,client_order_key,order_no)
SELECT 'us_fills','DUPLICATE_BROKER_ORDER_RELATED_FILL',to_jsonb(f),
       md5(f.trade_date::text||'|'||f.order_no),f.trade_date,f.client_order_key,f.order_no
FROM us_fills f JOIN (SELECT trade_date,order_no FROM us_orders WHERE order_no IS NOT NULL AND btrim(order_no)<>''
 GROUP BY trade_date,order_no HAVING count(*)>1) d USING(trade_date,order_no);
INSERT INTO us_trade_integrity_quarantine
 (source_table,reason,original_row,quarantine_batch_id,trade_date,client_order_key,order_no)
SELECT 'us_order_intents','DUPLICATE_BROKER_ORDER_RELATED_INTENT',to_jsonb(i),
       md5(o.trade_date::text||'|'||o.order_no),i.trade_date,i.client_order_key,o.order_no
FROM us_order_intents i JOIN us_orders o USING(client_order_key)
JOIN (SELECT trade_date,order_no FROM us_orders WHERE order_no IS NOT NULL AND btrim(order_no)<>''
 GROUP BY trade_date,order_no HAVING count(*)>1) d ON d.trade_date=o.trade_date AND d.order_no=o.order_no;
DELETE FROM us_fills f USING (SELECT trade_date,order_no FROM us_orders WHERE order_no IS NOT NULL AND btrim(order_no)<>''
 GROUP BY trade_date,order_no HAVING count(*)>1) d WHERE f.trade_date=d.trade_date AND f.order_no=d.order_no;
DELETE FROM us_order_intents i USING us_orders o,
 (SELECT trade_date,order_no FROM us_orders WHERE order_no IS NOT NULL AND btrim(order_no)<>'' GROUP BY trade_date,order_no HAVING count(*)>1) d
 WHERE i.client_order_key=o.client_order_key AND o.trade_date=d.trade_date AND o.order_no=d.order_no;

INSERT INTO us_trade_integrity_quarantine(source_table, reason, original_row)
SELECT 'us_orders','DUPLICATE_BROKER_ORDER_NUMBER',to_jsonb(o) FROM us_orders o
JOIN (SELECT trade_date,order_no FROM us_orders WHERE order_no IS NOT NULL AND btrim(order_no)<>''
      GROUP BY trade_date,order_no HAVING count(*)>1) d USING(trade_date,order_no);
DELETE FROM us_orders o USING (
  SELECT trade_date,order_no FROM us_orders WHERE order_no IS NOT NULL AND btrim(order_no)<>''
  GROUP BY trade_date,order_no HAVING count(*)>1
) d WHERE o.trade_date=d.trade_date AND o.order_no=d.order_no;

ALTER TABLE us_order_intents ALTER COLUMN client_order_key SET NOT NULL;
ALTER TABLE us_orders ALTER COLUMN client_order_key SET NOT NULL;
ALTER TABLE us_fills ALTER COLUMN client_order_key SET NOT NULL;
ALTER TABLE us_order_intents ADD CONSTRAINT us_order_intents_client_key_nonblank CHECK (btrim(client_order_key) <> '' AND lower(btrim(client_order_key)) NOT IN ('none','null'));
ALTER TABLE us_orders ADD CONSTRAINT us_orders_client_key_nonblank CHECK (btrim(client_order_key) <> '' AND lower(btrim(client_order_key)) NOT IN ('none','null'));
ALTER TABLE us_fills ADD CONSTRAINT us_fills_client_key_nonblank CHECK (btrim(client_order_key) <> '' AND lower(btrim(client_order_key)) NOT IN ('none','null'));
CREATE UNIQUE INDEX IF NOT EXISTS uq_us_orders_trade_date_order_no_nonblank
 ON us_orders(trade_date, order_no) WHERE order_no IS NOT NULL AND btrim(order_no) <> '';
