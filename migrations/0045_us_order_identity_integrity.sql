-- Quarantine contaminated rows before enabling fail-closed identity constraints.
CREATE TABLE IF NOT EXISTS us_trade_integrity_quarantine (
    id bigserial PRIMARY KEY, quarantined_at timestamptz NOT NULL DEFAULT now(),
    source_table text NOT NULL, reason text NOT NULL, original_row jsonb NOT NULL
);

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

ALTER TABLE us_order_intents ALTER COLUMN client_order_key SET NOT NULL;
ALTER TABLE us_orders ALTER COLUMN client_order_key SET NOT NULL;
ALTER TABLE us_fills ALTER COLUMN client_order_key SET NOT NULL;
ALTER TABLE us_order_intents ADD CONSTRAINT us_order_intents_client_key_nonblank CHECK (btrim(client_order_key) <> '' AND lower(btrim(client_order_key)) NOT IN ('none','null'));
ALTER TABLE us_orders ADD CONSTRAINT us_orders_client_key_nonblank CHECK (btrim(client_order_key) <> '' AND lower(btrim(client_order_key)) NOT IN ('none','null'));
ALTER TABLE us_fills ADD CONSTRAINT us_fills_client_key_nonblank CHECK (btrim(client_order_key) <> '' AND lower(btrim(client_order_key)) NOT IN ('none','null'));
CREATE UNIQUE INDEX IF NOT EXISTS uq_us_orders_trade_date_order_no_nonblank
 ON us_orders(trade_date, order_no) WHERE order_no IS NOT NULL AND btrim(order_no) <> '';
