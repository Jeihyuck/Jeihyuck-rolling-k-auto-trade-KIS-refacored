-- Persist the risk-accounting value and execution environment captured when
-- an order is submitted.  These values must not be reconstructed from fills.
ALTER TABLE us_orders
    ADD COLUMN IF NOT EXISTS committed_notional_usd NUMERIC(18, 4),
    ADD COLUMN IF NOT EXISTS env TEXT;

UPDATE us_orders o
SET committed_notional_usd = COALESCE(
        o.committed_notional_usd,
        i.notional_usd,
        CASE
            WHEN COALESCE(o.meta->>'requested_notional_usd', '') ~ '^[0-9]+([.][0-9]+)?$'
            THEN (o.meta->>'requested_notional_usd')::numeric
        END
    ),
    env = COALESCE(
        NULLIF(o.env, ''),
        NULLIF(o.meta->>'env', ''),
        NULLIF(o.meta->>'kis_env', ''),
        NULLIF(i.meta->>'env', ''),
        NULLIF(i.meta->>'kis_env', '')
    )
FROM us_order_intents i
WHERE i.client_order_key = o.client_order_key
  AND (o.committed_notional_usd IS NULL OR o.env IS NULL OR btrim(o.env) = '');

ALTER TABLE us_orders
    ALTER COLUMN env SET DEFAULT 'practice';

-- Do not silently attribute unresolved legacy rows to practice.  The explicit
-- sentinel remains filterable while allowing a NOT NULL contract for all new
-- writes.
UPDATE us_orders SET env = 'unknown' WHERE env IS NULL OR btrim(env) = '';
ALTER TABLE us_orders ALTER COLUMN env SET NOT NULL;

COMMENT ON COLUMN us_orders.committed_notional_usd IS
    'Requested order notional at submit time; independent of fills and average fill price';
COMMENT ON COLUMN us_orders.env IS
    'Actual KIS execution environment captured at submit time';
