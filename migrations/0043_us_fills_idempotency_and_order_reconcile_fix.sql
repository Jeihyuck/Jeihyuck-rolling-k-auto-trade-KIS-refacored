DO $$
DECLARE
    duplicate_count integer := 0;
    deleted_count integer := 0;
BEGIN
    RAISE NOTICE '[DB][MIGRATE][DEDUP][START] table=us_fills index=uq_us_fills_idempotent';

    SELECT COUNT(*)
    INTO duplicate_count
    FROM (
        SELECT 1
        FROM us_fills
        GROUP BY
            trade_date,
            symbol,
            side,
            COALESCE(order_no, ''),
            COALESCE(client_order_key, ''),
            qty,
            price_usd
        HAVING COUNT(*) > 1
    ) dup_keys;

    RAISE NOTICE '[DB][MIGRATE][DEDUP][DUPLICATES] count=%', duplicate_count;

    WITH ranked AS (
        SELECT
            ctid,
            ROW_NUMBER() OVER (
                PARTITION BY
                    trade_date,
                    symbol,
                    side,
                    COALESCE(order_no, ''),
                    COALESCE(client_order_key, ''),
                    qty,
                    price_usd
                ORDER BY
                    COALESCE(updated_at, created_at, NOW()) DESC,
                    ctid DESC
            ) AS rn
        FROM us_fills
    ),
    deleted AS (
        DELETE FROM us_fills f
        USING ranked r
        WHERE f.ctid = r.ctid
          AND r.rn > 1
        RETURNING 1
    )
    SELECT COUNT(*)
    INTO deleted_count
    FROM deleted;

    RAISE NOTICE '[DB][MIGRATE][DEDUP][DELETE] deleted=%', deleted_count;
    RAISE NOTICE '[DB][MIGRATE][DEDUP][DONE] status=OK';

    ALTER TABLE us_fills
    ADD COLUMN IF NOT EXISTS fill_idempotency_key TEXT;

    UPDATE us_fills
    SET fill_idempotency_key =
        trade_date::text || '|' ||
        symbol || '|' ||
        side || '|' ||
        COALESCE(order_no, '') || '|' ||
        COALESCE(client_order_key, '') || '|' ||
        qty::text || '|' ||
        price_usd::text
    WHERE fill_idempotency_key IS NULL
       OR fill_idempotency_key = '';

    RAISE NOTICE '[DB][MIGRATE][INDEX][CREATE] index=uq_us_fills_idempotent';
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_us_fills_idempotent
ON us_fills (
    trade_date,
    symbol,
    side,
    COALESCE(order_no, ''),
    COALESCE(client_order_key, ''),
    qty,
    price_usd
);

DO $$
BEGIN
    RAISE NOTICE '[DB][MIGRATE][INDEX][OK] index=uq_us_fills_idempotent';
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_us_fills_idempotency_key
ON us_fills (fill_idempotency_key);

CREATE INDEX IF NOT EXISTS idx_us_orders_trade_date_status
ON us_orders (trade_date, status);

CREATE INDEX IF NOT EXISTS idx_us_orders_order_no
ON us_orders (order_no);