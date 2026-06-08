-- Add columns outside DO block so they are visible to subsequent DML in the same transaction
ALTER TABLE us_fills ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE us_fills ADD COLUMN IF NOT EXISTS fill_idempotency_key TEXT;

-- Back-fill updated_at from created_at for rows that don't have it yet
UPDATE us_fills SET updated_at = COALESCE(created_at, NOW()) WHERE updated_at IS NULL;

-- Set DEFAULT for future rows (alter after back-fill so NOT NULL constraint is safe)
ALTER TABLE us_fills ALTER COLUMN updated_at SET DEFAULT NOW();

DO $$
DECLARE
    v_dup_count integer := 0;
    v_deleted_count integer := 0;
BEGIN

    SELECT COUNT(*)
    INTO v_dup_count
    FROM (
        SELECT
            trade_date,
            symbol,
            side,
            COALESCE(order_no, '') AS order_no_norm,
            COALESCE(client_order_key, '') AS client_order_key_norm,
            qty,
            price_usd,
            COUNT(*) AS cnt
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
    ) d;

    RAISE NOTICE USING MESSAGE =
        '[DB][MIGRATE][DEDUP][START] table=us_fills index=uq_us_fills_idempotent duplicates=' || v_dup_count::text;

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
                    updated_at DESC NULLS LAST,
                    created_at DESC NULLS LAST,
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
    INTO v_deleted_count
    FROM deleted;

    RAISE NOTICE USING MESSAGE =
        '[DB][MIGRATE][DEDUP][DELETE] deleted=' || v_deleted_count::text;
    RAISE NOTICE USING MESSAGE =
        '[DB][MIGRATE][DEDUP][DONE] status=OK';

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

    RAISE NOTICE USING MESSAGE =
        '[DB][MIGRATE][INDEX][CREATE] index=uq_us_fills_idempotent';
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
    RAISE NOTICE USING MESSAGE =
        '[DB][MIGRATE][INDEX][OK] index=uq_us_fills_idempotent';
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_us_fills_idempotency_key
ON us_fills (fill_idempotency_key);

CREATE INDEX IF NOT EXISTS idx_us_orders_trade_date_status
ON us_orders (trade_date, status);

CREATE INDEX IF NOT EXISTS idx_us_orders_order_no
ON us_orders (order_no);