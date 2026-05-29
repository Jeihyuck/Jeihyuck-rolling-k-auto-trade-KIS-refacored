-- migrations/0042_us_orders_client_order_id.sql
-- US 주문 idempotency: client_order_id 컬럼 + unique index 추가.
-- 기존 us_orders 또는 us_order_intents 테이블에 적용한다.
-- 테이블이 없으면 무시한다 (IF EXISTS 없으면 오류 발생 주의).

-- us_order_intents에 client_order_id 추가
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'us_order_intents'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'us_order_intents' AND column_name = 'client_order_id'
        ) THEN
            ALTER TABLE us_order_intents ADD COLUMN client_order_id TEXT;
        END IF;
    END IF;
END $$;

-- us_orders에 client_order_id 추가
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'us_orders'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'us_orders' AND column_name = 'client_order_id'
        ) THEN
            ALTER TABLE us_orders ADD COLUMN client_order_id TEXT;
        END IF;
    END IF;
END $$;

-- us_order_intents unique index
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'us_order_intents'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE tablename = 'us_order_intents' AND indexname = 'uq_us_order_intents_client_order_id'
        ) THEN
            CREATE UNIQUE INDEX uq_us_order_intents_client_order_id
            ON us_order_intents (client_order_id)
            WHERE client_order_id IS NOT NULL;
        END IF;
    END IF;
END $$;

-- us_orders unique index
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'us_orders'
    ) THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE tablename = 'us_orders' AND indexname = 'uq_us_orders_client_order_id'
        ) THEN
            CREATE UNIQUE INDEX uq_us_orders_client_order_id
            ON us_orders (client_order_id)
            WHERE client_order_id IS NOT NULL;
        END IF;
    END IF;
END $$;
