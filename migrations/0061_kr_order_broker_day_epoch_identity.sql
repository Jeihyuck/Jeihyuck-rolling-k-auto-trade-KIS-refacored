-- KR 2026-11-02: KIS ODNO is not globally unique over the lifetime of an account.
-- A date and trading epoch are mandatory parts of broker order identity.
-- No order/fill/position rows are updated or deleted by this migration.
-- Preflight: abort rather than collapse historical duplicates.
DO $kr_order_identity$
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.orders
        WHERE broker_order_id IS NOT NULL
        GROUP BY env, broker_order_id,
                 ((COALESCE(submitted_at, acked_at, created_at) AT TIME ZONE 'Asia/Seoul')::date),
                 COALESCE(trading_epoch_id::text, '__legacy__')
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'KR_BROKER_ORDER_SCOPED_IDENTITY_DUPLICATE';
    END IF;
END $kr_order_identity$;

-- Keep unique guards WITHIN the same broker day and account epoch.
-- The execution timestamp is immutable evidence; created_at is legacy fallback.
CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_env_broker_order_day_epoch
ON public.orders (
    env,
    broker_order_id,
    ((COALESCE(submitted_at, acked_at, created_at) AT TIME ZONE 'Asia/Seoul')::date),
    (COALESCE(trading_epoch_id::text, '__legacy__'))
)
WHERE broker_order_id IS NOT NULL;

-- Only remove the obsolete globally scoped index after the replacement exists.
DROP INDEX IF EXISTS public.uq_orders_env_broker_order_id;
