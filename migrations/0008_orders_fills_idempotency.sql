-- Orders/Fills idempotency + reconcile log + positions reconcile tracking

ALTER TABLE orders ADD COLUMN broker_order_id TEXT;
ALTER TABLE fills ADD COLUMN broker_fill_id TEXT;
ALTER TABLE positions ADD COLUMN last_reconciled_at TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_env_broker_order_id ON orders (env, broker_order_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_fills_env_broker_fill_id ON fills (env, broker_fill_id);

CREATE TABLE IF NOT EXISTS reconcile_log (
    env TEXT NOT NULL,
    strategy TEXT NOT NULL,
    tick_ts TEXT NOT NULL,
    action TEXT NOT NULL,
    details_json TEXT,
    created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);
