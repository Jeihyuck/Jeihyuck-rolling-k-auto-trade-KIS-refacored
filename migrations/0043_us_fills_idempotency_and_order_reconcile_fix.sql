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

CREATE INDEX IF NOT EXISTS idx_us_orders_trade_date_status
ON us_orders (trade_date, status);

CREATE INDEX IF NOT EXISTS idx_us_orders_order_no
ON us_orders (order_no);