CREATE TABLE IF NOT EXISTS us_order_events (
    event_id UUID PRIMARY KEY,
    trade_date DATE NOT NULL,
    event_type TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    session TEXT,
    session_run_id TEXT,
    tick_id TEXT,
    client_order_key TEXT NOT NULL,
    submit_attempt_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    requested_qty NUMERIC NOT NULL,
    raw_broker_order_no TEXT,
    canonical_broker_order_no TEXT,
    position_lifecycle_id TEXT,
    profit_capture_stage TEXT,
    cumulative_filled_qty NUMERIC,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key TEXT UNIQUE NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_us_order_events_trade_date ON us_order_events(trade_date, event_timestamp);
CREATE INDEX IF NOT EXISTS ix_us_order_events_attempt ON us_order_events(trade_date, submit_attempt_id);

CREATE TABLE IF NOT EXISTS us_profit_capture_lifecycle (
    trade_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    position_lifecycle_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    stage_status TEXT NOT NULL,
    client_order_key TEXT,
    raw_broker_order_no TEXT,
    canonical_broker_order_no TEXT,
    requested_qty NUMERIC NOT NULL DEFAULT 0,
    cumulative_filled_qty NUMERIC NOT NULL DEFAULT 0,
    state JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_date, symbol, position_lifecycle_id, stage)
);
CREATE INDEX IF NOT EXISTS ix_us_profit_capture_current ON us_profit_capture_lifecycle(trade_date, symbol, position_lifecycle_id);
