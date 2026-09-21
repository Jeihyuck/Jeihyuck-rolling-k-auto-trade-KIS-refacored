-- Unified KR/US trading generation boundary.
-- Existing history is preserved. Legacy transaction rows remain unscoped (NULL);
-- state tables that require a composite key are assigned to one ENDED legacy epoch.

CREATE TABLE IF NOT EXISTS trading_epochs (
    trading_epoch_id TEXT PRIMARY KEY,
    env TEXT NOT NULL,
    account_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_trading_epochs_status CHECK (status IN ('ACTIVE','ENDED'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_trading_epochs_active
    ON trading_epochs(env, account_id)
    WHERE status='ACTIVE';

INSERT INTO trading_epochs(
    trading_epoch_id, env, account_id, started_at, ended_at, status, reason
) VALUES (
    'legacy-unscoped-0052', 'legacy', 'legacy', NOW(), NOW(), 'ENDED',
    'MIGRATION_0052_LEGACY_STATE'
) ON CONFLICT (trading_epoch_id) DO NOTHING;

ALTER TABLE portfolio_epochs ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE fills ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;

CREATE INDEX IF NOT EXISTS ix_portfolio_epochs_trading_epoch
    ON portfolio_epochs(trading_epoch_id, status);
CREATE INDEX IF NOT EXISTS ix_orders_trading_epoch
    ON orders(trading_epoch_id, created_at);
CREATE INDEX IF NOT EXISTS ix_fills_trading_epoch
    ON fills(trading_epoch_id, filled_at);
CREATE INDEX IF NOT EXISTS ix_positions_trading_epoch
    ON positions(trading_epoch_id, status, code);

ALTER TABLE us_order_intents ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE us_orders ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE us_fills ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE us_positions ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE us_order_events ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE us_profit_capture_lifecycle ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;

CREATE INDEX IF NOT EXISTS ix_us_order_intents_trading_epoch
    ON us_order_intents(trading_epoch_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_us_orders_trading_epoch
    ON us_orders(trading_epoch_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_us_fills_trading_epoch
    ON us_fills(trading_epoch_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_us_order_events_trading_epoch
    ON us_order_events(trading_epoch_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_us_profit_capture_trading_epoch
    ON us_profit_capture_lifecycle(trading_epoch_id, trade_date);

ALTER TABLE us_positions DROP CONSTRAINT IF EXISTS us_positions_as_of_symbol_exchange_key;
DROP INDEX IF EXISTS uq_us_positions_epoch_asof_symbol_exchange;
CREATE UNIQUE INDEX uq_us_positions_epoch_asof_symbol_exchange
    ON us_positions(trading_epoch_id, as_of, symbol, exchange);

CREATE TABLE IF NOT EXISTS us_position_risk_state (
    trade_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    soft_stop_breach_count INTEGER NOT NULL DEFAULT 0,
    first_soft_stop_seen_at TIMESTAMPTZ,
    last_soft_stop_seen_at TIMESTAMPTZ,
    lowest_price_since_breach NUMERIC,
    last_price NUMERIC,
    last_pnl_pct NUMERIC,
    state JSONB DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE us_position_risk_state ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE us_position_risk_state DROP CONSTRAINT IF EXISTS us_position_risk_state_pkey;
DROP INDEX IF EXISTS uq_us_position_risk_state_epoch_date_symbol;
CREATE UNIQUE INDEX uq_us_position_risk_state_epoch_date_symbol
    ON us_position_risk_state(trading_epoch_id, trade_date, symbol);

ALTER TABLE us_tqqq_infinite_state ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
UPDATE us_tqqq_infinite_state
SET trading_epoch_id='legacy-unscoped-0052'
WHERE trading_epoch_id IS NULL;
ALTER TABLE us_tqqq_infinite_state ALTER COLUMN trading_epoch_id SET NOT NULL;
ALTER TABLE us_tqqq_infinite_state DROP CONSTRAINT IF EXISTS us_tqqq_infinite_state_pkey;
ALTER TABLE us_tqqq_infinite_state
    ADD CONSTRAINT us_tqqq_infinite_state_pkey
    PRIMARY KEY(trading_epoch_id, strategy_id, symbol);

ALTER TABLE kr_infinite_state ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
UPDATE kr_infinite_state
SET trading_epoch_id='legacy-unscoped-0052'
WHERE trading_epoch_id IS NULL;
ALTER TABLE kr_infinite_state ALTER COLUMN trading_epoch_id SET NOT NULL;
ALTER TABLE kr_infinite_state DROP CONSTRAINT IF EXISTS kr_infinite_state_pkey;
ALTER TABLE kr_infinite_state
    ADD CONSTRAINT kr_infinite_state_pkey
    PRIMARY KEY(trading_epoch_id, strategy_id, symbol);

ALTER TABLE kr_infinite_order_intents ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
UPDATE kr_infinite_order_intents
SET trading_epoch_id='legacy-unscoped-0052'
WHERE trading_epoch_id IS NULL;
ALTER TABLE kr_infinite_order_intents ALTER COLUMN trading_epoch_id SET NOT NULL;
ALTER TABLE kr_infinite_order_intents
    DROP CONSTRAINT IF EXISTS kr_infinite_order_intents_idempotency_key_key;
DROP INDEX IF EXISTS uq_kr_infinite_intent_epoch_key;
CREATE UNIQUE INDEX uq_kr_infinite_intent_epoch_key
    ON kr_infinite_order_intents(trading_epoch_id, idempotency_key);

CREATE INDEX IF NOT EXISTS ix_us_tqqq_inf_trading_epoch
    ON us_tqqq_infinite_state(trading_epoch_id, status, updated_at);
CREATE INDEX IF NOT EXISTS ix_kr_inf_state_trading_epoch
    ON kr_infinite_state(trading_epoch_id, status, updated_at);
CREATE INDEX IF NOT EXISTS ix_kr_inf_intent_trading_epoch
    ON kr_infinite_order_intents(trading_epoch_id, status, updated_at);
