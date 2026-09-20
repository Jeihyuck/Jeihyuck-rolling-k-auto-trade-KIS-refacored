-- Global logical trading epoch shared by KR and US practice trading.
-- Existing history is retained and explicitly tagged as LEGACY_PRE_EPOCH.
-- A KIS practice-account reset should be followed by scripts/start_new_practice_trading_epoch.py.

CREATE TABLE IF NOT EXISTS trading_epochs (
    trading_epoch_id TEXT PRIMARY KEY,
    env TEXT NOT NULL,
    account_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_trading_epochs_active
    ON trading_epochs(env, account_id)
    WHERE status='ACTIVE';

INSERT INTO trading_epochs(trading_epoch_id, env, account_id, status, reason, ended_at)
VALUES ('LEGACY_PRE_EPOCH', 'legacy', 'legacy', 'ENDED', 'PRE_GLOBAL_EPOCH_HISTORY', NOW())
ON CONFLICT (trading_epoch_id) DO NOTHING;

ALTER TABLE portfolio_epochs ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE fills ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;

UPDATE portfolio_epochs SET trading_epoch_id='LEGACY_PRE_EPOCH' WHERE trading_epoch_id IS NULL;
UPDATE orders SET trading_epoch_id='LEGACY_PRE_EPOCH' WHERE trading_epoch_id IS NULL;
UPDATE fills SET trading_epoch_id='LEGACY_PRE_EPOCH' WHERE trading_epoch_id IS NULL;
UPDATE positions SET trading_epoch_id='LEGACY_PRE_EPOCH' WHERE trading_epoch_id IS NULL;

CREATE INDEX IF NOT EXISTS ix_portfolio_epochs_trading_epoch ON portfolio_epochs(trading_epoch_id, status);
CREATE INDEX IF NOT EXISTS ix_orders_trading_epoch ON orders(trading_epoch_id, env, created_at);
CREATE INDEX IF NOT EXISTS ix_fills_trading_epoch ON fills(trading_epoch_id, env, filled_at);
CREATE INDEX IF NOT EXISTS ix_positions_trading_epoch ON positions(trading_epoch_id, env, status, code);

ALTER TABLE IF EXISTS us_order_intents ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE IF EXISTS us_orders ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE IF EXISTS us_fills ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE IF EXISTS us_positions ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE IF EXISTS us_reconcile_logs ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE IF EXISTS us_position_risk_state ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
ALTER TABLE IF EXISTS us_profit_capture_lifecycle ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;

UPDATE us_order_intents SET trading_epoch_id='LEGACY_PRE_EPOCH' WHERE trading_epoch_id IS NULL;
UPDATE us_orders SET trading_epoch_id='LEGACY_PRE_EPOCH' WHERE trading_epoch_id IS NULL;
UPDATE us_fills SET trading_epoch_id='LEGACY_PRE_EPOCH' WHERE trading_epoch_id IS NULL;
UPDATE us_positions SET trading_epoch_id='LEGACY_PRE_EPOCH' WHERE trading_epoch_id IS NULL;
UPDATE us_reconcile_logs SET trading_epoch_id='LEGACY_PRE_EPOCH' WHERE trading_epoch_id IS NULL;
UPDATE us_position_risk_state SET trading_epoch_id='LEGACY_PRE_EPOCH' WHERE trading_epoch_id IS NULL;
UPDATE us_profit_capture_lifecycle SET trading_epoch_id='LEGACY_PRE_EPOCH' WHERE trading_epoch_id IS NULL;

CREATE INDEX IF NOT EXISTS ix_us_order_intents_trading_epoch ON us_order_intents(trading_epoch_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_us_orders_trading_epoch ON us_orders(trading_epoch_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_us_fills_trading_epoch ON us_fills(trading_epoch_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_us_positions_trading_epoch ON us_positions(trading_epoch_id, as_of);

ALTER TABLE IF EXISTS us_tqqq_infinite_state ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
UPDATE us_tqqq_infinite_state SET trading_epoch_id='LEGACY_PRE_EPOCH' WHERE trading_epoch_id IS NULL;
ALTER TABLE IF EXISTS us_tqqq_infinite_state ALTER COLUMN trading_epoch_id SET NOT NULL;
ALTER TABLE IF EXISTS us_tqqq_infinite_state DROP CONSTRAINT IF EXISTS us_tqqq_infinite_state_pkey;
ALTER TABLE IF EXISTS us_tqqq_infinite_state
    ADD CONSTRAINT us_tqqq_infinite_state_pkey PRIMARY KEY(trading_epoch_id, strategy_id, symbol);

ALTER TABLE IF EXISTS kr_infinite_state ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
UPDATE kr_infinite_state SET trading_epoch_id='LEGACY_PRE_EPOCH' WHERE trading_epoch_id IS NULL;
ALTER TABLE IF EXISTS kr_infinite_state ALTER COLUMN trading_epoch_id SET NOT NULL;
ALTER TABLE IF EXISTS kr_infinite_state DROP CONSTRAINT IF EXISTS kr_infinite_state_pkey;
ALTER TABLE IF EXISTS kr_infinite_state
    ADD CONSTRAINT kr_infinite_state_pkey PRIMARY KEY(trading_epoch_id, strategy_id, symbol);

ALTER TABLE IF EXISTS kr_infinite_order_intents ADD COLUMN IF NOT EXISTS trading_epoch_id TEXT;
UPDATE kr_infinite_order_intents SET trading_epoch_id='LEGACY_PRE_EPOCH' WHERE trading_epoch_id IS NULL;
CREATE INDEX IF NOT EXISTS ix_kr_inf_intent_trading_epoch
    ON kr_infinite_order_intents(trading_epoch_id, trade_date, status);

COMMENT ON TABLE trading_epochs IS 'Account-wide logical trading run boundary shared by KR and US';
COMMENT ON COLUMN orders.trading_epoch_id IS 'Global KR/US trading epoch identity';
COMMENT ON COLUMN us_orders.trading_epoch_id IS 'Global KR/US trading epoch identity';
