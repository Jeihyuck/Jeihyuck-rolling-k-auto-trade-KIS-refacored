CREATE TABLE IF NOT EXISTS execution_claims (
    action_key TEXT PRIMARY KEY,
    env TEXT NOT NULL,
    account_id TEXT NOT NULL,
    market TEXT NOT NULL,
    trading_epoch_id TEXT NOT NULL,
    strategy_owner TEXT NOT NULL,
    lifecycle_id TEXT NOT NULL,
    action TEXT NOT NULL,
    target_qty INTEGER NOT NULL CHECK (target_qty > 0),
    cumulative_filled_qty INTEGER,
    remaining_target_qty INTEGER,
    filled_qty_before_attempt INTEGER NOT NULL DEFAULT 0,
    active_attempt_id TEXT,
    action_state TEXT NOT NULL,
    claim_conflicts INTEGER NOT NULL DEFAULT 0,
    trade_date DATE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS execution_attempts (
    attempt_id TEXT PRIMARY KEY,
    action_key TEXT NOT NULL REFERENCES execution_claims(action_key),
    attempt_no INTEGER NOT NULL,
    requested_qty INTEGER NOT NULL CHECK (requested_qty > 0),
    client_order_key TEXT,
    cumulative_filled_qty INTEGER,
    attempt_state TEXT NOT NULL,
    authoritative BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (action_key, attempt_no)
);

CREATE INDEX IF NOT EXISTS ix_execution_claims_state
    ON execution_claims(action_state, updated_at);
CREATE INDEX IF NOT EXISTS ix_execution_attempts_client_key
    ON execution_attempts(client_order_key);

CREATE TABLE IF NOT EXISTS us_execution_claims (
    action_key TEXT PRIMARY KEY,
    env TEXT NOT NULL,
    account_id TEXT NOT NULL,
    market TEXT NOT NULL,
    trading_epoch_id TEXT NOT NULL,
    strategy_owner TEXT NOT NULL,
    lifecycle_id TEXT NOT NULL,
    action TEXT NOT NULL,
    target_qty INTEGER NOT NULL CHECK (target_qty > 0),
    cumulative_filled_qty INTEGER,
    remaining_target_qty INTEGER,
    filled_qty_before_attempt INTEGER NOT NULL DEFAULT 0,
    active_attempt_id TEXT,
    action_state TEXT NOT NULL,
    claim_conflicts INTEGER NOT NULL DEFAULT 0,
    trade_date DATE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS us_execution_attempts (
    attempt_id TEXT PRIMARY KEY,
    action_key TEXT NOT NULL REFERENCES us_execution_claims(action_key),
    attempt_no INTEGER NOT NULL,
    requested_qty INTEGER NOT NULL CHECK (requested_qty > 0),
    client_order_key TEXT,
    cumulative_filled_qty INTEGER,
    attempt_state TEXT NOT NULL,
    authoritative BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (action_key, attempt_no)
);

CREATE INDEX IF NOT EXISTS ix_us_execution_claims_state
    ON us_execution_claims(action_state, updated_at);
CREATE INDEX IF NOT EXISTS ix_us_execution_attempts_client_key
    ON us_execution_attempts(client_order_key);
