-- KR/US additive settlement ledger. Existing order/fill/position rows are retained.
-- Explicit migration only; live writers remain disabled until release gating.
CREATE TABLE IF NOT EXISTS broker_settlement_evidence (
    evidence_id TEXT PRIMARY KEY,
    settlement_key TEXT NOT NULL,
    env TEXT NOT NULL,
    market TEXT NOT NULL CHECK (market IN ('KR','US')),
    account_scope TEXT NOT NULL,
    trading_epoch_id TEXT NOT NULL,
    strategy_owner TEXT NOT NULL,
    position_cycle_id TEXT NOT NULL,
    client_order_key TEXT NOT NULL,
    broker_trade_date DATE NOT NULL,
    exchange TEXT NOT NULL,
    broker_order_no TEXT,
    side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
    evidence_type TEXT NOT NULL,
    evidence_digest TEXT NOT NULL,
    confirmed_cumulative_qty INTEGER NOT NULL CHECK (confirmed_cumulative_qty >= 0),
    execution_price NUMERIC(24,8),
    currency TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_broker_settlement_evidence_key
    ON broker_settlement_evidence(settlement_key, observed_at DESC);

CREATE TABLE IF NOT EXISTS settlement_applications (
    settlement_key TEXT PRIMARY KEY,
    env TEXT NOT NULL,
    market TEXT NOT NULL CHECK (market IN ('KR','US')),
    account_scope TEXT NOT NULL,
    trading_epoch_id TEXT NOT NULL,
    strategy_owner TEXT NOT NULL,
    position_cycle_id TEXT NOT NULL,
    client_order_key TEXT NOT NULL,
    broker_trade_date DATE NOT NULL,
    exchange TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
    currency TEXT NOT NULL,
    applied_qty INTEGER NOT NULL DEFAULT 0 CHECK (applied_qty >= 0),
    applied_notional NUMERIC(24,8) NOT NULL DEFAULT 0,
    price_status TEXT NOT NULL DEFAULT 'PRICE_PENDING',
    settlement_status TEXT NOT NULL DEFAULT 'QUANTITY_PENDING',
    last_evidence_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_settlement_applications_unresolved
    ON settlement_applications(market, env, trading_epoch_id, settlement_status, updated_at);
