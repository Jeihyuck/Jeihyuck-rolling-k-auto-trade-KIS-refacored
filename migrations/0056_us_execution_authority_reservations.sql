-- US execution authority and account-wide reservation primitives.
-- Migration is additive; no worker/submit flag is enabled by this DDL.
-- All SQL writers must hold the current lease under a row lock; lease expiry
-- does not permit implicit release of an unresolved broker reservation.
CREATE TABLE IF NOT EXISTS us_execution_authority_leases (
    env TEXT NOT NULL,
    account_fingerprint TEXT NOT NULL,
    trading_epoch_id TEXT NOT NULL,
    holder_id TEXT NOT NULL,
    fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (env, account_fingerprint, trading_epoch_id)
);
CREATE TABLE IF NOT EXISTS us_execution_account_reservations (
    action_key TEXT PRIMARY KEY,
    env TEXT NOT NULL,
    account_fingerprint TEXT NOT NULL,
    trading_epoch_id TEXT NOT NULL,
    strategy_owner TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
    requested_qty INTEGER NOT NULL CHECK (requested_qty > 0),
    reserved_cash_usd NUMERIC(24,6) NOT NULL DEFAULT 0 CHECK (reserved_cash_usd >= 0),
    reserved_shares INTEGER NOT NULL DEFAULT 0 CHECK (reserved_shares >= 0),
    state TEXT NOT NULL DEFAULT 'RESERVED' CHECK (state IN ('RESERVED','UNCERTAIN','RELEASED','CONSUMED')),
    holder_id TEXT NOT NULL,
    fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
    terminal_evidence TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((side='BUY' AND reserved_cash_usd > 0 AND reserved_shares=0)
        OR (side='SELL' AND reserved_shares > 0 AND reserved_cash_usd=0))
);
CREATE INDEX IF NOT EXISTS ix_us_execution_reservation_scope
  ON us_execution_account_reservations(env, account_fingerprint, trading_epoch_id, state, side, symbol);

-- Supabase public is an exposed schema. No anon/authenticated policy is
-- intentionally granted; only the trusted database owner/service role may
-- operate account-wide reservations. Deployment must separately audit grants.
ALTER TABLE us_execution_authority_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE us_execution_account_reservations ENABLE ROW LEVEL SECURITY;
