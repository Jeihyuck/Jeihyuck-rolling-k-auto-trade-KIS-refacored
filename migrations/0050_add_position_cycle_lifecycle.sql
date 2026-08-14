-- Isolate KR position state by portfolio epoch and position lifecycle.
-- Existing rows remain available for audit, but are assigned a closed LEGACY cycle.

CREATE TABLE IF NOT EXISTS portfolio_epochs (
    portfolio_epoch_id TEXT PRIMARY KEY,
    env TEXT NOT NULL,
    account_id TEXT NOT NULL,
    sid INTEGER NOT NULL,
    mode INTEGER NOT NULL,
    strategy TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_portfolio_epochs_active
    ON portfolio_epochs (env, account_id, sid, mode, strategy)
    WHERE status = 'ACTIVE';

ALTER TABLE positions ADD COLUMN IF NOT EXISTS position_cycle_id TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS portfolio_epoch_id TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS opened_at TIMESTAMPTZ;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS position_origin TEXT;

-- Deterministic IDs make this migration idempotent and preserve every row.
UPDATE positions
SET position_cycle_id = COALESCE(position_cycle_id, 'legacy-cycle-' || position_id::text),
    portfolio_epoch_id = COALESCE(portfolio_epoch_id, 'legacy-epoch-' || env || '-' || strategy || '-' || sid || '-' || mode),
    opened_at = COALESCE(opened_at, last_trade_at, updated_at, NOW()),
    position_origin = COALESCE(position_origin, 'RECOVERY'),
    status = 'CLOSED',
    closed_ts = COALESCE(closed_ts, NOW()),
    closed_reason = COALESCE(closed_reason, 'LEGACY_MIGRATION_0050');

ALTER TABLE positions ALTER COLUMN position_cycle_id SET NOT NULL;
ALTER TABLE positions ALTER COLUMN portfolio_epoch_id SET NOT NULL;
ALTER TABLE positions ALTER COLUMN opened_at SET NOT NULL;
ALTER TABLE positions ALTER COLUMN position_origin SET NOT NULL;

ALTER TABLE positions DROP CONSTRAINT IF EXISTS uq_positions_identity;
CREATE UNIQUE INDEX IF NOT EXISTS uq_positions_cycle_id ON positions(position_cycle_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_positions_one_open_cycle
    ON positions(env, strategy, sid, mode, code, portfolio_epoch_id)
    WHERE status = 'OPEN';

ALTER TABLE orders ADD COLUMN IF NOT EXISTS position_cycle_id TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS portfolio_epoch_id TEXT;
ALTER TABLE fills ADD COLUMN IF NOT EXISTS position_cycle_id TEXT;
ALTER TABLE fills ADD COLUMN IF NOT EXISTS portfolio_epoch_id TEXT;

CREATE INDEX IF NOT EXISTS ix_orders_position_cycle ON orders(position_cycle_id, portfolio_epoch_id);
CREATE INDEX IF NOT EXISTS ix_fills_position_cycle ON fills(position_cycle_id, portfolio_epoch_id);
CREATE INDEX IF NOT EXISTS ix_positions_active_epoch ON positions(portfolio_epoch_id, status, code);

COMMENT ON COLUMN positions.position_cycle_id IS 'Immutable identity of one 0->positive->0 position lifecycle';
COMMENT ON COLUMN positions.position_origin IS 'SYSTEM, IMPORTED, or RECOVERY; legacy rows are RECOVERY+CLOSED';
