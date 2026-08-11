-- Additive, rollback-compatible metadata for the isolated TQQQ Infinite sleeve.
-- Broker positions, orders and fills remain authoritative in the existing us_* tables.
CREATE TABLE IF NOT EXISTS us_tqqq_infinite_state (
    strategy_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    cycle_id TEXT,
    cycle_start_date DATE,
    cycle_complete_date DATE,
    anchor_price NUMERIC(20, 8),
    core_filled_notional NUMERIC(20, 4) NOT NULL DEFAULT 0,
    reserve_filled_notional NUMERIC(20, 4) NOT NULL DEFAULT 0,
    last_buy_date DATE,
    last_exit_date DATE,
    market_crash_streak INTEGER NOT NULL DEFAULT 0,
    material_market_crash BOOLEAN NOT NULL DEFAULT FALSE,
    reserve_unlocked BOOLEAN NOT NULL DEFAULT FALSE,
    cycle_age_trading_days INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'READY',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (strategy_id, symbol),
    CONSTRAINT ck_us_tqqq_inf_notionals_nonnegative CHECK (
        core_filled_notional >= 0 AND reserve_filled_notional >= 0
    ),
    CONSTRAINT ck_us_tqqq_inf_crash_streak_nonnegative CHECK (market_crash_streak >= 0),
    CONSTRAINT ck_us_tqqq_inf_cycle_age_nonnegative CHECK (cycle_age_trading_days >= 0)
);

CREATE INDEX IF NOT EXISTS ix_us_tqqq_infinite_state_status
    ON us_tqqq_infinite_state (status, updated_at);
