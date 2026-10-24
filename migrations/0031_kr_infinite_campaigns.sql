CREATE TABLE IF NOT EXISTS kr_infinite_campaigns (
    env TEXT NOT NULL,
    strategy TEXT NOT NULL,
    code TEXT NOT NULL,
    cycle_id TEXT NOT NULL,
    started_on DATE NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    deployed_tranches INTEGER NOT NULL DEFAULT 0,
    last_fill_price DOUBLE PRECISION NOT NULL DEFAULT 0,
    closed_on DATE,
    close_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (env, strategy, code, cycle_id)
);

CREATE INDEX IF NOT EXISTS ix_kr_infinite_campaigns_active
    ON kr_infinite_campaigns (env, strategy, code, status, started_on DESC);
