-- Extend derived_minervini with env partition key and create derived_flow table.

ALTER TABLE IF EXISTS derived_minervini
    ADD COLUMN IF NOT EXISTS env TEXT;

UPDATE derived_minervini
SET env = COALESCE(NULLIF(TRIM(env), ''), 'practice')
WHERE env IS NULL OR TRIM(env) = '';

ALTER TABLE IF EXISTS derived_minervini
    ALTER COLUMN env SET NOT NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'derived_minervini'
          AND constraint_type = 'PRIMARY KEY'
          AND constraint_name = 'derived_minervini_pkey'
    ) THEN
        ALTER TABLE derived_minervini DROP CONSTRAINT derived_minervini_pkey;
    END IF;
END
$$;

ALTER TABLE IF EXISTS derived_minervini
    ADD CONSTRAINT derived_minervini_pkey PRIMARY KEY (env, symbol, as_of);

CREATE UNIQUE INDEX IF NOT EXISTS uq_derived_minervini_env_as_of_symbol
    ON derived_minervini (env, as_of, symbol);

CREATE INDEX IF NOT EXISTS idx_derived_minervini_env_as_of
    ON derived_minervini (env, as_of);

CREATE TABLE IF NOT EXISTS derived_flow (
    env TEXT NOT NULL,
    as_of DATE NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    flow_score DOUBLE PRECISION,
    foreign_20_ratio DOUBLE PRECISION,
    inst_20_ratio DOUBLE PRECISION,
    flow_missing BOOLEAN NOT NULL DEFAULT FALSE,
    source TEXT,
    features_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (env, as_of, symbol)
);

CREATE INDEX IF NOT EXISTS idx_derived_flow_env_as_of
    ON derived_flow (env, as_of);

CREATE INDEX IF NOT EXISTS idx_derived_flow_symbol_as_of
    ON derived_flow (symbol, as_of);
