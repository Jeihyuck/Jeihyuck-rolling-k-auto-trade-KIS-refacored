-- PB-Core v2 base schema
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Enum placeholders as TEXT to keep compatibility with sqlite tests.

CREATE TABLE IF NOT EXISTS runs (
    run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    env text NOT NULL,
    strategy text NOT NULL,
    run_window text,
    phase text,
    event_name text,
    dry_run boolean NOT NULL DEFAULT false,
    git_sha text,
    workflow text,
    workflow_run_id text,
    workflow_attempt int,
    config_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL DEFAULT 'STARTED',
    started_at timestamptz DEFAULT now(),
    finished_at timestamptz,
    notes text
);

CREATE TABLE IF NOT EXISTS universe (
    universe_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    env text NOT NULL,
    strategy text NOT NULL,
    as_of_date text NOT NULL,
    source text NOT NULL,
    params_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz DEFAULT now(),
    CONSTRAINT uq_universe_env_strategy_as_of UNIQUE (env, strategy, as_of_date)
);

CREATE TABLE IF NOT EXISTS universe_members (
    universe_member_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    universe_id uuid NOT NULL REFERENCES universe(universe_id),
    code text NOT NULL,
    market text,
    weight double precision,
    rank int,
    meta_json jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS orders (
    order_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    env text NOT NULL,
    run_id uuid REFERENCES runs(run_id),
    strategy text NOT NULL,
    sid int NOT NULL,
    mode int NOT NULL,
    code text NOT NULL,
    market text,
    side text NOT NULL,
    ord_type text NOT NULL,
    qty int NOT NULL,
    limit_price double precision,
    stage text,
    client_order_key text NOT NULL,
    status text NOT NULL DEFAULT 'INTENT',
    kis_odno text,
    request_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    response_json jsonb,
    submitted_at timestamptz,
    acked_at timestamptz,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),
    CONSTRAINT uq_orders_env_client_order_key UNIQUE (env, client_order_key)
);

CREATE TABLE IF NOT EXISTS fills (
    fill_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    env text NOT NULL,
    run_id uuid REFERENCES runs(run_id),
    order_id uuid REFERENCES orders(order_id),
    kis_odno text,
    trade_id text,
    code text NOT NULL,
    market text,
    side text NOT NULL,
    qty int NOT NULL,
    price double precision NOT NULL,
    fee double precision NOT NULL DEFAULT 0.0,
    tax double precision NOT NULL DEFAULT 0.0,
    filled_at timestamptz NOT NULL,
    raw_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz DEFAULT now(),
    CONSTRAINT uq_fills_env_trade_id UNIQUE (env, trade_id),
    CONSTRAINT uq_fills_fallback UNIQUE (env, kis_odno, code, side, qty, price, filled_at)
);

CREATE TABLE IF NOT EXISTS positions (
    position_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    env text NOT NULL,
    strategy text NOT NULL,
    sid int NOT NULL,
    mode int NOT NULL,
    code text NOT NULL,
    market text,
    qty int NOT NULL DEFAULT 0,
    avg_buy_price double precision,
    total_cost double precision NOT NULL DEFAULT 0.0,
    realized_pnl double precision NOT NULL DEFAULT 0.0,
    last_trade_at timestamptz,
    updated_at timestamptz DEFAULT now(),
    CONSTRAINT uq_positions_identity UNIQUE (env, strategy, sid, mode, code)
);

CREATE TABLE IF NOT EXISTS ledger_events (
    ledger_event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    env text NOT NULL,
    run_id uuid REFERENCES runs(run_id),
    event_type text NOT NULL,
    ts timestamptz NOT NULL,
    code text,
    market text,
    sid int,
    mode int,
    side text,
    qty int,
    price double precision,
    kis_odno text,
    client_order_key text,
    ok boolean NOT NULL DEFAULT true,
    reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
    stage text,
    payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz DEFAULT now()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_runs_strategy_started_at ON runs (strategy, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_env_code_status ON orders (env, code, status);
CREATE INDEX IF NOT EXISTS idx_fills_env_code_filled_at ON fills (env, code, filled_at DESC);
CREATE INDEX IF NOT EXISTS idx_positions_env_updated_at ON positions (env, updated_at DESC);
