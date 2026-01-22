-- PB-Core v2 base schema
-- Enum placeholders as TEXT to keep compatibility with sqlite tests.

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    env TEXT NOT NULL,
    strategy TEXT NOT NULL,
    run_window TEXT,
    phase TEXT,
    event_name TEXT,
    dry_run INTEGER NOT NULL DEFAULT 0,
    git_sha TEXT,
    workflow TEXT,
    workflow_run_id TEXT,
    workflow_attempt INTEGER,
    config_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'STARTED',
    started_at TEXT DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS universe (
    universe_id TEXT PRIMARY KEY,
    env TEXT NOT NULL,
    strategy TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    source TEXT NOT NULL,
    params_json TEXT NOT NULL DEFAULT '{}',
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_universe_env_strategy_as_of UNIQUE (env, strategy, as_of_date)
);

CREATE TABLE IF NOT EXISTS universe_members (
    universe_member_id TEXT PRIMARY KEY,
    universe_id TEXT NOT NULL REFERENCES universe(universe_id),
    code TEXT NOT NULL,
    market TEXT,
    weight REAL,
    rank INTEGER,
    meta_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    env TEXT NOT NULL,
    run_id TEXT REFERENCES runs(run_id),
    strategy TEXT NOT NULL,
    sid INTEGER NOT NULL,
    mode INTEGER NOT NULL,
    code TEXT NOT NULL,
    market TEXT,
    side TEXT NOT NULL,
    ord_type TEXT NOT NULL,
    qty INTEGER NOT NULL,
    limit_price REAL,
    stage TEXT,
    client_order_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'INTENT',
    kis_odno TEXT,
    request_json TEXT NOT NULL DEFAULT '{}',
    response_json TEXT,
    submitted_at TEXT,
    acked_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_orders_env_client_order_key UNIQUE (env, client_order_key)
);

CREATE TABLE IF NOT EXISTS fills (
    fill_id TEXT PRIMARY KEY,
    env TEXT NOT NULL,
    run_id TEXT REFERENCES runs(run_id),
    order_id TEXT REFERENCES orders(order_id),
    kis_odno TEXT,
    trade_id TEXT,
    code TEXT NOT NULL,
    market TEXT,
    side TEXT NOT NULL,
    qty INTEGER NOT NULL,
    price REAL NOT NULL,
    fee REAL NOT NULL DEFAULT 0.0,
    tax REAL NOT NULL DEFAULT 0.0,
    filled_at TEXT NOT NULL,
    raw_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fills_env_trade_id UNIQUE (env, trade_id),
    CONSTRAINT uq_fills_fallback UNIQUE (env, kis_odno, code, side, qty, price, filled_at)
);

CREATE TABLE IF NOT EXISTS positions (
    position_id TEXT PRIMARY KEY,
    env TEXT NOT NULL,
    strategy TEXT NOT NULL,
    sid INTEGER NOT NULL,
    mode INTEGER NOT NULL,
    code TEXT NOT NULL,
    market TEXT,
    qty INTEGER NOT NULL DEFAULT 0,
    avg_buy_price REAL,
    total_cost REAL NOT NULL DEFAULT 0.0,
    realized_pnl REAL NOT NULL DEFAULT 0.0,
    last_trade_at TEXT,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_positions_identity UNIQUE (env, strategy, sid, mode, code)
);

CREATE TABLE IF NOT EXISTS ledger_events (
    ledger_event_id TEXT PRIMARY KEY,
    env TEXT NOT NULL,
    run_id TEXT REFERENCES runs(run_id),
    event_type TEXT NOT NULL,
    ts TEXT NOT NULL,
    code TEXT,
    market TEXT,
    sid INTEGER,
    mode INTEGER,
    side TEXT,
    qty INTEGER,
    price REAL,
    kis_odno TEXT,
    client_order_key TEXT,
    ok INTEGER NOT NULL DEFAULT 1,
    reasons TEXT NOT NULL DEFAULT '[]',
    stage TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_runs_strategy_started_at ON runs (strategy, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_env_code_status ON orders (env, code, status);
CREATE INDEX IF NOT EXISTS idx_fills_env_code_filled_at ON fills (env, code, filled_at DESC);
CREATE INDEX IF NOT EXISTS idx_positions_env_updated_at ON positions (env, updated_at DESC);
