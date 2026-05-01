-- Migration: 0038_us_agent_tables.sql
-- US stock paper trading multi-agent system tables
-- Created: 2026-04-30

-- universe: 매매 가능 종목 목록
CREATE TABLE IF NOT EXISTS us_universe (
    id          SERIAL PRIMARY KEY,
    symbol      TEXT NOT NULL,
    exchange    TEXT NOT NULL,
    category    TEXT NOT NULL DEFAULT 'unknown',
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, exchange)
);

-- watchlist: 전략이 선별한 당일 관심 종목
CREATE TABLE IF NOT EXISTS us_watchlist (
    id          SERIAL PRIMARY KEY,
    trade_date  DATE NOT NULL,
    symbol      TEXT NOT NULL,
    exchange    TEXT NOT NULL,
    strategy    TEXT NOT NULL,
    score       NUMERIC(10, 4),
    meta        JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (trade_date, symbol, strategy)
);

-- order_intents: 전략이 생성한 주문 의도
CREATE TABLE IF NOT EXISTS us_order_intents (
    id                SERIAL PRIMARY KEY,
    trade_date        DATE NOT NULL,
    client_order_key  TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    exchange          TEXT NOT NULL,
    side              TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    qty               INTEGER NOT NULL CHECK (qty > 0),
    limit_price_usd   NUMERIC(12, 4),
    notional_usd      NUMERIC(12, 4),
    strategy          TEXT,
    status            TEXT NOT NULL DEFAULT 'PENDING',
    meta              JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (client_order_key)
);

-- orders: KIS API 발주 결과
CREATE TABLE IF NOT EXISTS us_orders (
    id                SERIAL PRIMARY KEY,
    trade_date        DATE NOT NULL,
    client_order_key  TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    exchange          TEXT NOT NULL,
    side              TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    qty_requested     INTEGER NOT NULL,
    qty_filled        INTEGER NOT NULL DEFAULT 0,
    avg_price_usd     NUMERIC(12, 4),
    order_no          TEXT,
    status            TEXT NOT NULL DEFAULT 'SENT',
    dry_run           BOOLEAN NOT NULL DEFAULT TRUE,
    meta              JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (client_order_key)
);

-- fills: 체결 내역
CREATE TABLE IF NOT EXISTS us_fills (
    id               SERIAL PRIMARY KEY,
    trade_date       DATE NOT NULL,
    symbol           TEXT NOT NULL,
    exchange         TEXT NOT NULL,
    side             TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    qty              INTEGER NOT NULL,
    price_usd        NUMERIC(12, 4) NOT NULL,
    order_no         TEXT,
    client_order_key TEXT,
    filled_at        TIMESTAMPTZ,
    meta             JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- positions: 포지션 스냅샷
CREATE TABLE IF NOT EXISTS us_positions (
    id          SERIAL PRIMARY KEY,
    as_of       DATE NOT NULL,
    symbol      TEXT NOT NULL,
    exchange    TEXT NOT NULL,
    qty         INTEGER NOT NULL DEFAULT 0,
    avg_cost    NUMERIC(12, 4),
    current_px  NUMERIC(12, 4),
    unrealized_pnl_usd NUMERIC(12, 4),
    meta        JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (as_of, symbol, exchange)
);

-- reconcile_logs: reconcile 실행 이력
CREATE TABLE IF NOT EXISTS us_reconcile_logs (
    id          SERIAL PRIMARY KEY,
    trade_date  DATE NOT NULL,
    status      TEXT NOT NULL,
    message     TEXT,
    meta        JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- agent_runs: agent 실행 이력
CREATE TABLE IF NOT EXISTS us_agent_runs (
    id          SERIAL PRIMARY KEY,
    run_id      TEXT NOT NULL DEFAULT gen_random_uuid()::TEXT,
    trade_date  DATE NOT NULL,
    agent_name  TEXT NOT NULL,
    mode        TEXT,
    env         TEXT NOT NULL DEFAULT 'practice',
    status      TEXT NOT NULL DEFAULT 'STARTED',
    result      JSONB,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

-- harness_runs: harness 시나리오 실행 이력
CREATE TABLE IF NOT EXISTS us_harness_runs (
    id            SERIAL PRIMARY KEY,
    run_id        TEXT NOT NULL DEFAULT gen_random_uuid()::TEXT,
    scenario_name TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('PASS', 'FAIL', 'SKIP')),
    stdout        TEXT,
    stderr        TEXT,
    elapsed_s     NUMERIC(8, 3),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- failure_events: failure_triage_agent 분류 이력
CREATE TABLE IF NOT EXISTS us_failure_events (
    id              SERIAL PRIMARY KEY,
    event_date      DATE NOT NULL DEFAULT CURRENT_DATE,
    workflow_run_id TEXT,
    failure_type    TEXT NOT NULL,
    log_snippet     TEXT,
    patch_plan      JSONB,
    resolved        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_us_watchlist_date    ON us_watchlist (trade_date);
CREATE INDEX IF NOT EXISTS idx_us_order_intents_date ON us_order_intents (trade_date);
CREATE INDEX IF NOT EXISTS idx_us_orders_date       ON us_orders (trade_date);
CREATE INDEX IF NOT EXISTS idx_us_fills_date        ON us_fills (trade_date);
CREATE INDEX IF NOT EXISTS idx_us_positions_date    ON us_positions (as_of);
CREATE INDEX IF NOT EXISTS idx_us_agent_runs_date   ON us_agent_runs (trade_date);
CREATE INDEX IF NOT EXISTS idx_us_harness_runs_date ON us_harness_runs (created_at);
CREATE INDEX IF NOT EXISTS idx_us_failure_events    ON us_failure_events (event_date);
