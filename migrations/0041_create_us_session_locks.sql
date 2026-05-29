-- migrations/0041_create_us_session_locks.sql
-- US 세션 중복 실행 방지용 DB session lock 테이블.
-- UNIQUE(market, env, trade_date, session)으로 하루에 1회 실행 보장.

CREATE TABLE IF NOT EXISTS us_session_locks (
    id BIGSERIAL PRIMARY KEY,
    market TEXT NOT NULL DEFAULT 'US',
    env TEXT NOT NULL,
    trade_date DATE NOT NULL,
    session TEXT NOT NULL,
    status TEXT NOT NULL,
    github_run_id TEXT,
    github_workflow TEXT,
    github_run_attempt TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    heartbeat_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    result_status TEXT,
    reason TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_us_session_once UNIQUE (market, env, trade_date, session)
);

CREATE INDEX IF NOT EXISTS idx_us_session_locks_date
ON us_session_locks (trade_date, env, session);

CREATE INDEX IF NOT EXISTS idx_us_session_locks_status
ON us_session_locks (status, updated_at);
