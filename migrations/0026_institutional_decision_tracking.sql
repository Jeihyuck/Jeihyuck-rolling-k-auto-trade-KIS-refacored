-- Migration 0026: Institutional Decision Tracking
-- PB1 + Minervini Institutional 확장
-- 매수/매도 의사결정 추적 및 비교 분석 기능

-- 1. Watchlist Snapshot (Final 30 선정 이유 저장)
CREATE TABLE IF NOT EXISTS watchlist_snapshot (
    id SERIAL PRIMARY KEY,
    as_of DATE NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    rank INT,
    tech_score FLOAT,
    flow_score FLOAT,
    final_score FLOAT,
    reasons JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_watchlist_snapshot_as_of 
    ON watchlist_snapshot(as_of);
CREATE INDEX IF NOT EXISTS idx_watchlist_snapshot_code 
    ON watchlist_snapshot(code);
CREATE INDEX IF NOT EXISTS idx_watchlist_snapshot_as_of_code 
    ON watchlist_snapshot(as_of, code);

-- 2. Minervini Snapshot (미너비니 통과 종목 이유 저장)
CREATE TABLE IF NOT EXISTS minervini_snapshot (
    id SERIAL PRIMARY KEY,
    as_of DATE NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    rs_percentile FLOAT,
    vcp_score FLOAT,
    trend_ok BOOLEAN,
    score FLOAT,
    reasons JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_minervini_snapshot_as_of 
    ON minervini_snapshot(as_of);
CREATE INDEX IF NOT EXISTS idx_minervini_snapshot_code 
    ON minervini_snapshot(code);
CREATE INDEX IF NOT EXISTS idx_minervini_snapshot_as_of_code 
    ON minervini_snapshot(as_of, code);

-- 3. Entry Decision Snapshot (매수 당시 상태 저장)
CREATE TABLE IF NOT EXISTS entry_decision_snapshot (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    as_of DATE NOT NULL,
    code TEXT NOT NULL,
    entry_price FLOAT,
    stop_price FLOAT,
    qty INT,
    features JSONB,
    reasons JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entry_decision_run_id 
    ON entry_decision_snapshot(run_id);
CREATE INDEX IF NOT EXISTS idx_entry_decision_code 
    ON entry_decision_snapshot(code);
CREATE INDEX IF NOT EXISTS idx_entry_decision_as_of 
    ON entry_decision_snapshot(as_of);

-- 4. Exit Analysis Snapshot (매도 시 비교 분석 저장)
CREATE TABLE IF NOT EXISTS exit_analysis_snapshot (
    id SERIAL PRIMARY KEY,
    code TEXT NOT NULL,
    entry_snapshot_id INT REFERENCES entry_decision_snapshot(id),
    exit_date DATE NOT NULL,
    exit_price FLOAT,
    pnl FLOAT,
    pnl_pct FLOAT,
    hold_days INT,
    comparison JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_exit_analysis_code 
    ON exit_analysis_snapshot(code);
CREATE INDEX IF NOT EXISTS idx_exit_analysis_entry_snapshot 
    ON exit_analysis_snapshot(entry_snapshot_id);
CREATE INDEX IF NOT EXISTS idx_exit_analysis_exit_date 
    ON exit_analysis_snapshot(exit_date);

-- Comments
COMMENT ON TABLE watchlist_snapshot IS 'Final 30 선정 이유 스냅샷';
COMMENT ON TABLE minervini_snapshot IS '미너비니 통과 종목 이유 스냅샷';
COMMENT ON TABLE entry_decision_snapshot IS '매수 당시 전체 상태 스냅샷';
COMMENT ON TABLE exit_analysis_snapshot IS '매도 시 매수 당시 vs 현재 비교 분석';
