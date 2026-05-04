-- Migration: 0039_us_prep_locked_watchlist_contract.sql
-- US Prep → Trade locked watchlist contract enforcement
-- Created: 2026-05-04
-- 
-- Purpose:
--   한국장 pb1_watchlist(final30) → trade 구조를 미국장에도 적용.
--   us-trade-prep이 당일 locked watchlist를 생성하고,
--   us-trade-am/afternoon은 이 locked watchlist만 authoritative input으로 사용.
--
-- Changes:
--   1. us_watchlist에 prep_status, locked, data_source 필드 추가
--   2. us_agent_runs에 prep 상태 추적 필드 보강
--   3. prep run 조회용 인덱스 추가

-- ============================================================================
-- 1. us_watchlist: locked watchlist contract 필드 추가
-- ============================================================================

-- prep_status: prep run 상태 (OK, OK_WITH_WARNINGS, DEGRADED, ERROR)
ALTER TABLE us_watchlist 
ADD COLUMN IF NOT EXISTS prep_status TEXT;

-- locked: 이 watchlist가 prep에서 locked된 최종 input인지 여부
ALTER TABLE us_watchlist 
ADD COLUMN IF NOT EXISTS locked BOOLEAN NOT NULL DEFAULT FALSE;

-- run_id: 이 watchlist를 생성한 prep run_id
ALTER TABLE us_watchlist 
ADD COLUMN IF NOT EXISTS run_id TEXT;

-- data_source: 데이터 출처 (kis, db_fallback, precomputed, stale_fallback)
ALTER TABLE us_watchlist 
ADD COLUMN IF NOT EXISTS data_source TEXT;

-- updated_at: watchlist 갱신 시각
ALTER TABLE us_watchlist 
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

COMMENT ON COLUMN us_watchlist.prep_status IS 'Prep run status: OK|OK_WITH_WARNINGS|DEGRADED|ERROR';
COMMENT ON COLUMN us_watchlist.locked IS 'True if this is the locked authoritative watchlist for trade sessions';
COMMENT ON COLUMN us_watchlist.run_id IS 'Prep run_id that generated this watchlist entry';
COMMENT ON COLUMN us_watchlist.data_source IS 'Data source: kis|db_fallback|precomputed|stale_fallback';

-- ============================================================================
-- 2. us_agent_runs: prep status 추적 필드 보강
-- ============================================================================

-- 기존 result JSONB에 다음 필드들이 저장됨을 명시:
-- {
--   "trade_date": "2026-05-04",
--   "watchlist_count": 23,
--   "data_error_count": 28,
--   "strategy_skip_count": 26,
--   "critical_etf_errors": ["QQQ", "SPY"],
--   "status_reason": "critical_market_data_failed",
--   "source": "us_trade_prep",
--   "locked": true,
--   "data_error_rate": 0.28,
--   "degraded_reason": "...",
--   "warnings": [...]
-- }

COMMENT ON COLUMN us_agent_runs.result IS 'Prep result summary including watchlist_count, data_error_count, critical_etf_errors, status_reason';

-- ============================================================================
-- 3. 인덱스: prep run 조회 최적화
-- ============================================================================

-- trade_date + agent_name + mode로 최신 prep run 조회
CREATE INDEX IF NOT EXISTS idx_us_agent_runs_prep_lookup 
ON us_agent_runs (trade_date, agent_name, mode, started_at DESC) 
WHERE agent_name = 'us_prep' AND mode = 'prep';

-- locked watchlist 조회 최적화
CREATE INDEX IF NOT EXISTS idx_us_watchlist_locked 
ON us_watchlist (trade_date, locked, prep_status) 
WHERE locked = TRUE;

-- run_id로 watchlist 역참조
CREATE INDEX IF NOT EXISTS idx_us_watchlist_run_id 
ON us_watchlist (run_id) 
WHERE run_id IS NOT NULL;

-- ============================================================================
-- 4. UNIQUE constraint 보강
-- ============================================================================

-- 기존 UNIQUE (trade_date, symbol, strategy)는 유지
-- 단, 하나의 symbol이 여러 strategy에 의해 선정될 수 있으므로 이 제약은 적절함.

-- ============================================================================
-- 5. 데이터 검증 함수 (선택적)
-- ============================================================================

-- prep run status 검증
CREATE OR REPLACE FUNCTION validate_us_prep_status(p_status TEXT) 
RETURNS BOOLEAN AS $$
BEGIN
    RETURN p_status IN ('OK', 'OK_WITH_WARNINGS', 'DEGRADED', 'ERROR');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION validate_us_prep_status IS 'Validates us prep status values';

-- CHECK constraint 추가 (선택적, 기존 데이터가 있으면 주의)
-- ALTER TABLE us_watchlist 
-- ADD CONSTRAINT check_prep_status 
-- CHECK (prep_status IS NULL OR validate_us_prep_status(prep_status));

-- ============================================================================
-- 6. View: 최신 locked watchlist 조회 (편의용)
-- ============================================================================

CREATE OR REPLACE VIEW v_us_latest_locked_watchlist AS
SELECT 
    w.*,
    r.started_at AS prep_run_started_at,
    r.finished_at AS prep_run_finished_at,
    r.status AS prep_run_status,
    r.result AS prep_run_result
FROM us_watchlist w
LEFT JOIN us_agent_runs r ON w.run_id = r.run_id
WHERE w.locked = TRUE
  AND w.trade_date = CURRENT_DATE;

COMMENT ON VIEW v_us_latest_locked_watchlist IS 'Latest locked watchlist for today with prep run metadata';

-- ============================================================================
-- 완료
-- ============================================================================

-- Migration complete.
-- Next steps:
--   1. repos.py에 load_locked_us_watchlist(), save_us_prep_run() 함수 추가
--   2. prep_runner.py에서 watchlist 저장 시 locked=True, prep_status 설정
--   3. trade_tick_runner.py에서 locked watchlist 로드 후 get_all_tickers() 대신 사용
--   4. prep status가 DEGRADED이면 trade session에서 신규 BUY 차단
