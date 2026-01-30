-- Migration: Add PB1_WATCHLIST table
-- Description: Create table to store PB1 daily watchlist cache

CREATE TABLE IF NOT EXISTS pb1_watchlist (
    env TEXT NOT NULL,
    strategy TEXT NOT NULL,
    as_of DATE NOT NULL,
    code TEXT NOT NULL,
    rank INTEGER NOT NULL,
    score FLOAT,
    meta JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (env, strategy, as_of, code)
);

CREATE INDEX IF NOT EXISTS ix_pb1_watchlist_lookup 
    ON pb1_watchlist (env, strategy, as_of);

COMMENT ON TABLE pb1_watchlist IS 'PB1 daily watchlist cache - stores pre-filtered candidates';
COMMENT ON COLUMN pb1_watchlist.env IS 'Environment (live/paper)';
COMMENT ON COLUMN pb1_watchlist.strategy IS 'Strategy name (e.g., best_k_meta)';
COMMENT ON COLUMN pb1_watchlist.as_of IS 'Trading day (KST date)';
COMMENT ON COLUMN pb1_watchlist.code IS 'Stock code (6 digits)';
COMMENT ON COLUMN pb1_watchlist.rank IS 'Rank within watchlist (1=best)';
COMMENT ON COLUMN pb1_watchlist.score IS 'Combined score (RS + VCP)';
COMMENT ON COLUMN pb1_watchlist.meta IS 'Metadata (liq_avg, rs_pctile, vcp_score, etc.)';
