-- Migration 0027: Run ID Removal and DB Namespace Normalization
-- 
-- Phase 1: Code changes (deploy FIRST before running this migration)
-- Phase 2: Database schema changes (run AFTER code is deployed and tested)
-- 
-- WARNING: This is a BREAKING change. Follow the deployment sequence carefully.

-- =============================================================================
-- PHASE 1: CODE DEPLOYMENT (BEFORE THIS MIGRATION)
-- =============================================================================
-- 
-- 1. Deploy code changes:
--    - trader/types.py (new file)
--    - trader/run_context.py (env → account_env + exec_mode)
--    - trader/db/repos.py (remove run_id filters)
--    - trader/watchlist_loader.py (new file)
-- 
-- 2. Verify in staging:
--    - Check "members=0 (no run)" is gone from logs
--    - Verify universe/watchlist load without run_id
--    - Confirm trade latency < 10s
-- 
-- 3. Monitor production for 24-48 hours
-- 
-- =============================================================================
-- PHASE 2: DATABASE SCHEMA CHANGES (AFTER CODE IS STABLE)
-- =============================================================================

-- Step 1: Remove run_id from universe_runs (already has unique constraint on strategy+provider+as_of)
-- 
-- NOTE: universe_runs.run_id is only used internally, NOT as a foreign key filter.
-- The real query key is (strategy, provider, as_of).
-- 
-- We keep run_id as primary key for now (for backward compatibility with joins),
-- but remove it from query filters in code.

-- Step 2: Drop run_id columns from transactional tables (OPTIONAL - can defer)
-- 
-- WARNING: This breaks any external tools that rely on run_id.
-- Only proceed if you're certain no external dependencies exist.

-- Uncomment the following to drop run_id from orders/fills/ledger_events:
-- 
-- ALTER TABLE orders DROP COLUMN IF EXISTS run_id;
-- ALTER TABLE fills DROP COLUMN IF EXISTS run_id;
-- ALTER TABLE ledger_events DROP COLUMN IF EXISTS run_id;

-- Step 3: Add indexes for new query patterns (account_env based)
-- 
-- These indexes optimize queries using (env, strategy, as_of) tuples.

CREATE INDEX IF NOT EXISTS idx_universe_runs_env_strategy_asof 
    ON universe_runs(strategy, as_of DESC);

CREATE INDEX IF NOT EXISTS idx_pb1_watchlist_env_strategy_asof 
    ON pb1_watchlist(env, strategy, as_of DESC);

-- Step 4: Cleanup old indexes (OPTIONAL)
-- 
-- If run_id is no longer used for queries, these indexes can be dropped:
-- 
-- DROP INDEX IF EXISTS idx_orders_run_id;
-- DROP INDEX IF EXISTS idx_fills_run_id;
-- DROP INDEX IF EXISTS idx_ledger_events_run_id;

-- =============================================================================
-- VALIDATION QUERIES
-- =============================================================================

-- Check universe without run_id:
-- 
-- SELECT strategy, as_of, COUNT(*) as members
-- FROM universe_runs ur
-- JOIN universe_members um ON ur.run_id = um.run_id
-- WHERE ur.strategy = 'practice:best_k_meta'
--   AND ur.as_of = '2026-02-13'
-- GROUP BY strategy, as_of;

-- Check watchlist without run_id:
-- 
-- SELECT env, strategy, as_of, COUNT(*) as members
-- FROM pb1_watchlist
-- WHERE env = 'practice'
--   AND strategy = 'best_k_meta'
--   AND as_of = '2026-02-13'
-- GROUP BY env, strategy, as_of;

-- =============================================================================
-- ROLLBACK PLAN
-- =============================================================================

-- If issues arise, revert code deployment FIRST.
-- Database schema changes (index creation) are safe and don't need rollback.
-- 
-- If you dropped run_id columns and need to rollback:
-- 
-- ALTER TABLE orders ADD COLUMN run_id TEXT;
-- ALTER TABLE fills ADD COLUMN run_id TEXT;
-- ALTER TABLE ledger_events ADD COLUMN run_id TEXT;
-- 
-- Then backfill run_id from runs table or use a placeholder value.

-- =============================================================================
-- NOTES
-- =============================================================================

-- 1. run_id removal is GRADUAL:
--    - Code removes run_id from filters (Phase 1)
--    - Database keeps run_id column for history (Phase 2 optional)
--    - Dropping columns can happen in a future migration
-- 
-- 2. DB namespace normalization:
--    - All queries use (account_env, strategy, as_of) tuples
--    - account_env MUST be one of: practice, paper, real
--    - exec_mode (LIVE/DIAG/SIM) is runtime-only, NOT in DB
-- 
-- 3. Migration is SAFE:
--    - Indexes are additive (no data loss)
--    - Column drops are OPTIONAL and deferred
--    - Code changes are backward compatible (run_id deprecated, not removed)
