-- Migration 0022: Add unique constraint to pb1_watchlist
-- Enables safe UPSERT operations (INSERT ... ON CONFLICT DO UPDATE)

-- Create unique index on (env, strategy, as_of, code)
-- This prevents duplicate entries and enables efficient upserts
CREATE UNIQUE INDEX IF NOT EXISTS uq_pb1_watchlist_key
ON pb1_watchlist (env, strategy, as_of, code);

-- Log migration completion
DO $$
BEGIN
  RAISE NOTICE 'Migration 0022 completed: pb1_watchlist unique constraint added';
END $$;
