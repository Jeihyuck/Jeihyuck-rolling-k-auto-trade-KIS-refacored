-- Migration 0021: Ensure pb1_watchlist.as_of is DATE type
-- Fixes type mismatch errors: "operator does not exist: date = character varying"

DO $$
BEGIN
  -- Check if as_of column exists and is not DATE type
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name='pb1_watchlist'
      AND column_name='as_of'
      AND data_type <> 'date'
  ) THEN
    -- Convert to DATE type
    ALTER TABLE pb1_watchlist
      ALTER COLUMN as_of TYPE date
      USING (as_of::date);
    
    RAISE NOTICE 'pb1_watchlist.as_of converted to DATE type';
  ELSE
    RAISE NOTICE 'pb1_watchlist.as_of already DATE type or column does not exist';
  END IF;
END $$;

-- Optional: Add index for faster lookups
CREATE INDEX IF NOT EXISTS idx_pb1_watchlist_env_strategy_asof_rank
ON pb1_watchlist (env, strategy, as_of, rank);

-- Log migration completion
DO $$
BEGIN
  RAISE NOTICE 'Migration 0021 completed: pb1_watchlist.as_of type enforcement';
END $$;
