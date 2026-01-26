-- Change runs.dry_run from INTEGER to BOOLEAN safely
-- Fix: drop integer default before type cast

ALTER TABLE runs ALTER COLUMN dry_run DROP DEFAULT;

-- safety: if nulls exist
UPDATE runs SET dry_run = 0 WHERE dry_run IS NULL;

ALTER TABLE runs
  ALTER COLUMN dry_run TYPE BOOLEAN
  USING (dry_run <> 0);

ALTER TABLE runs ALTER COLUMN dry_run SET DEFAULT FALSE;
ALTER TABLE runs ALTER COLUMN dry_run SET NOT NULL;