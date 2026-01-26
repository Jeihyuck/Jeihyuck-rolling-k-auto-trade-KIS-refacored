-- Change runs.dry_run from INTEGER to BOOLEAN for consistency

ALTER TABLE runs ALTER COLUMN dry_run TYPE BOOLEAN USING (dry_run <> 0);
ALTER TABLE runs ALTER COLUMN dry_run SET DEFAULT FALSE;
ALTER TABLE runs ALTER COLUMN dry_run SET NOT NULL;