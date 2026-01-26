-- Add status tracking to universe runs

ALTER TABLE universe_runs
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'SUCCESS';

ALTER TABLE universe_runs
  ADD COLUMN IF NOT EXISTS error_reason TEXT;

ALTER TABLE universe_runs
  ADD COLUMN IF NOT EXISTS members_count INTEGER;

