-- 0002: make sure runs has run_window (safe for fresh DB; migrator splits by ;)
ALTER TABLE IF EXISTS runs
  ADD COLUMN IF NOT EXISTS run_window TEXT;
