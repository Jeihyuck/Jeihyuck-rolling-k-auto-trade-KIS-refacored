-- 0002_runs_window_rename.sql (sqlite/pg compatible)
-- Best-effort: some engines may fail if already applied; migrate runner must ignore benign errors.

ALTER TABLE runs RENAME COLUMN "window" TO run_window;
ALTER TABLE runs ADD COLUMN run_window TEXT;
