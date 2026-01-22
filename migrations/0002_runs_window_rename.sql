ALTER TABLE runs RENAME COLUMN "window" TO run_window;
ALTER TABLE runs ADD COLUMN run_window TEXT;
