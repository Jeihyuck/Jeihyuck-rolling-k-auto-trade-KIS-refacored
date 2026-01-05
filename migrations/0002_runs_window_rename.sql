DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='runs' AND column_name='window'
  ) THEN
    EXECUTE 'ALTER TABLE runs RENAME COLUMN "window" TO run_window';
  ELSIF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name='runs' AND column_name='run_window'
  ) THEN
    EXECUTE 'ALTER TABLE runs ADD COLUMN run_window text';
  END IF;
END $$;
