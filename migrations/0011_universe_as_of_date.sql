-- Align universe as_of columns to DATE where present
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'universe_runs'
      AND column_name = 'as_of'
  ) THEN
    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_name = 'universe_runs'
        AND column_name = 'as_of'
        AND data_type <> 'date'
    ) THEN
      ALTER TABLE universe_runs
        ALTER COLUMN as_of TYPE date
        USING (as_of::date);
    END IF;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'universe_current'
      AND column_name = 'as_of'
  ) THEN
    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_name = 'universe_current'
        AND column_name = 'as_of'
        AND data_type <> 'date'
    ) THEN
      ALTER TABLE universe_current
        ALTER COLUMN as_of TYPE date
        USING (as_of::date);
    END IF;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = 'universe_members'
      AND column_name = 'as_of'
  ) THEN
    IF EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_name = 'universe_members'
        AND column_name = 'as_of'
        AND data_type <> 'date'
    ) THEN
      ALTER TABLE universe_members
        ALTER COLUMN as_of TYPE date
        USING (as_of::date);
    END IF;
  END IF;
END $$;
