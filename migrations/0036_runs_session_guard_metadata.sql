ALTER TABLE public.runs
    ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE public.runs
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE public.runs
    ADD COLUMN IF NOT EXISTS aborted_reason TEXT;

ALTER TABLE public.runs
    ADD COLUMN IF NOT EXISTS takeover_from_run_id TEXT;

UPDATE public.runs
SET heartbeat_at = COALESCE(heartbeat_at, started_at, finished_at, CURRENT_TIMESTAMP)
WHERE heartbeat_at IS NULL;

UPDATE public.runs
SET updated_at = COALESCE(updated_at, heartbeat_at, started_at, finished_at, CURRENT_TIMESTAMP)
WHERE updated_at IS NULL;

ALTER TABLE public.runs
    ALTER COLUMN heartbeat_at SET DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE public.runs
    ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP;

DO $$
DECLARE
    takeover_type text;
BEGIN
    SELECT data_type
      INTO takeover_type
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = 'runs'
       AND column_name = 'takeover_from_run_id';

    IF takeover_type = 'uuid' THEN
        RAISE NOTICE '[DB][MIGRATE][TYPE_MISMATCH] version=0036 run_id_type=unknown takeover_from_run_id_type=% action=alter_to_text', takeover_type;
        ALTER TABLE public.runs
            ALTER COLUMN takeover_from_run_id TYPE TEXT
            USING takeover_from_run_id::text;
    ELSIF takeover_type IS NOT NULL AND takeover_type <> 'text' THEN
        RAISE NOTICE '[DB][MIGRATE][0036] takeover_from_run_id already exists with non-text type=%; forcing TEXT', takeover_type;
        ALTER TABLE public.runs
            ALTER COLUMN takeover_from_run_id TYPE TEXT
            USING takeover_from_run_id::text;
    END IF;
END $$;

DO $$
DECLARE
    run_id_type text;
    takeover_type text;
BEGIN
    SELECT data_type
      INTO run_id_type
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = 'runs'
       AND column_name = 'run_id';

    SELECT data_type
      INTO takeover_type
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = 'runs'
       AND column_name = 'takeover_from_run_id';

    IF run_id_type IS NULL OR takeover_type IS NULL THEN
        RAISE NOTICE '[DB][MIGRATE][0036] skip FK because column metadata is missing: run_id_type=%% takeover_from_run_id_type=%%', run_id_type, takeover_type;
        RETURN;
    END IF;

    IF run_id_type <> 'text' THEN
        RAISE NOTICE '[DB][MIGRATE][0036] skip FK because run_id is not text: run_id_type=%% takeover_from_run_id_type=%%', run_id_type, takeover_type;
        RETURN;
    END IF;

    IF takeover_type <> 'text' THEN
        RAISE NOTICE '[DB][MIGRATE][TYPE_MISMATCH] version=0036 run_id_type=%% takeover_from_run_id_type=%% action=skip_fk_until_text', run_id_type, takeover_type;
        RETURN;
    END IF;

    IF run_id_type <> takeover_type THEN
        RAISE NOTICE '[DB][MIGRATE][0036] skip FK because type mismatch remains: run_id_type=%% takeover_from_run_id_type=%%', run_id_type, takeover_type;
        RETURN;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = 'public'
          AND table_name = 'runs'
          AND constraint_name = 'runs_takeover_from_run_id_fkey'
    ) THEN
        ALTER TABLE public.runs
            ADD CONSTRAINT runs_takeover_from_run_id_fkey
            FOREIGN KEY (takeover_from_run_id) REFERENCES public.runs(run_id);
    END IF;
END $$;