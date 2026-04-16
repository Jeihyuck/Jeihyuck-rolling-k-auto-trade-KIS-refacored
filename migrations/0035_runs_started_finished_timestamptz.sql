DO $$
DECLARE
    started_type text;
    finished_type text;
BEGIN
    IF to_regclass('public.runs') IS NULL THEN
        RAISE NOTICE 'runs table missing; skipping timestamptz normalization';
        RETURN;
    END IF;

    SELECT data_type
    INTO started_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'runs' AND column_name = 'started_at';

    IF started_type IN ('text', 'character varying', 'character') THEN
        UPDATE public.runs
        SET started_at = NULL
        WHERE started_at IS NOT NULL AND btrim(started_at) IN ('', 'None', 'null');

        EXECUTE $sql$
            ALTER TABLE public.runs
            ALTER COLUMN started_at TYPE timestamptz
            USING CASE
                WHEN started_at IS NULL THEN NULL
                WHEN btrim(started_at) IN ('', 'None', 'null') THEN NULL
                WHEN started_at ~ '^\d{4}-\d{2}-\d{2}' THEN started_at::timestamptz
                ELSE NULL
            END
        $sql$;

        EXECUTE 'ALTER TABLE public.runs ALTER COLUMN started_at SET DEFAULT CURRENT_TIMESTAMP';
    END IF;

    SELECT data_type
    INTO finished_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'runs' AND column_name = 'finished_at';

    IF finished_type IN ('text', 'character varying', 'character') THEN
        UPDATE public.runs
        SET finished_at = NULL
        WHERE finished_at IS NOT NULL AND btrim(finished_at) IN ('', 'None', 'null');

        EXECUTE $sql$
            ALTER TABLE public.runs
            ALTER COLUMN finished_at TYPE timestamptz
            USING CASE
                WHEN finished_at IS NULL THEN NULL
                WHEN btrim(finished_at) IN ('', 'None', 'null') THEN NULL
                WHEN finished_at ~ '^\d{4}-\d{2}-\d{2}' THEN finished_at::timestamptz
                ELSE NULL
            END
        $sql$;
    END IF;
END $$;