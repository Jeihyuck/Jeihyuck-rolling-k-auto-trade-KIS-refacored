ALTER TABLE public.runs
    ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE public.runs
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE public.runs
    ADD COLUMN IF NOT EXISTS aborted_reason TEXT;

ALTER TABLE public.runs
    ADD COLUMN IF NOT EXISTS takeover_from_run_id UUID;

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
BEGIN
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