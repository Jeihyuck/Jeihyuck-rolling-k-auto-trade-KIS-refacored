-- Migration: 0019_run_id_uuid_migration.sql
-- Goal: enforce run_id UUID-only SSOT across all tables
-- Strategy: FK DROP -> child conversion -> parent conversion -> FK recreation

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE IF EXISTS public.runs
  ADD COLUMN IF NOT EXISTS workflow_run_id TEXT;

DO $$
DECLARE
  run_id_udt TEXT;
  fk RECORD;
  t RECORD;
  col_udt TEXT;
BEGIN
  SELECT udt_name INTO run_id_udt
  FROM information_schema.columns
  WHERE table_schema='public' AND table_name='runs' AND column_name='run_id';

  IF run_id_udt = 'uuid' THEN
    RAISE NOTICE 'runs.run_id already UUID; skipping.';
    RETURN;
  END IF;

  --------------------------------------------------------------------
  -- 1) Drop all FKs referencing public.runs(run_id) (pg_catalog 기반, 누락 방지)
  --------------------------------------------------------------------
  FOR fk IN
    SELECT
      n.nspname  AS table_schema,
      c.relname  AS table_name,
      con.conname AS constraint_name
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE con.contype = 'f'
      AND con.confrelid = 'public.runs'::regclass
  LOOP
    RAISE NOTICE 'Dropping FK: %.% -> %', fk.table_schema, fk.table_name, fk.constraint_name;
    EXECUTE 'ALTER TABLE '
      || quote_ident(fk.table_schema) || '.' || quote_ident(fk.table_name)
      || ' DROP CONSTRAINT ' || quote_ident(fk.constraint_name);
  END LOOP;

  --------------------------------------------------------------------
  -- 1.5) 방어적 점검: FK가 남아있는지 확인
  --------------------------------------------------------------------
  DECLARE
    remaining_fk_count INTEGER;
  BEGIN
    SELECT COUNT(*) INTO remaining_fk_count
    FROM pg_constraint con
    WHERE con.contype = 'f'
      AND con.confrelid = 'public.runs'::regclass;
    
    IF remaining_fk_count > 0 THEN
      RAISE EXCEPTION 'FK constraints still reference runs(run_id) after drop attempt: % remaining', remaining_fk_count;
    END IF;
  END;

  --------------------------------------------------------------------
  -- 2) Mapping legacy text run_id -> uuid
  --------------------------------------------------------------------
  CREATE TEMP TABLE run_id_mapping(
    old_run_id TEXT PRIMARY KEY,
    new_run_id UUID NOT NULL DEFAULT gen_random_uuid()
  ) ON COMMIT DROP;

  INSERT INTO run_id_mapping(old_run_id)
  SELECT run_id::text
  FROM public.runs
  WHERE run_id::text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

  --------------------------------------------------------------------
  -- 3) Convert child tables first: run_id TEXT -> UUID
  --------------------------------------------------------------------
  FOR t IN
    SELECT table_schema, table_name
    FROM information_schema.columns
    WHERE table_schema='public'
      AND column_name='run_id'
      AND table_name <> 'runs'
    GROUP BY table_schema, table_name
  LOOP
    SELECT udt_name INTO col_udt
    FROM information_schema.columns
    WHERE table_schema=t.table_schema AND table_name=t.table_name AND column_name='run_id';

    IF col_udt = 'uuid' THEN
      CONTINUE;
    END IF;

    -- replace legacy values using mapping (if any)
    IF EXISTS (SELECT 1 FROM run_id_mapping) THEN
      EXECUTE
        'UPDATE ' || quote_ident(t.table_schema) || '.' || quote_ident(t.table_name) || ' c '
        || 'SET run_id = m.new_run_id::text '
        || 'FROM run_id_mapping m '
        || 'WHERE c.run_id::text = m.old_run_id';
    END IF;

    -- assert remaining values uuid-shaped
    DECLARE
      bad_count INTEGER;
    BEGIN
      EXECUTE
        'SELECT COUNT(*) FROM ' || quote_ident(t.table_schema) || '.' || quote_ident(t.table_name)
        || ' WHERE run_id IS NOT NULL '
        || '   AND run_id::text !~ ''^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'''
      INTO bad_count;
      
      IF bad_count > 0 THEN
        RAISE EXCEPTION 'Cannot cast run_id to UUID in table: non-uuid strings remain'
          USING HINT = 'Table: ' || quote_ident(t.table_schema) || '.' || quote_ident(t.table_name);
      END IF;
    END;

    EXECUTE
      'ALTER TABLE ' || quote_ident(t.table_schema) || '.' || quote_ident(t.table_name)
      || ' ALTER COLUMN run_id TYPE UUID USING run_id::uuid';
  END LOOP;

  --------------------------------------------------------------------
  -- 4) Convert runs.run_id after children
  --------------------------------------------------------------------
  UPDATE public.runs
  SET workflow_run_id = COALESCE(workflow_run_id, run_id::text);

  IF EXISTS (SELECT 1 FROM run_id_mapping) THEN
    UPDATE public.runs r
    SET run_id = m.new_run_id::text
    FROM run_id_mapping m
    WHERE r.run_id::text = m.old_run_id;
  END IF;

  IF EXISTS (
    SELECT 1 FROM public.runs
    WHERE run_id::text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
  ) THEN
    RAISE EXCEPTION 'Cannot cast runs.run_id to UUID: non-uuid strings remain.';
  END IF;

  EXECUTE 'ALTER TABLE public.runs ALTER COLUMN run_id TYPE UUID USING run_id::uuid';

  --------------------------------------------------------------------
  -- 5) Recreate FKs with IF NOT EXISTS protection
  --------------------------------------------------------------------
  FOR t IN
    SELECT table_schema, table_name
    FROM information_schema.columns
    WHERE table_schema='public'
      AND column_name='run_id'
      AND table_name <> 'runs'
    GROUP BY table_schema, table_name
  LOOP
    -- Check if FK already exists before creating
    IF NOT EXISTS (
      SELECT 1 FROM pg_constraint con
      JOIN pg_class c ON c.oid = con.conrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE con.contype = 'f'
        AND n.nspname = t.table_schema
        AND c.relname = t.table_name
        AND con.conname = t.table_name || '_run_id_fkey'
    ) THEN
      RAISE NOTICE 'Creating FK: %.% -> runs(run_id)', t.table_schema, t.table_name;
      EXECUTE
        'ALTER TABLE ' || quote_ident(t.table_schema) || '.' || quote_ident(t.table_name)
        || ' ADD CONSTRAINT ' || quote_ident(t.table_name || '_run_id_fkey')
        || ' FOREIGN KEY (run_id) REFERENCES public.runs(run_id) ON DELETE CASCADE';
    ELSE
      RAISE NOTICE 'FK already exists: %.%_run_id_fkey', t.table_schema, t.table_name;
    END IF;
  END LOOP;

END $$;

COMMIT;