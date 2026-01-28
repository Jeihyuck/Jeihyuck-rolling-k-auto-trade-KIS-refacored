-- Migration: 0019_run_id_uuid_migration.sql
-- Goal: enforce run_id UUID-only SSOT, move legacy run ids to workflow_run_id, update all FK run_id columns automatically.

BEGIN;

-- 0) uuid generator
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) ensure workflow_run_id exists
ALTER TABLE IF EXISTS public.runs
  ADD COLUMN IF NOT EXISTS workflow_run_id TEXT;

-- 2) Detect runs.run_id type
DO $$
DECLARE
  run_id_udt TEXT;
BEGIN
  SELECT udt_name INTO run_id_udt
  FROM information_schema.columns
  WHERE table_schema='public' AND table_name='runs' AND column_name='run_id';

  -- If runs.run_id already uuid => nothing to do here.
  IF run_id_udt = 'uuid' THEN
    RAISE NOTICE 'runs.run_id already UUID - skipping legacy migration steps.';
    RETURN;
  END IF;

  -- 3) Mapping old run_id (text) -> new UUID
  CREATE TEMP TABLE run_id_mapping(
    old_run_id TEXT PRIMARY KEY,
    new_run_id UUID NOT NULL DEFAULT gen_random_uuid()
  ) ON COMMIT DROP;

  -- Collect all non-uuid shaped run_id values (legacy)
  INSERT INTO run_id_mapping(old_run_id)
  SELECT run_id::text
  FROM public.runs
  WHERE run_id::text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

  -- If nothing legacy, we still may want to cast column to UUID (but only if all rows are UUID-shaped)
  IF NOT EXISTS (SELECT 1 FROM run_id_mapping) THEN
    -- verify all are uuid-shaped before casting
    IF EXISTS (
      SELECT 1 FROM public.runs
      WHERE run_id::text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    ) THEN
      RAISE EXCEPTION 'runs.run_id contains non-uuid strings but mapping empty?';
    END IF;
  ELSE
    -- 4) Create new runs rows with UUID run_id (parent prepared first => FK safe)
    -- Copy essential columns dynamically:
    -- We assume runs has columns: run_id, env, strategy, mode, ts_start, ts_end, status, payload_json, ... etc.
    -- We'll insert with run_id=new_uuid, workflow_run_id=old_run_id, and copy other columns by selecting runs.*
    -- To avoid listing all columns manually, we build dynamic SQL.
    PERFORM 1;

    -- Create a minimal insert that works even if schema differs:
    -- - If your runs table has more NOT NULL columns, you MUST handle them here.
    -- Strategy: Insert only known columns that exist.
    -- We'll generate column lists excluding run_id and workflow_run_id, and re-insert them.
    -- Note: Based on schema.py, NOT NULL columns are env, strategy, dry_run, config_json, status.
    -- Since we're copying from existing rows, they should have values.
    DECLARE
      cols TEXT;
      cols_select TEXT;
      sql TEXT;
    BEGIN
      SELECT string_agg(quote_ident(column_name), ', ')
      INTO cols
      FROM information_schema.columns
      WHERE table_schema='public' AND table_name='runs'
        AND column_name NOT IN ('run_id','workflow_run_id');

      SELECT string_agg('r.'||quote_ident(column_name), ', ')
      INTO cols_select
      FROM information_schema.columns
      WHERE table_schema='public' AND table_name='runs'
        AND column_name NOT IN ('run_id','workflow_run_id');

      sql := 'INSERT INTO public.runs (run_id, workflow_run_id' ||
             CASE WHEN cols IS NULL THEN '' ELSE ', ' || cols END ||
             ') SELECT m.new_run_id, m.old_run_id' ||
             CASE WHEN cols_select IS NULL THEN '' ELSE ', ' || cols_select END ||
             ' FROM public.runs r JOIN run_id_mapping m ON m.old_run_id = r.run_id::text' ||
             ' ON CONFLICT DO NOTHING';

      EXECUTE sql;
    END;

    -- 5) Update ALL child tables that have a run_id column (auto-discovery)
    -- We update only rows whose run_id matches old_run_id (text)
    DECLARE
      rec RECORD;
      upd_sql TEXT;
    BEGIN
      FOR rec IN
        SELECT table_schema, table_name, column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema='public'
          AND column_name='run_id'
          AND table_name <> 'runs'
      LOOP
        upd_sql := 'UPDATE ' || quote_ident(rec.table_schema) || '.' || quote_ident(rec.table_name) || ' t ' ||
                   'SET run_id = m.new_run_id ' ||
                   'FROM run_id_mapping m ' ||
                   'WHERE t.run_id::text = m.old_run_id';
        EXECUTE upd_sql;
      END LOOP;
    END;

    -- 6) Move legacy runs.run_id into workflow_run_id for the old rows (optional cleanup)
    UPDATE public.runs r
    SET workflow_run_id = COALESCE(workflow_run_id, r.run_id::text)
    WHERE r.run_id::text IN (SELECT old_run_id FROM run_id_mapping);

    -- 7) Delete legacy runs rows (safe now because children updated to new UUID rows)
    DELETE FROM public.runs r
    WHERE r.run_id::text IN (SELECT old_run_id FROM run_id_mapping);

  END IF;

  -- 8) Now cast runs.run_id and all child run_id columns to UUID
  -- Only cast if column type is not uuid already.
  DECLARE
    rec2 RECORD;
    cast_sql TEXT;
  BEGIN
    -- runs first
    SELECT udt_name INTO run_id_udt
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='runs' AND column_name='run_id';

    IF run_id_udt <> 'uuid' THEN
      -- assert all are uuid-shaped before cast
      IF EXISTS (
        SELECT 1 FROM public.runs
        WHERE run_id::text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      ) THEN
        RAISE EXCEPTION 'Cannot cast runs.run_id to UUID: non-uuid strings remain.';
      END IF;

      EXECUTE 'ALTER TABLE public.runs ALTER COLUMN run_id TYPE UUID USING run_id::uuid';
    END IF;

    -- children
    FOR rec2 IN
      SELECT table_schema, table_name
      FROM information_schema.columns
      WHERE table_schema='public' AND column_name='run_id' AND table_name <> 'runs'
      GROUP BY table_schema, table_name
    LOOP
      -- check type
      SELECT udt_name INTO run_id_udt
      FROM information_schema.columns
      WHERE table_schema=rec2.table_schema AND table_name=rec2.table_name AND column_name='run_id';

      IF run_id_udt <> 'uuid' THEN
        -- assert all are uuid-shaped
        cast_sql := 'DO $x$ BEGIN ' ||
                    'IF EXISTS ( ' ||
                    'SELECT 1 FROM ' || quote_ident(rec2.table_schema) || '.' || quote_ident(rec2.table_name) || ' ' ||
                    'WHERE run_id IS NOT NULL ' ||
                    'AND run_id::text !~ ''^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'' ' ||
                    ') THEN ' ||
                    'RAISE EXCEPTION ''Cannot cast ' || rec2.table_schema || '.' || rec2.table_name || ' run_id to UUID: non-uuid strings remain.''; ' ||
                    'END IF; ' ||
                    'END $x$;';
        EXECUTE cast_sql;

        EXECUTE 'ALTER TABLE ' || quote_ident(rec2.table_schema) || '.' || quote_ident(rec2.table_name) ||
                ' ALTER COLUMN run_id TYPE UUID USING run_id::uuid';
      END IF;
    END LOOP;
  END;

END $$;

COMMIT;