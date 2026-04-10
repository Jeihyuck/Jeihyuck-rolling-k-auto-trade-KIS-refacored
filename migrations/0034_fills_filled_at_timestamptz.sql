CREATE OR REPLACE FUNCTION safe_parse_timestamptz(input_text TEXT)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
AS $$
BEGIN
    IF input_text IS NULL OR btrim(input_text) = '' THEN
        RETURN NULL;
    END IF;
    RETURN btrim(input_text)::timestamptz;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$;

DO $$
DECLARE
    filled_at_data_type TEXT;
    filled_at_udt_name TEXT;
    invalid_count BIGINT := 0;
    invalid_sample TEXT := '';
BEGIN
    SELECT data_type, udt_name
      INTO filled_at_data_type, filled_at_udt_name
      FROM information_schema.columns
     WHERE table_schema = current_schema()
       AND table_name = 'fills'
       AND column_name = 'filled_at';

    IF filled_at_udt_name = 'timestamptz' THEN
        RAISE NOTICE '[DB][MIGRATE][FILLS_FILLED_AT] already timestamptz -> no-op';
    ELSIF filled_at_data_type IN ('text', 'character varying') OR filled_at_udt_name IN ('text', 'varchar') THEN
        ALTER TABLE fills ADD COLUMN IF NOT EXISTS filled_at_ts TIMESTAMPTZ;

        SELECT COUNT(*)
          INTO invalid_count
          FROM fills
         WHERE NULLIF(btrim(filled_at), '') IS NOT NULL
           AND safe_parse_timestamptz(filled_at) IS NULL;

        SELECT COALESCE(string_agg(sample_value, ', '), '')
          INTO invalid_sample
          FROM (
                SELECT filled_at AS sample_value
                  FROM fills
                 WHERE NULLIF(btrim(filled_at), '') IS NOT NULL
                   AND safe_parse_timestamptz(filled_at) IS NULL
                 LIMIT 5
               ) samples;

        IF invalid_count > 0 THEN
          RAISE EXCEPTION '[DB][MIGRATE][FILLS_FILLED_AT][INVALID] count=%% sample=%%', invalid_count, invalid_sample;
        END IF;

        UPDATE fills
           SET filled_at_ts = safe_parse_timestamptz(filled_at)
         WHERE filled_at_ts IS NULL;

        ALTER TABLE fills ALTER COLUMN filled_at_ts SET NOT NULL;
        ALTER TABLE fills DROP CONSTRAINT IF EXISTS uq_fills_fallback;
        ALTER TABLE fills DROP COLUMN filled_at;
        ALTER TABLE fills RENAME COLUMN filled_at_ts TO filled_at;
        ALTER TABLE fills ADD CONSTRAINT uq_fills_fallback UNIQUE (env, kis_odno, code, side, qty, price, filled_at);
    ELSE
      RAISE NOTICE '[DB][MIGRATE][FILLS_FILLED_AT] skip unsupported type data_type=%% udt_name=%%', filled_at_data_type, filled_at_udt_name;
    END IF;

    SELECT udt_name
      INTO filled_at_udt_name
      FROM information_schema.columns
     WHERE table_schema = current_schema()
       AND table_name = 'fills'
       AND column_name = 'filled_at';

    IF filled_at_udt_name IS DISTINCT FROM 'timestamptz' THEN
      RAISE EXCEPTION '[DB][MIGRATE][FILLS_FILLED_AT][VERIFY_FAIL] actual_type=%% expected=timestamptz', filled_at_udt_name;
    END IF;
END;
$$;

DROP FUNCTION IF EXISTS safe_parse_timestamptz(TEXT);

CREATE INDEX IF NOT EXISTS idx_fills_env_filled_at_side_code ON fills (env, filled_at DESC, side, code);