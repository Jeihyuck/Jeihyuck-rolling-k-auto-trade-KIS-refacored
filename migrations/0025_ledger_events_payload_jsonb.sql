ALTER TABLE ledger_events
  ALTER COLUMN payload_json DROP DEFAULT;

ALTER TABLE ledger_events
  ALTER COLUMN payload_json TYPE jsonb
  USING
    CASE
      WHEN payload_json IS NULL OR btrim(payload_json) = '' THEN '{}'::jsonb
      WHEN left(btrim(payload_json), 1) IN ('{', '[') THEN payload_json::jsonb
      ELSE '{}'::jsonb
    END;

ALTER TABLE ledger_events
  ALTER COLUMN payload_json SET DEFAULT '{}'::jsonb;
