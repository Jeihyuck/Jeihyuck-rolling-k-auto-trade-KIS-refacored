-- Ensure ledger_events.ok default is TRUE (DB column_default)
ALTER TABLE ledger_events
  ALTER COLUMN ok SET DEFAULT TRUE;

-- (방어적으로 유지)
ALTER TABLE ledger_events
  ALTER COLUMN ok SET NOT NULL;