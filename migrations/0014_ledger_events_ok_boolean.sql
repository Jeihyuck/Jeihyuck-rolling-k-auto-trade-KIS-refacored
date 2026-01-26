-- Change ledger_events.ok from INTEGER to BOOLEAN (safe cast)
ALTER TABLE ledger_events
  ALTER COLUMN ok DROP DEFAULT;

ALTER TABLE ledger_events
  ALTER COLUMN ok TYPE BOOLEAN
  USING (ok <> 0);

ALTER TABLE ledger_events
  ALTER COLUMN ok SET DEFAULT TRUE;

ALTER TABLE ledger_events
  ALTER COLUMN ok SET NOT NULL;