-- Migration: 0019_run_id_uuid_migration.sql
-- Ensure run_id is UUID string only, separate gh_run_number

-- Step 1: Create temporary table for mapping integer run_ids to new UUIDs
CREATE TEMP TABLE run_id_mapping (
    old_run_id TEXT,
    new_run_id UUID DEFAULT gen_random_uuid()
);

-- Step 2: Identify non-UUID run_ids in runs table
INSERT INTO run_id_mapping (old_run_id)
SELECT run_id
FROM runs
WHERE run_id !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

-- Step 3: Update runs table: move integer run_ids to workflow_run_id, assign new UUIDs
UPDATE runs
SET
    workflow_run_id = CASE
        WHEN run_id !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        THEN run_id
        ELSE workflow_run_id
    END,
    run_id = COALESCE(
        (SELECT new_run_id FROM run_id_mapping WHERE old_run_id = runs.run_id),
        run_id
    )
WHERE run_id !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

-- Step 4: Update ledger_events: map run_ids using the mapping
UPDATE ledger_events
SET run_id = COALESCE(
    (SELECT new_run_id FROM run_id_mapping WHERE old_run_id = ledger_events.run_id::TEXT),
    ledger_events.run_id
)
WHERE run_id::TEXT !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

-- Step 5: Update orders table if it has run_id
-- Assuming orders has run_id FK
UPDATE orders
SET run_id = COALESCE(
    (SELECT new_run_id FROM run_id_mapping WHERE old_run_id = orders.run_id::TEXT),
    orders.run_id
)
WHERE run_id IS NOT NULL AND run_id::TEXT !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

-- Step 6: Update fills table if it has run_id
UPDATE fills
SET run_id = COALESCE(
    (SELECT new_run_id FROM run_id_mapping WHERE old_run_id = fills.run_id::TEXT),
    fills.run_id
)
WHERE run_id IS NOT NULL AND run_id::TEXT !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

-- Step 7: Add CHECK constraint to runs.run_id
ALTER TABLE runs ADD CONSTRAINT runs_run_id_uuid_check
CHECK (run_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$');

-- Step 8: Add CHECK constraint to ledger_events.run_id
ALTER TABLE ledger_events ADD CONSTRAINT ledger_events_run_id_uuid_check
CHECK (run_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$');

-- Step 9: Add CHECK constraint to orders.run_id if exists
-- Assuming orders has run_id column
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'orders' AND column_name = 'run_id') THEN
        EXECUTE 'ALTER TABLE orders ADD CONSTRAINT orders_run_id_uuid_check CHECK (run_id ~ ''^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'')';
    END IF;
END $$;

-- Step 10: Add CHECK constraint to fills.run_id if exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'fills' AND column_name = 'run_id') THEN
        EXECUTE 'ALTER TABLE fills ADD CONSTRAINT fills_run_id_uuid_check CHECK (run_id ~ ''^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'')';
    END IF;
END $$;

-- Step 11: Log the migration
INSERT INTO reconcile_log (env, strategy, tick_ts, action, details_json)
VALUES ('migration', 'run_id_uuid', NOW(), 'migrated', json_build_object('rows_updated', (SELECT COUNT(*) FROM run_id_mapping)));