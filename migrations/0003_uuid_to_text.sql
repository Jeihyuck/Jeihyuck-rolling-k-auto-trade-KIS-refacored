DO $$
BEGIN
  -- runs.run_id and related FKs
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'runs' AND column_name = 'run_id' AND data_type = 'uuid'
  ) THEN
    EXECUTE 'ALTER TABLE fills DROP CONSTRAINT IF EXISTS fills_run_id_fkey';
    EXECUTE 'ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_run_id_fkey';
    EXECUTE 'ALTER TABLE ledger_events DROP CONSTRAINT IF EXISTS ledger_events_run_id_fkey';
    EXECUTE 'ALTER TABLE runs ALTER COLUMN run_id DROP DEFAULT';
    EXECUTE 'ALTER TABLE runs ALTER COLUMN run_id TYPE text USING run_id::text';
    EXECUTE 'ALTER TABLE runs ALTER COLUMN run_id SET DEFAULT gen_random_uuid()::text';
    EXECUTE 'ALTER TABLE orders ALTER COLUMN run_id TYPE text USING run_id::text';
    EXECUTE 'ALTER TABLE fills ALTER COLUMN run_id TYPE text USING run_id::text';
    EXECUTE 'ALTER TABLE ledger_events ALTER COLUMN run_id TYPE text USING run_id::text';
    EXECUTE 'ALTER TABLE orders ADD CONSTRAINT orders_run_id_fkey FOREIGN KEY (run_id) REFERENCES runs(run_id)';
    EXECUTE 'ALTER TABLE fills ADD CONSTRAINT fills_run_id_fkey FOREIGN KEY (run_id) REFERENCES runs(run_id)';
    EXECUTE 'ALTER TABLE ledger_events ADD CONSTRAINT ledger_events_run_id_fkey FOREIGN KEY (run_id) REFERENCES runs(run_id)';
  END IF;

  -- universe.universe_id and related FKs
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'universe' AND column_name = 'universe_id' AND data_type = 'uuid'
  ) THEN
    EXECUTE 'ALTER TABLE universe_members DROP CONSTRAINT IF EXISTS universe_members_universe_id_fkey';
    EXECUTE 'ALTER TABLE universe ALTER COLUMN universe_id DROP DEFAULT';
    EXECUTE 'ALTER TABLE universe ALTER COLUMN universe_id TYPE text USING universe_id::text';
    EXECUTE 'ALTER TABLE universe ALTER COLUMN universe_id SET DEFAULT gen_random_uuid()::text';
    EXECUTE 'ALTER TABLE universe_members ALTER COLUMN universe_id TYPE text USING universe_id::text';
    EXECUTE 'ALTER TABLE universe_members ADD CONSTRAINT universe_members_universe_id_fkey FOREIGN KEY (universe_id) REFERENCES universe(universe_id)';
  END IF;

  -- orders.order_id and dependent FK
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'orders' AND column_name = 'order_id' AND data_type = 'uuid'
  ) THEN
    EXECUTE 'ALTER TABLE fills DROP CONSTRAINT IF EXISTS fills_order_id_fkey';
    EXECUTE 'ALTER TABLE orders ALTER COLUMN order_id DROP DEFAULT';
    EXECUTE 'ALTER TABLE orders ALTER COLUMN order_id TYPE text USING order_id::text';
    EXECUTE 'ALTER TABLE orders ALTER COLUMN order_id SET DEFAULT gen_random_uuid()::text';
    EXECUTE 'ALTER TABLE fills ALTER COLUMN order_id TYPE text USING order_id::text';
    EXECUTE 'ALTER TABLE fills ADD CONSTRAINT fills_order_id_fkey FOREIGN KEY (order_id) REFERENCES orders(order_id)';
  END IF;

  -- universe_members PK
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'universe_members' AND column_name = 'universe_member_id' AND data_type = 'uuid'
  ) THEN
    EXECUTE 'ALTER TABLE universe_members ALTER COLUMN universe_member_id DROP DEFAULT';
    EXECUTE 'ALTER TABLE universe_members ALTER COLUMN universe_member_id TYPE text USING universe_member_id::text';
    EXECUTE 'ALTER TABLE universe_members ALTER COLUMN universe_member_id SET DEFAULT gen_random_uuid()::text';
  END IF;

  -- fills PK
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'fills' AND column_name = 'fill_id' AND data_type = 'uuid'
  ) THEN
    EXECUTE 'ALTER TABLE fills ALTER COLUMN fill_id DROP DEFAULT';
    EXECUTE 'ALTER TABLE fills ALTER COLUMN fill_id TYPE text USING fill_id::text';
    EXECUTE 'ALTER TABLE fills ALTER COLUMN fill_id SET DEFAULT gen_random_uuid()::text';
  END IF;

  -- positions PK
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'positions' AND column_name = 'position_id' AND data_type = 'uuid'
  ) THEN
    EXECUTE 'ALTER TABLE positions ALTER COLUMN position_id DROP DEFAULT';
    EXECUTE 'ALTER TABLE positions ALTER COLUMN position_id TYPE text USING position_id::text';
    EXECUTE 'ALTER TABLE positions ALTER COLUMN position_id SET DEFAULT gen_random_uuid()::text';
  END IF;

  -- ledger_events PK
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'ledger_events' AND column_name = 'ledger_event_id' AND data_type = 'uuid'
  ) THEN
    EXECUTE 'ALTER TABLE ledger_events ALTER COLUMN ledger_event_id DROP DEFAULT';
    EXECUTE 'ALTER TABLE ledger_events ALTER COLUMN ledger_event_id TYPE text USING ledger_event_id::text';
    EXECUTE 'ALTER TABLE ledger_events ALTER COLUMN ledger_event_id SET DEFAULT gen_random_uuid()::text';
  END IF;
END $$;
