-- Universe snapshot tables with current pointer

ALTER TABLE universe_members RENAME TO universe_members_legacy;
ALTER TABLE universe RENAME TO universe_legacy;

CREATE TABLE IF NOT EXISTS universe_runs (
  run_id TEXT PRIMARY KEY,
  strategy TEXT NOT NULL,
  provider TEXT NOT NULL,
  as_of DATE NOT NULL,
  created_ts TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_universe_runs_key
ON universe_runs(strategy, provider, as_of);

CREATE TABLE IF NOT EXISTS universe_members (
  run_id TEXT NOT NULL,
  stock_code TEXT NOT NULL,
  name TEXT,
  market TEXT,
  rank INTEGER,
  market_cap REAL,
  reason TEXT,
  PRIMARY KEY (run_id, stock_code),
  FOREIGN KEY (run_id) REFERENCES universe_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_universe_members_code ON universe_members(stock_code);

CREATE TABLE IF NOT EXISTS universe_current (
  strategy TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  updated_ts TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES universe_runs(run_id) ON DELETE RESTRICT
);
