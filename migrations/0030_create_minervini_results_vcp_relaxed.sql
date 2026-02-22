-- minervini relaxed-vcp results table (final30 input only, minervini_test)
CREATE TABLE IF NOT EXISTS minervini_results_vcp_relaxed (
  run_date                    DATE NOT NULL,
  env                         TEXT NOT NULL,
  symbol                      TEXT NOT NULL,
  name                        TEXT NULL,

  rs_pass                     BOOLEAN NOT NULL DEFAULT FALSE,
  trend_pass                  BOOLEAN NOT NULL DEFAULT FALSE,
  vcp_pass                    BOOLEAN NOT NULL DEFAULT FALSE,
  final_pass                  BOOLEAN NOT NULL DEFAULT FALSE,

  fail_reasons                JSONB NOT NULL DEFAULT '[]'::jsonb,

  version                     TEXT NOT NULL DEFAULT 'v2_relaxed_vcp',
  input_source                TEXT NOT NULL DEFAULT 'final30',
  param_snapshot              JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  rs_score                    DOUBLE PRECISION NULL,
  pivot_price                 DOUBLE PRECISION NULL,
  close_price                 DOUBLE PRECISION NULL,
  recent_range_pct            DOUBLE PRECISION NULL,
  vcp_volume_metric_prev      DOUBLE PRECISION NULL,
  vcp_volume_metric_recent    DOUBLE PRECISION NULL,

  PRIMARY KEY (run_date, env, symbol)
);

CREATE INDEX IF NOT EXISTS idx_minervini_results_vcp_relaxed_lookup
  ON minervini_results_vcp_relaxed (run_date, env, final_pass);
