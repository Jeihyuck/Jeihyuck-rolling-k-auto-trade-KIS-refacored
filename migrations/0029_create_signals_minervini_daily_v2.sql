-- signals_minervini_daily_v2: rule-based VCP + trend template + RS + regime snapshot
CREATE TABLE IF NOT EXISTS signals_minervini_daily_v2 (
  env               TEXT NOT NULL,
  as_of             DATE NOT NULL,
  symbol            TEXT NOT NULL,

  benchmark         TEXT NOT NULL DEFAULT '229200',
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  run_id            TEXT NULL,

  regime_pass       BOOLEAN NOT NULL DEFAULT FALSE,
  rs63              DOUBLE PRECISION NULL,
  rs126             DOUBLE PRECISION NULL,
  rs_score          DOUBLE PRECISION NULL,
  rs_pctile         DOUBLE PRECISION NULL,
  rs_pass           BOOLEAN NOT NULL DEFAULT FALSE,

  close             DOUBLE PRECISION NULL,
  ma50              DOUBLE PRECISION NULL,
  ma150             DOUBLE PRECISION NULL,
  ma200             DOUBLE PRECISION NULL,
  ma200_up          BOOLEAN NOT NULL DEFAULT FALSE,
  high_52w          DOUBLE PRECISION NULL,
  near_52w_high     BOOLEAN NOT NULL DEFAULT FALSE,
  trend_template_pass BOOLEAN NOT NULL DEFAULT FALSE,

  pivot_price       DOUBLE PRECISION NULL,
  contraction_count INTEGER NOT NULL DEFAULT 0,
  c1_pct            DOUBLE PRECISION NULL,
  c2_pct            DOUBLE PRECISION NULL,
  c3_pct            DOUBLE PRECISION NULL,
  tight_pct         DOUBLE PRECISION NULL,
  atr14             DOUBLE PRECISION NULL,
  atr_pct           DOUBLE PRECISION NULL,
  vol_shrink_ratio  DOUBLE PRECISION NULL,

  vcp_pass          BOOLEAN NOT NULL DEFAULT FALSE,
  vcp_reasons       JSONB NOT NULL DEFAULT '[]'::jsonb,

  pass              BOOLEAN NOT NULL DEFAULT FALSE,
  reject_reasons    JSONB NOT NULL DEFAULT '[]'::jsonb,

  PRIMARY KEY (env, as_of, symbol)
);

CREATE INDEX IF NOT EXISTS idx_signals_minervini_daily_v2_asof
  ON signals_minervini_daily_v2 (env, as_of);

CREATE INDEX IF NOT EXISTS idx_signals_minervini_daily_v2_pass
  ON signals_minervini_daily_v2 (env, as_of, pass);
