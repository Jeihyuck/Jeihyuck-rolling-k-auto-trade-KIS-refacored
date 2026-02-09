-- Derived Minervini features snapshot (as_of daily)

CREATE TABLE IF NOT EXISTS derived_minervini (
    symbol VARCHAR(16) NOT NULL,
    as_of DATE NOT NULL,
    close DOUBLE PRECISION,
    ma50 DOUBLE PRECISION,
    ma150 DOUBLE PRECISION,
    ma200 DOUBLE PRECISION,
    ma200_slope DOUBLE PRECISION,
    dollar_vol_50 DOUBLE PRECISION,
    atr DOUBLE PRECISION,
    atr_pct DOUBLE PRECISION,
    rs_percentile DOUBLE PRECISION,
    vcp_score DOUBLE PRECISION,
    vcp_ok BOOLEAN,
    pivot DOUBLE PRECISION,
    minervini_score DOUBLE PRECISION,
    minervini_pass BOOLEAN,
    features_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, as_of)
);

CREATE INDEX IF NOT EXISTS idx_derived_minervini_as_of ON derived_minervini(as_of);
CREATE INDEX IF NOT EXISTS idx_derived_minervini_symbol ON derived_minervini(symbol);
