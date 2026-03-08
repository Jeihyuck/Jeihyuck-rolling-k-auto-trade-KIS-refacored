-- Add explicit score columns to derived_minervini for watchlist merge/load consistency.

ALTER TABLE IF EXISTS derived_minervini
    ADD COLUMN IF NOT EXISTS rs_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS trend_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS breakout_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS pullback_score DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS momentum_score DOUBLE PRECISION;
