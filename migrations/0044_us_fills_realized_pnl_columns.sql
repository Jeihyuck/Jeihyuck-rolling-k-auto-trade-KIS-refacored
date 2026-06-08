-- Migration 0044: Add realized PNL audit columns to us_fills
-- These columns store FIFO-matched cost basis and realized PNL at time of SELL fill,
-- enabling fast audit/replay without recomputing from scratch.
-- All columns are nullable — existing rows and BUY fills will have NULL values.

ALTER TABLE us_fills
    ADD COLUMN IF NOT EXISTS avg_cost_at_sell   NUMERIC,
    ADD COLUMN IF NOT EXISTS realized_pnl_usd   NUMERIC,
    ADD COLUMN IF NOT EXISTS realized_pnl_pct   NUMERIC,
    ADD COLUMN IF NOT EXISTS matched_cost_method TEXT;

COMMENT ON COLUMN us_fills.avg_cost_at_sell   IS 'FIFO average cost basis per share at time of SELL (USD)';
COMMENT ON COLUMN us_fills.realized_pnl_usd   IS 'Realized PNL for this SELL fill in USD (filled_price - avg_cost) * qty';
COMMENT ON COLUMN us_fills.realized_pnl_pct   IS 'Realized PNL percentage: realized_pnl_usd / (avg_cost_at_sell * qty) * 100';
COMMENT ON COLUMN us_fills.matched_cost_method IS 'Cost basis method used: FIFO | AVERAGE | UNAVAILABLE';
