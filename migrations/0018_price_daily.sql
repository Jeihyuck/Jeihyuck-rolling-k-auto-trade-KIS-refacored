-- Migration: 0018_price_daily
-- Description: Add price_daily table for caching daily price candles from KIS API
-- Created: 2026-01-27

CREATE TABLE IF NOT EXISTS price_daily (
    market TEXT NOT NULL,
    code TEXT NOT NULL,
    date DATE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    value NUMERIC,
    source TEXT NOT NULL DEFAULT 'KIS',
    PRIMARY KEY (market, code, date)
);

-- Optional: Index for faster queries by code and date range
CREATE INDEX IF NOT EXISTS idx_price_daily_code_date ON price_daily (code, date);

-- Optional: Index for market
CREATE INDEX IF NOT EXISTS idx_price_daily_market ON price_daily (market);