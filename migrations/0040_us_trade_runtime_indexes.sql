-- US trade runtime indexes for 2026-06-26 dual-agent stability.
CREATE INDEX IF NOT EXISTS idx_us_watchlist_trade_date_score
ON us_watchlist (trade_date, score DESC);

CREATE INDEX IF NOT EXISTS idx_us_watchlist_trade_date_symbol
ON us_watchlist (trade_date, symbol);

CREATE INDEX IF NOT EXISTS idx_us_orders_trade_date_status
ON us_orders (trade_date, status);

CREATE INDEX IF NOT EXISTS idx_us_fills_trade_date_symbol_side
ON us_fills (trade_date, symbol, side);
