CREATE TABLE IF NOT EXISTS kr_infinite_cycle_history (
  strategy_id text NOT NULL,
  symbol text NOT NULL,
  book text NOT NULL,
  cycle_id text NOT NULL,
  cycle_status text NOT NULL,
  policy_version text NOT NULL,
  filled_quantity integer NOT NULL DEFAULT 0,
  authoritative_buy_notional numeric(18,2) NOT NULL DEFAULT 0,
  authoritative_sell_notional numeric(18,2) NOT NULL DEFAULT 0,
  authoritative_average_price numeric(18,4) NOT NULL DEFAULT 0,
  used_unit_fraction numeric(18,6) NOT NULL DEFAULT 0,
  last_buy_trade_date date,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  completed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(strategy_id, cycle_id)
);
CREATE INDEX IF NOT EXISTS ix_kr_infinite_history_symbol_completed
  ON kr_infinite_cycle_history(symbol, completed_at);
