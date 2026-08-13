CREATE TABLE IF NOT EXISTS kr_infinite_state (
  strategy_id text NOT NULL,
  symbol text NOT NULL,
  book text NOT NULL,
  cycle_id text,
  cycle_status text NOT NULL DEFAULT 'READY',
  policy_version text NOT NULL,
  filled_quantity integer NOT NULL DEFAULT 0 CHECK (filled_quantity >= 0),
  authoritative_buy_notional numeric(18,2) NOT NULL DEFAULT 0,
  authoritative_sell_notional numeric(18,2) NOT NULL DEFAULT 0,
  authoritative_average_price numeric(18,4) NOT NULL DEFAULT 0,
  used_unit_fraction numeric(18,6) NOT NULL DEFAULT 0,
  last_buy_trade_date date,
  pending_order_key text,
  current_regime_state text,
  current_regime_score numeric(8,3),
  data_quality text NOT NULL DEFAULT 'UNKNOWN',
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(strategy_id, symbol)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_kr_infinite_pending_order_key
  ON kr_infinite_state(pending_order_key) WHERE pending_order_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_kr_infinite_cycle ON kr_infinite_state(book, cycle_id);
