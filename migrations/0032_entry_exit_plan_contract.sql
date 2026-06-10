-- Entry/Exit plan contract persisted on positions.
ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_thesis TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS trade_horizon TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS eod_action TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS force_eod_close BOOLEAN DEFAULT FALSE;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS max_trading_days INTEGER;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS initial_stop_price NUMERIC;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS initial_risk_r NUMERIC;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_exit_plan_json JSONB DEFAULT '{}'::jsonb;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS last_exit_plan_eval_json JSONB DEFAULT '{}'::jsonb;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS policy_source TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS policy_version TEXT;

CREATE INDEX IF NOT EXISTS idx_positions_entry_exit_plan_open
ON positions (env, strategy, trade_horizon, exit_policy_family, eod_action, qty)
WHERE qty > 0;

CREATE INDEX IF NOT EXISTS idx_positions_force_eod_close
ON positions (env, strategy, force_eod_close, qty)
WHERE qty > 0;
