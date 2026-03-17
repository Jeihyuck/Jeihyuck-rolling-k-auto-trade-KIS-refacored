ALTER TABLE orders ADD COLUMN IF NOT EXISTS entry_reason TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS entry_style_selected TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS entry_decision_family TEXT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS entry_meta_json JSONB DEFAULT '{}'::jsonb;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS stop_price_at_entry DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS pivot_price_at_entry DOUBLE PRECISION;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS entry_rule_version TEXT;

ALTER TABLE fills ADD COLUMN IF NOT EXISTS entry_reason TEXT;
ALTER TABLE fills ADD COLUMN IF NOT EXISTS entry_style_selected TEXT;
ALTER TABLE fills ADD COLUMN IF NOT EXISTS entry_decision_family TEXT;
ALTER TABLE fills ADD COLUMN IF NOT EXISTS fill_meta_json JSONB DEFAULT '{}'::jsonb;
ALTER TABLE fills ADD COLUMN IF NOT EXISTS stop_price_at_entry DOUBLE PRECISION;
ALTER TABLE fills ADD COLUMN IF NOT EXISTS pivot_price_at_entry DOUBLE PRECISION;
ALTER TABLE fills ADD COLUMN IF NOT EXISTS entry_rule_version TEXT;

ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_reason TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_style_selected TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_decision_family TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_rule_version TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_meta_json JSONB DEFAULT '{}'::jsonb;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS stop_price_at_entry DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS pivot_price_at_entry DOUBLE PRECISION;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS exit_policy_family TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS last_exit_eval_json JSONB DEFAULT '{}'::jsonb;