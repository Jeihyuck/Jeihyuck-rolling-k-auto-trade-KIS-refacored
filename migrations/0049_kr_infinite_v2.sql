-- Additive state/history for the isolated 122630 sleeve. Canonical orders/fills remain authoritative.
CREATE TABLE IF NOT EXISTS kr_infinite_state (
 strategy_id TEXT NOT NULL, symbol TEXT NOT NULL CHECK (symbol='122630'), book TEXT NOT NULL,
 cycle_id TEXT NOT NULL, cycle_status TEXT NOT NULL CHECK (cycle_status IN
 ('READY','ENTRY_PENDING','ACTIVE','EXIT_PENDING','RECONCILE_PENDING','COMPLETE','OWNERSHIP_CONFLICT')),
 policy_version TEXT NOT NULL, filled_quantity INTEGER NOT NULL DEFAULT 0 CHECK(filled_quantity>=0),
 authoritative_buy_notional NUMERIC(20,4) NOT NULL DEFAULT 0,
 authoritative_sell_notional NUMERIC(20,4) NOT NULL DEFAULT 0,
 authoritative_average_price NUMERIC(20,8) NOT NULL DEFAULT 0,
 used_unit_fraction NUMERIC(10,4) NOT NULL DEFAULT 0, last_buy_trade_date DATE,
 pending_order_key TEXT, current_regime_state TEXT, current_regime_score NUMERIC(8,4),
 data_quality TEXT NOT NULL DEFAULT 'BLOCKED', metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 PRIMARY KEY(strategy_id,symbol), CHECK(authoritative_buy_notional>=0 AND authoritative_buy_notional<=15000000),
 CHECK(used_unit_fraction>=0 AND used_unit_fraction<=40));
CREATE TABLE IF NOT EXISTS kr_infinite_cycle_history (
 cycle_id TEXT PRIMARY KEY, strategy_id TEXT NOT NULL, symbol TEXT NOT NULL CHECK(symbol='122630'),
 policy_version TEXT NOT NULL, completed_trade_date DATE NOT NULL, filled_quantity INTEGER NOT NULL DEFAULT 0,
 authoritative_buy_notional NUMERIC(20,4) NOT NULL, authoritative_sell_notional NUMERIC(20,4) NOT NULL,
 metadata JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
