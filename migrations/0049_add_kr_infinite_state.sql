CREATE TABLE IF NOT EXISTS kr_infinite_state (
 strategy_id TEXT NOT NULL, symbol TEXT NOT NULL, cycle_id TEXT, cycle_start_date DATE, cycle_complete_date DATE,
 policy_version TEXT NOT NULL DEFAULT 'KR_INFINITE_ADAPTIVE_V1', allocated_capital_krw NUMERIC(20,4) NOT NULL DEFAULT 0,
 unit_krw NUMERIC(20,4) NOT NULL DEFAULT 0, core_filled_notional NUMERIC(20,4) NOT NULL DEFAULT 0,
 reserve_filled_notional NUMERIC(20,4) NOT NULL DEFAULT 0, units_used INTEGER NOT NULL DEFAULT 0,
 core_units_used INTEGER NOT NULL DEFAULT 0, reserve_units_used INTEGER NOT NULL DEFAULT 0, last_buy_date DATE,
 last_buy_price NUMERIC(20,8), last_exit_date DATE, reserve_unlocked BOOLEAN NOT NULL DEFAULT FALSE,
 crash_seen BOOLEAN NOT NULL DEFAULT FALSE, recovery_probe_done BOOLEAN NOT NULL DEFAULT FALSE,
 cycle_age_trading_days INTEGER NOT NULL DEFAULT 0, capital_preservation BOOLEAN NOT NULL DEFAULT FALSE,
 status TEXT NOT NULL DEFAULT 'READY', metadata JSONB NOT NULL DEFAULT '{}'::jsonb, version INTEGER NOT NULL DEFAULT 1,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY(strategy_id,symbol),
 CONSTRAINT ck_kr_inf_capital CHECK(allocated_capital_krw>=0 AND unit_krw>=0 AND core_filled_notional>=0 AND reserve_filled_notional>=0),
 CONSTRAINT ck_kr_inf_units CHECK(units_used BETWEEN 0 AND 40 AND core_units_used BETWEEN 0 AND 30 AND reserve_units_used BETWEEN 0 AND 10 AND core_units_used+reserve_units_used=units_used)
);
CREATE INDEX IF NOT EXISTS ix_kr_infinite_state_status ON kr_infinite_state(status,updated_at);
CREATE TABLE IF NOT EXISTS kr_infinite_order_intents (
 id BIGSERIAL PRIMARY KEY, strategy_id TEXT NOT NULL, symbol TEXT NOT NULL, cycle_id TEXT NOT NULL, trade_date DATE NOT NULL,
 side TEXT NOT NULL, reason TEXT NOT NULL, unit_sequence INTEGER, requested_notional_krw NUMERIC(20,4), requested_qty INTEGER,
 limit_price NUMERIC(20,8), idempotency_key TEXT NOT NULL UNIQUE, broker_order_id TEXT, status TEXT NOT NULL DEFAULT 'INTENT_CREATED',
 filled_qty INTEGER NOT NULL DEFAULT 0, filled_notional_krw NUMERIC(20,4) NOT NULL DEFAULT 0, filled_avg_price NUMERIC(20,8),
 market_state TEXT, metadata JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 CONSTRAINT ck_kr_inf_intent_qty CHECK(requested_qty IS NULL OR requested_qty>=0), CONSTRAINT ck_kr_inf_fill CHECK(filled_qty>=0 AND filled_notional_krw>=0)
);
CREATE INDEX IF NOT EXISTS ix_kr_infinite_intent_cycle_date ON kr_infinite_order_intents(strategy_id,symbol,cycle_id,trade_date);
CREATE INDEX IF NOT EXISTS ix_kr_infinite_intent_status ON kr_infinite_order_intents(status,updated_at);
