-- 0062: US order recovery uses the KIS account + Trading Epoch +
-- owner-specific Portfolio Epoch + immutable Position Cycle.
-- SCHEMA-ONLY. Historical rows remain NULL/UNKNOWN, never inferred or
-- rebound to today's account, portfolio or position. Install before US WSL
-- enforcement, and separately authorize each owner portfolio epoch.
BEGIN;

ALTER TABLE public.us_orders ADD COLUMN IF NOT EXISTS account_id text;
ALTER TABLE public.us_orders ADD COLUMN IF NOT EXISTS portfolio_epoch_id text;
ALTER TABLE public.us_orders ADD COLUMN IF NOT EXISTS position_cycle_id text;

ALTER TABLE public.us_order_intents ADD COLUMN IF NOT EXISTS account_id text;
ALTER TABLE public.us_order_intents ADD COLUMN IF NOT EXISTS portfolio_epoch_id text;
ALTER TABLE public.us_order_intents ADD COLUMN IF NOT EXISTS position_cycle_id text;

ALTER TABLE public.us_order_events ADD COLUMN IF NOT EXISTS account_id text;
ALTER TABLE public.us_order_events ADD COLUMN IF NOT EXISTS portfolio_epoch_id text;
ALTER TABLE public.us_order_events ADD COLUMN IF NOT EXISTS position_cycle_id text;

ALTER TABLE public.us_fills ADD COLUMN IF NOT EXISTS account_id text;
ALTER TABLE public.us_fills ADD COLUMN IF NOT EXISTS portfolio_epoch_id text;
ALTER TABLE public.us_fills ADD COLUMN IF NOT EXISTS position_cycle_id text;

ALTER TABLE public.us_positions ADD COLUMN IF NOT EXISTS account_id text;
ALTER TABLE public.us_positions ADD COLUMN IF NOT EXISTS portfolio_epoch_id text;
ALTER TABLE public.us_positions ADD COLUMN IF NOT EXISTS position_cycle_id text;

ALTER TABLE IF EXISTS public.us_position_risk_state ADD COLUMN IF NOT EXISTS account_id text;
ALTER TABLE IF EXISTS public.us_position_risk_state ADD COLUMN IF NOT EXISTS portfolio_epoch_id text;
ALTER TABLE IF EXISTS public.us_position_risk_state ADD COLUMN IF NOT EXISTS position_cycle_id text;

ALTER TABLE public.us_execution_claims ADD COLUMN IF NOT EXISTS portfolio_epoch_id text;
ALTER TABLE public.us_execution_claims ADD COLUMN IF NOT EXISTS position_cycle_id text;

ALTER TABLE public.us_protective_stop_obligations ADD COLUMN IF NOT EXISTS portfolio_epoch_id text;
ALTER TABLE public.us_protective_stop_obligations ADD COLUMN IF NOT EXISTS position_cycle_id text;

-- A new, fully scoped order must never omit a member of its provenance.
-- Legacy rows with missing scope remain readable and fenced, not fabricated.
DO $us_scope$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='ck_us_order_scope_complete_0062') THEN
   ALTER TABLE public.us_orders
     ADD CONSTRAINT ck_us_order_scope_complete_0062 CHECK (
       portfolio_epoch_id IS NULL OR
       (NULLIF(account_id,'') IS NOT NULL
        AND NULLIF(trading_epoch_id,'') IS NOT NULL
        AND NULLIF(position_cycle_id,'') IS NOT NULL)
     ) NOT VALID;
 END IF;
END $us_scope$;

-- A KIS order number is NOT globally unique over account generations.
-- Preflight uniqueness *before* dropping the old account-blind index.
DO $us_scoped_index$ BEGIN
  IF EXISTS (
    SELECT 1 FROM public.us_orders
    WHERE order_no IS NOT NULL AND btrim(order_no)<>''
    GROUP BY trade_date, order_no,
       COALESCE(NULLIF(env,''),'__legacy_env__'),
       COALESCE(NULLIF(account_id,''),'__legacy_account__'),
       COALESCE(NULLIF(trading_epoch_id,''),'__legacy_epoch__')
    HAVING count(*)>1
  ) THEN
    RAISE EXCEPTION 'US_ORDER_BROKER_SCOPED_DUPLICATES_REQUIRE_MANUAL_EVIDENCE';
  END IF;
END $us_scoped_index$;
CREATE UNIQUE INDEX IF NOT EXISTS uq_us_orders_broker_account_epoch_day_0062
  ON public.us_orders (
    trade_date, order_no,
    COALESCE(NULLIF(env,''),'__legacy_env__'),
    COALESCE(NULLIF(account_id,''),'__legacy_account__'),
    COALESCE(NULLIF(trading_epoch_id,''),'__legacy_epoch__')
  ) WHERE order_no IS NOT NULL AND btrim(order_no)<>'';
DROP INDEX IF EXISTS public.uq_us_orders_trade_date_order_no_nonblank;

-- Fail before touching the legacy fill uniqueness constraint. This is
-- deliberately conservative: ambiguous historical fills require manual
-- broker evidence, not automatic account/epoch backfill.
DO $us_fill_preflight$ BEGIN
  IF EXISTS (
    SELECT 1 FROM public.us_fills
    GROUP BY COALESCE(NULLIF(account_id,''),'__legacy_account__'),
             COALESCE(NULLIF(trading_epoch_id,''),'__legacy_epoch__'),
             trade_date, symbol, side, COALESCE(order_no,''),
             COALESCE(client_order_key,''), qty, price_usd
    HAVING count(*) > 1
  ) THEN
    RAISE EXCEPTION 'US_FILL_BROKER_SCOPED_DUPLICATES_REQUIRE_MANUAL_EVIDENCE';
  END IF;
END $us_fill_preflight$;

-- Fill identity must be account/epoch scoped too. A separate globally
-- unique fill_idempotency_key remains; writers must include source scope.
CREATE UNIQUE INDEX IF NOT EXISTS uq_us_fills_broker_account_epoch_0062
  ON public.us_fills (
    COALESCE(NULLIF(account_id,''),'__legacy_account__'),
    COALESCE(NULLIF(trading_epoch_id,''),'__legacy_epoch__'),
    trade_date, symbol, side,
    COALESCE(order_no,''), COALESCE(client_order_key,''), qty, price_usd
  );
DROP INDEX IF EXISTS public.uq_us_fills_idempotent;

CREATE INDEX IF NOT EXISTS ix_us_orders_recovery_scope_0062
  ON public.us_orders(env, account_id, trading_epoch_id,
                      portfolio_epoch_id, position_cycle_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_us_claims_recovery_scope_0062
  ON public.us_execution_claims(env,account_id,trading_epoch_id,
                               portfolio_epoch_id,position_cycle_id,action_key);
CREATE INDEX IF NOT EXISTS ix_us_stop_debt_recovery_scope_0062
  ON public.us_protective_stop_obligations(
    env,account_id,trading_epoch_id,portfolio_epoch_id,position_cycle_id
  );

-- portfolio_epochs is ALREADY the canonical registry. US owners use
-- sid=0/mode=0 and their exact strategy_owner string. No automatic INSERT,
-- and no UPDATE of the existing KR pb1_pullback_close epoch.
CREATE UNIQUE INDEX IF NOT EXISTS uq_us_portfolio_epoch_owner_0062
  ON public.portfolio_epochs(env,account_id,trading_epoch_id,strategy)
  WHERE status='ACTIVE' AND sid=0 AND mode=0
    AND strategy IN ('US_STANDARD','TQQQ_INFINITE');

COMMIT;
