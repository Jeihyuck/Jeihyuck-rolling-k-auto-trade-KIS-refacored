-- KR immutable BUY-to-SELL contract guard. Schema-only; never rewrites a trade.
-- 0063: applies to signed PB1 BUYs and linked signed PB1 positions, including
-- historic closed cycles. Imported/unproven holdings remain reviewable.
-- This migration is transactional under trader.db.migrate (no COMMIT here).
-- Superusers and migration owners can disable/drop triggers: not a WORM archive.

CREATE OR REPLACE FUNCTION public.kr_signed_buy_order_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_buy_0063$
BEGIN
    -- New signed BUY must carry a complete immutable envelope at INSERT time.
    IF TG_OP = 'INSERT' THEN
        IF NEW.side = 'BUY' AND NEW.strategy = 'pb1_pullback_close'
           AND NEW.request_json::jsonb ->> 'enforce_entry_contract' = 'true' THEN
            IF COALESCE(NEW.request_json::jsonb ->> 'entry_contract_sha256', '') !~ '^[0-9a-f]{64}
       OR COALESCE(OLD.request_json::jsonb ->> 'entry_contract_sha256', '') !~ '^[0-9a-f]{64}
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE INSERT OR UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    IF TG_OP = 'INSERT' THEN
        new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
        IF NEW.strategy = 'pb1_pullback_close'
           AND (new_meta ->> 'entry_contract_sha256') IS NOT NULL THEN
            IF COALESCE(new_meta ->> 'entry_contract_sha256', '') !~ '^[0-9a-f]{64}
    IF OLD.strategy <> 'pb1_pullback_close'
       OR COALESCE(old_meta ->> 'entry_contract_sha256', '') !~ '^[0-9a-f]{64}
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE INSERT OR UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();


               OR NEW.request_json::jsonb -> 'entry_exit_plan' IS NULL
               OR NEW.request_json::jsonb -> 'entry_exit_plan' = '{}'::jsonb
               OR NULLIF(NEW.request_json::jsonb ->> 'entry_contract_version', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'client_order_key', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'position_cycle_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'portfolio_epoch_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'trading_epoch_id', '') IS NULL
               OR (NEW.request_json::jsonb ->> 'position_cycle_id') IS DISTINCT FROM NEW.position_cycle_id
               OR (NEW.request_json::jsonb ->> 'portfolio_epoch_id') IS DISTINCT FROM NEW.portfolio_epoch_id
               OR (NEW.request_json::jsonb ->> 'trading_epoch_id') IS DISTINCT FROM NEW.trading_epoch_id
               OR (NEW.request_json::jsonb ->> 'client_order_key') IS DISTINCT FROM NEW.client_order_key
               OR (NEW.request_json::jsonb ->> 'requested_qty')::integer IS DISTINCT FROM NEW.qty
               OR COALESCE((NEW.request_json::jsonb ->> 'submitted_qty')::integer, 0) <= 0
            THEN
                RAISE EXCEPTION 'KR_SIGNED_BUY_INTENT_CONTRACT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    -- Never silently promote a legacy unsigned row to a signed contract.
    IF TG_OP = 'UPDATE' AND OLD.side = 'BUY' AND OLD.strategy = 'pb1_pullback_close'
       AND COALESCE(OLD.request_json::jsonb ->> 'enforce_entry_contract', '') <> 'true'
       AND (NEW.request_json::jsonb ->> 'enforce_entry_contract' = 'true'
            OR NEW.request_json::jsonb ->> 'entry_contract_sha256' IS NOT NULL)
    THEN
        RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_BUY_REBIND_REQUIRES_VERIFIED_REPAIR';
    END IF;
    IF OLD.side <> 'BUY' OR OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(OLD.request_json::jsonb ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...
 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...

               OR NEW.request_json::jsonb -> 'entry_exit_plan' IS NULL
               OR NEW.request_json::jsonb -> 'entry_exit_plan' = '{}'::jsonb
               OR NULLIF(NEW.request_json::jsonb ->> 'entry_contract_version', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'client_order_key', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'position_cycle_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'portfolio_epoch_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'trading_epoch_id', '') IS NULL
               OR (NEW.request_json::jsonb ->> 'position_cycle_id') IS DISTINCT FROM NEW.position_cycle_id
               OR (NEW.request_json::jsonb ->> 'portfolio_epoch_id') IS DISTINCT FROM NEW.portfolio_epoch_id
               OR (NEW.request_json::jsonb ->> 'trading_epoch_id') IS DISTINCT FROM NEW.trading_epoch_id
               OR (NEW.request_json::jsonb ->> 'client_order_key') IS DISTINCT FROM NEW.client_order_key
               OR (NEW.request_json::jsonb ->> 'requested_qty')::integer IS DISTINCT FROM NEW.qty
               OR COALESCE((NEW.request_json::jsonb ->> 'submitted_qty')::integer, 0) <= 0
            THEN
                RAISE EXCEPTION 'KR_SIGNED_BUY_INTENT_CONTRACT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    -- Never silently promote a legacy unsigned row to a signed contract.
    IF TG_OP = 'UPDATE' AND OLD.side = 'BUY' AND OLD.strategy = 'pb1_pullback_close'
       AND COALESCE(OLD.request_json::jsonb ->> 'enforce_entry_contract', '') <> 'true'
       AND (NEW.request_json::jsonb ->> 'enforce_entry_contract' = 'true'
            OR NEW.request_json::jsonb ->> 'entry_contract_sha256' IS NOT NULL)
    THEN
        RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_BUY_REBIND_REQUIRES_VERIFIED_REPAIR';
    END IF;
    IF OLD.side <> 'BUY' OR OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(OLD.request_json::jsonb ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...

               OR COALESCE(new_meta ->> 'entry_exit_plan_sha256', '') !~ '^[0-9a-f]{64}
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...

               OR NEW.request_json::jsonb -> 'entry_exit_plan' IS NULL
               OR NEW.request_json::jsonb -> 'entry_exit_plan' = '{}'::jsonb
               OR NULLIF(NEW.request_json::jsonb ->> 'entry_contract_version', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'client_order_key', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'position_cycle_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'portfolio_epoch_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'trading_epoch_id', '') IS NULL
               OR (NEW.request_json::jsonb ->> 'position_cycle_id') IS DISTINCT FROM NEW.position_cycle_id
               OR (NEW.request_json::jsonb ->> 'portfolio_epoch_id') IS DISTINCT FROM NEW.portfolio_epoch_id
               OR (NEW.request_json::jsonb ->> 'trading_epoch_id') IS DISTINCT FROM NEW.trading_epoch_id
               OR (NEW.request_json::jsonb ->> 'client_order_key') IS DISTINCT FROM NEW.client_order_key
               OR (NEW.request_json::jsonb ->> 'requested_qty')::integer IS DISTINCT FROM NEW.qty
               OR COALESCE((NEW.request_json::jsonb ->> 'submitted_qty')::integer, 0) <= 0
            THEN
                RAISE EXCEPTION 'KR_SIGNED_BUY_INTENT_CONTRACT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    -- Never silently promote a legacy unsigned row to a signed contract.
    IF TG_OP = 'UPDATE' AND OLD.side = 'BUY' AND OLD.strategy = 'pb1_pullback_close'
       AND COALESCE(OLD.request_json::jsonb ->> 'enforce_entry_contract', '') <> 'true'
       AND (NEW.request_json::jsonb ->> 'enforce_entry_contract' = 'true'
            OR NEW.request_json::jsonb ->> 'entry_contract_sha256' IS NOT NULL)
    THEN
        RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_BUY_REBIND_REQUIRES_VERIFIED_REPAIR';
    END IF;
    IF OLD.side <> 'BUY' OR OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(OLD.request_json::jsonb ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...
 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...

               OR NEW.request_json::jsonb -> 'entry_exit_plan' IS NULL
               OR NEW.request_json::jsonb -> 'entry_exit_plan' = '{}'::jsonb
               OR NULLIF(NEW.request_json::jsonb ->> 'entry_contract_version', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'client_order_key', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'position_cycle_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'portfolio_epoch_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'trading_epoch_id', '') IS NULL
               OR (NEW.request_json::jsonb ->> 'position_cycle_id') IS DISTINCT FROM NEW.position_cycle_id
               OR (NEW.request_json::jsonb ->> 'portfolio_epoch_id') IS DISTINCT FROM NEW.portfolio_epoch_id
               OR (NEW.request_json::jsonb ->> 'trading_epoch_id') IS DISTINCT FROM NEW.trading_epoch_id
               OR (NEW.request_json::jsonb ->> 'client_order_key') IS DISTINCT FROM NEW.client_order_key
               OR (NEW.request_json::jsonb ->> 'requested_qty')::integer IS DISTINCT FROM NEW.qty
               OR COALESCE((NEW.request_json::jsonb ->> 'submitted_qty')::integer, 0) <= 0
            THEN
                RAISE EXCEPTION 'KR_SIGNED_BUY_INTENT_CONTRACT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    -- Never silently promote a legacy unsigned row to a signed contract.
    IF TG_OP = 'UPDATE' AND OLD.side = 'BUY' AND OLD.strategy = 'pb1_pullback_close'
       AND COALESCE(OLD.request_json::jsonb ->> 'enforce_entry_contract', '') <> 'true'
       AND (NEW.request_json::jsonb ->> 'enforce_entry_contract' = 'true'
            OR NEW.request_json::jsonb ->> 'entry_contract_sha256' IS NOT NULL)
    THEN
        RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_BUY_REBIND_REQUIRES_VERIFIED_REPAIR';
    END IF;
    IF OLD.side <> 'BUY' OR OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(OLD.request_json::jsonb ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...

               OR NEW.entry_exit_plan_json::jsonb IS NULL
               OR NEW.entry_exit_plan_json::jsonb = '{}'::jsonb
               OR NULLIF(new_meta ->> 'source_buy_order_id','') IS NULL
               OR NULLIF(NEW.position_cycle_id,'') IS NULL
               OR NULLIF(NEW.portfolio_epoch_id,'') IS NULL
               OR NULLIF(NEW.trading_epoch_id,'') IS NULL
            THEN
                RAISE EXCEPTION 'KR_SIGNED_POSITION_INSERT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF TG_OP = 'UPDATE' AND OLD.strategy = 'pb1_pullback_close'
       AND (old_meta ->> 'entry_contract_sha256') IS NULL
       AND (COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb) ->> 'entry_contract_sha256') IS NOT NULL THEN
        RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_POSITION_REBIND_REQUIRES_VERIFIED_REPAIR';
    END IF;
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...

               OR NEW.request_json::jsonb -> 'entry_exit_plan' IS NULL
               OR NEW.request_json::jsonb -> 'entry_exit_plan' = '{}'::jsonb
               OR NULLIF(NEW.request_json::jsonb ->> 'entry_contract_version', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'client_order_key', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'position_cycle_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'portfolio_epoch_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'trading_epoch_id', '') IS NULL
               OR (NEW.request_json::jsonb ->> 'position_cycle_id') IS DISTINCT FROM NEW.position_cycle_id
               OR (NEW.request_json::jsonb ->> 'portfolio_epoch_id') IS DISTINCT FROM NEW.portfolio_epoch_id
               OR (NEW.request_json::jsonb ->> 'trading_epoch_id') IS DISTINCT FROM NEW.trading_epoch_id
               OR (NEW.request_json::jsonb ->> 'client_order_key') IS DISTINCT FROM NEW.client_order_key
               OR (NEW.request_json::jsonb ->> 'requested_qty')::integer IS DISTINCT FROM NEW.qty
               OR COALESCE((NEW.request_json::jsonb ->> 'submitted_qty')::integer, 0) <= 0
            THEN
                RAISE EXCEPTION 'KR_SIGNED_BUY_INTENT_CONTRACT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    -- Never silently promote a legacy unsigned row to a signed contract.
    IF TG_OP = 'UPDATE' AND OLD.side = 'BUY' AND OLD.strategy = 'pb1_pullback_close'
       AND COALESCE(OLD.request_json::jsonb ->> 'enforce_entry_contract', '') <> 'true'
       AND (NEW.request_json::jsonb ->> 'enforce_entry_contract' = 'true'
            OR NEW.request_json::jsonb ->> 'entry_contract_sha256' IS NOT NULL)
    THEN
        RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_BUY_REBIND_REQUIRES_VERIFIED_REPAIR';
    END IF;
    IF OLD.side <> 'BUY' OR OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(OLD.request_json::jsonb ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...
 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...

               OR NEW.request_json::jsonb -> 'entry_exit_plan' IS NULL
               OR NEW.request_json::jsonb -> 'entry_exit_plan' = '{}'::jsonb
               OR NULLIF(NEW.request_json::jsonb ->> 'entry_contract_version', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'client_order_key', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'position_cycle_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'portfolio_epoch_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'trading_epoch_id', '') IS NULL
               OR (NEW.request_json::jsonb ->> 'position_cycle_id') IS DISTINCT FROM NEW.position_cycle_id
               OR (NEW.request_json::jsonb ->> 'portfolio_epoch_id') IS DISTINCT FROM NEW.portfolio_epoch_id
               OR (NEW.request_json::jsonb ->> 'trading_epoch_id') IS DISTINCT FROM NEW.trading_epoch_id
               OR (NEW.request_json::jsonb ->> 'client_order_key') IS DISTINCT FROM NEW.client_order_key
               OR (NEW.request_json::jsonb ->> 'requested_qty')::integer IS DISTINCT FROM NEW.qty
               OR COALESCE((NEW.request_json::jsonb ->> 'submitted_qty')::integer, 0) <= 0
            THEN
                RAISE EXCEPTION 'KR_SIGNED_BUY_INTENT_CONTRACT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    -- Never silently promote a legacy unsigned row to a signed contract.
    IF TG_OP = 'UPDATE' AND OLD.side = 'BUY' AND OLD.strategy = 'pb1_pullback_close'
       AND COALESCE(OLD.request_json::jsonb ->> 'enforce_entry_contract', '') <> 'true'
       AND (NEW.request_json::jsonb ->> 'enforce_entry_contract' = 'true'
            OR NEW.request_json::jsonb ->> 'entry_contract_sha256' IS NOT NULL)
    THEN
        RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_BUY_REBIND_REQUIRES_VERIFIED_REPAIR';
    END IF;
    IF OLD.side <> 'BUY' OR OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(OLD.request_json::jsonb ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...
 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...

               OR NEW.request_json::jsonb -> 'entry_exit_plan' IS NULL
               OR NEW.request_json::jsonb -> 'entry_exit_plan' = '{}'::jsonb
               OR NULLIF(NEW.request_json::jsonb ->> 'entry_contract_version', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'client_order_key', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'position_cycle_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'portfolio_epoch_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'trading_epoch_id', '') IS NULL
               OR (NEW.request_json::jsonb ->> 'position_cycle_id') IS DISTINCT FROM NEW.position_cycle_id
               OR (NEW.request_json::jsonb ->> 'portfolio_epoch_id') IS DISTINCT FROM NEW.portfolio_epoch_id
               OR (NEW.request_json::jsonb ->> 'trading_epoch_id') IS DISTINCT FROM NEW.trading_epoch_id
               OR (NEW.request_json::jsonb ->> 'client_order_key') IS DISTINCT FROM NEW.client_order_key
               OR (NEW.request_json::jsonb ->> 'requested_qty')::integer IS DISTINCT FROM NEW.qty
               OR COALESCE((NEW.request_json::jsonb ->> 'submitted_qty')::integer, 0) <= 0
            THEN
                RAISE EXCEPTION 'KR_SIGNED_BUY_INTENT_CONTRACT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    -- Never silently promote a legacy unsigned row to a signed contract.
    IF TG_OP = 'UPDATE' AND OLD.side = 'BUY' AND OLD.strategy = 'pb1_pullback_close'
       AND COALESCE(OLD.request_json::jsonb ->> 'enforce_entry_contract', '') <> 'true'
       AND (NEW.request_json::jsonb ->> 'enforce_entry_contract' = 'true'
            OR NEW.request_json::jsonb ->> 'entry_contract_sha256' IS NOT NULL)
    THEN
        RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_BUY_REBIND_REQUIRES_VERIFIED_REPAIR';
    END IF;
    IF OLD.side <> 'BUY' OR OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(OLD.request_json::jsonb ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...
 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...

               OR NEW.request_json::jsonb -> 'entry_exit_plan' IS NULL
               OR NEW.request_json::jsonb -> 'entry_exit_plan' = '{}'::jsonb
               OR NULLIF(NEW.request_json::jsonb ->> 'entry_contract_version', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'client_order_key', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'position_cycle_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'portfolio_epoch_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'trading_epoch_id', '') IS NULL
               OR (NEW.request_json::jsonb ->> 'position_cycle_id') IS DISTINCT FROM NEW.position_cycle_id
               OR (NEW.request_json::jsonb ->> 'portfolio_epoch_id') IS DISTINCT FROM NEW.portfolio_epoch_id
               OR (NEW.request_json::jsonb ->> 'trading_epoch_id') IS DISTINCT FROM NEW.trading_epoch_id
               OR (NEW.request_json::jsonb ->> 'client_order_key') IS DISTINCT FROM NEW.client_order_key
               OR (NEW.request_json::jsonb ->> 'requested_qty')::integer IS DISTINCT FROM NEW.qty
               OR COALESCE((NEW.request_json::jsonb ->> 'submitted_qty')::integer, 0) <= 0
            THEN
                RAISE EXCEPTION 'KR_SIGNED_BUY_INTENT_CONTRACT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    -- Never silently promote a legacy unsigned row to a signed contract.
    IF TG_OP = 'UPDATE' AND OLD.side = 'BUY' AND OLD.strategy = 'pb1_pullback_close'
       AND COALESCE(OLD.request_json::jsonb ->> 'enforce_entry_contract', '') <> 'true'
       AND (NEW.request_json::jsonb ->> 'enforce_entry_contract' = 'true'
            OR NEW.request_json::jsonb ->> 'entry_contract_sha256' IS NOT NULL)
    THEN
        RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_BUY_REBIND_REQUIRES_VERIFIED_REPAIR';
    END IF;
    IF OLD.side <> 'BUY' OR OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(OLD.request_json::jsonb ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...

               OR COALESCE(new_meta ->> 'entry_exit_plan_sha256', '') !~ '^[0-9a-f]{64}
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...

               OR NEW.request_json::jsonb -> 'entry_exit_plan' IS NULL
               OR NEW.request_json::jsonb -> 'entry_exit_plan' = '{}'::jsonb
               OR NULLIF(NEW.request_json::jsonb ->> 'entry_contract_version', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'client_order_key', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'position_cycle_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'portfolio_epoch_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'trading_epoch_id', '') IS NULL
               OR (NEW.request_json::jsonb ->> 'position_cycle_id') IS DISTINCT FROM NEW.position_cycle_id
               OR (NEW.request_json::jsonb ->> 'portfolio_epoch_id') IS DISTINCT FROM NEW.portfolio_epoch_id
               OR (NEW.request_json::jsonb ->> 'trading_epoch_id') IS DISTINCT FROM NEW.trading_epoch_id
               OR (NEW.request_json::jsonb ->> 'client_order_key') IS DISTINCT FROM NEW.client_order_key
               OR (NEW.request_json::jsonb ->> 'requested_qty')::integer IS DISTINCT FROM NEW.qty
               OR COALESCE((NEW.request_json::jsonb ->> 'submitted_qty')::integer, 0) <= 0
            THEN
                RAISE EXCEPTION 'KR_SIGNED_BUY_INTENT_CONTRACT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    -- Never silently promote a legacy unsigned row to a signed contract.
    IF TG_OP = 'UPDATE' AND OLD.side = 'BUY' AND OLD.strategy = 'pb1_pullback_close'
       AND COALESCE(OLD.request_json::jsonb ->> 'enforce_entry_contract', '') <> 'true'
       AND (NEW.request_json::jsonb ->> 'enforce_entry_contract' = 'true'
            OR NEW.request_json::jsonb ->> 'entry_contract_sha256' IS NOT NULL)
    THEN
        RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_BUY_REBIND_REQUIRES_VERIFIED_REPAIR';
    END IF;
    IF OLD.side <> 'BUY' OR OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(OLD.request_json::jsonb ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...
 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...

               OR NEW.request_json::jsonb -> 'entry_exit_plan' IS NULL
               OR NEW.request_json::jsonb -> 'entry_exit_plan' = '{}'::jsonb
               OR NULLIF(NEW.request_json::jsonb ->> 'entry_contract_version', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'client_order_key', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'position_cycle_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'portfolio_epoch_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'trading_epoch_id', '') IS NULL
               OR (NEW.request_json::jsonb ->> 'position_cycle_id') IS DISTINCT FROM NEW.position_cycle_id
               OR (NEW.request_json::jsonb ->> 'portfolio_epoch_id') IS DISTINCT FROM NEW.portfolio_epoch_id
               OR (NEW.request_json::jsonb ->> 'trading_epoch_id') IS DISTINCT FROM NEW.trading_epoch_id
               OR (NEW.request_json::jsonb ->> 'client_order_key') IS DISTINCT FROM NEW.client_order_key
               OR (NEW.request_json::jsonb ->> 'requested_qty')::integer IS DISTINCT FROM NEW.qty
               OR COALESCE((NEW.request_json::jsonb ->> 'submitted_qty')::integer, 0) <= 0
            THEN
                RAISE EXCEPTION 'KR_SIGNED_BUY_INTENT_CONTRACT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    -- Never silently promote a legacy unsigned row to a signed contract.
    IF TG_OP = 'UPDATE' AND OLD.side = 'BUY' AND OLD.strategy = 'pb1_pullback_close'
       AND COALESCE(OLD.request_json::jsonb ->> 'enforce_entry_contract', '') <> 'true'
       AND (NEW.request_json::jsonb ->> 'enforce_entry_contract' = 'true'
            OR NEW.request_json::jsonb ->> 'entry_contract_sha256' IS NOT NULL)
    THEN
        RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_BUY_REBIND_REQUIRES_VERIFIED_REPAIR';
    END IF;
    IF OLD.side <> 'BUY' OR OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(OLD.request_json::jsonb ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...

               OR NEW.entry_exit_plan_json::jsonb IS NULL
               OR NEW.entry_exit_plan_json::jsonb = '{}'::jsonb
               OR NULLIF(new_meta ->> 'source_buy_order_id','') IS NULL
               OR NULLIF(NEW.position_cycle_id,'') IS NULL
               OR NULLIF(NEW.portfolio_epoch_id,'') IS NULL
               OR NULLIF(NEW.trading_epoch_id,'') IS NULL
            THEN
                RAISE EXCEPTION 'KR_SIGNED_POSITION_INSERT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF TG_OP = 'UPDATE' AND OLD.strategy = 'pb1_pullback_close'
       AND (old_meta ->> 'entry_contract_sha256') IS NULL
       AND (COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb) ->> 'entry_contract_sha256') IS NOT NULL THEN
        RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_POSITION_REBIND_REQUIRES_VERIFIED_REPAIR';
    END IF;
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...

               OR NEW.request_json::jsonb -> 'entry_exit_plan' IS NULL
               OR NEW.request_json::jsonb -> 'entry_exit_plan' = '{}'::jsonb
               OR NULLIF(NEW.request_json::jsonb ->> 'entry_contract_version', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'client_order_key', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'position_cycle_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'portfolio_epoch_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'trading_epoch_id', '') IS NULL
               OR (NEW.request_json::jsonb ->> 'position_cycle_id') IS DISTINCT FROM NEW.position_cycle_id
               OR (NEW.request_json::jsonb ->> 'portfolio_epoch_id') IS DISTINCT FROM NEW.portfolio_epoch_id
               OR (NEW.request_json::jsonb ->> 'trading_epoch_id') IS DISTINCT FROM NEW.trading_epoch_id
               OR (NEW.request_json::jsonb ->> 'client_order_key') IS DISTINCT FROM NEW.client_order_key
               OR (NEW.request_json::jsonb ->> 'requested_qty')::integer IS DISTINCT FROM NEW.qty
               OR COALESCE((NEW.request_json::jsonb ->> 'submitted_qty')::integer, 0) <= 0
            THEN
                RAISE EXCEPTION 'KR_SIGNED_BUY_INTENT_CONTRACT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    -- Never silently promote a legacy unsigned row to a signed contract.
    IF TG_OP = 'UPDATE' AND OLD.side = 'BUY' AND OLD.strategy = 'pb1_pullback_close'
       AND COALESCE(OLD.request_json::jsonb ->> 'enforce_entry_contract', '') <> 'true'
       AND (NEW.request_json::jsonb ->> 'enforce_entry_contract' = 'true'
            OR NEW.request_json::jsonb ->> 'entry_contract_sha256' IS NOT NULL)
    THEN
        RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_BUY_REBIND_REQUIRES_VERIFIED_REPAIR';
    END IF;
    IF OLD.side <> 'BUY' OR OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(OLD.request_json::jsonb ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...
 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...

               OR NEW.request_json::jsonb -> 'entry_exit_plan' IS NULL
               OR NEW.request_json::jsonb -> 'entry_exit_plan' = '{}'::jsonb
               OR NULLIF(NEW.request_json::jsonb ->> 'entry_contract_version', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'client_order_key', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'position_cycle_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'portfolio_epoch_id', '') IS NULL
               OR NULLIF(NEW.request_json::jsonb ->> 'trading_epoch_id', '') IS NULL
               OR (NEW.request_json::jsonb ->> 'position_cycle_id') IS DISTINCT FROM NEW.position_cycle_id
               OR (NEW.request_json::jsonb ->> 'portfolio_epoch_id') IS DISTINCT FROM NEW.portfolio_epoch_id
               OR (NEW.request_json::jsonb ->> 'trading_epoch_id') IS DISTINCT FROM NEW.trading_epoch_id
               OR (NEW.request_json::jsonb ->> 'client_order_key') IS DISTINCT FROM NEW.client_order_key
               OR (NEW.request_json::jsonb ->> 'requested_qty')::integer IS DISTINCT FROM NEW.qty
               OR COALESCE((NEW.request_json::jsonb ->> 'submitted_qty')::integer, 0) <= 0
            THEN
                RAISE EXCEPTION 'KR_SIGNED_BUY_INTENT_CONTRACT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    -- Never silently promote a legacy unsigned row to a signed contract.
    IF TG_OP = 'UPDATE' AND OLD.side = 'BUY' AND OLD.strategy = 'pb1_pullback_close'
       AND COALESCE(OLD.request_json::jsonb ->> 'enforce_entry_contract', '') <> 'true'
       AND (NEW.request_json::jsonb ->> 'enforce_entry_contract' = 'true'
            OR NEW.request_json::jsonb ->> 'entry_contract_sha256' IS NOT NULL)
    THEN
        RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_BUY_REBIND_REQUIRES_VERIFIED_REPAIR';
    END IF;
    IF OLD.side <> 'BUY' OR OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(OLD.request_json::jsonb ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id)
       OR NEW.request_json::jsonb -> 'entry_exit_plan'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_exit_plan'
       OR NEW.request_json::jsonb -> 'entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'entry_contract_version'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'entry_contract_version'
       OR NEW.request_json::jsonb -> 'enforce_entry_contract'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'enforce_entry_contract'
       OR NEW.request_json::jsonb -> 'strategy_owner'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'strategy_owner'
       OR NEW.request_json::jsonb -> 'pre_order_holding_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'pre_order_holding_qty'
       OR NEW.request_json::jsonb -> 'requested_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'requested_qty'
       OR NEW.request_json::jsonb -> 'submitted_qty'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'submitted_qty'
       OR NEW.request_json::jsonb -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_entry_contract_sha256'
       OR NEW.request_json::jsonb -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_contract_binding_sha256'
       OR NEW.request_json::jsonb -> 'parent_position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_position_cycle_id'
       OR NEW.request_json::jsonb -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'parent_portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'position_cycle_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'position_cycle_id'
       OR NEW.request_json::jsonb -> 'portfolio_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'portfolio_epoch_id'
       OR NEW.request_json::jsonb -> 'trading_epoch_id'
          IS DISTINCT FROM OLD.request_json::jsonb -> 'trading_epoch_id'
    THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_CONTRACT_IMMUTABLE';
    END IF;
    RETURN NEW;
END;
$kr_buy_0063$;

DROP TRIGGER IF EXISTS kr_signed_buy_order_guard_0063 ON public.orders;
CREATE TRIGGER kr_signed_buy_order_guard_0063
BEFORE UPDATE OR DELETE ON public.orders FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_buy_order_guard_0063();

CREATE OR REPLACE FUNCTION public.kr_signed_position_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_position_0063$
DECLARE
    old_meta jsonb;
    new_meta jsonb;
    frozen_key text;
BEGIN
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy <> 'pb1_pullback_close'
       OR length(COALESCE(old_meta ->> 'entry_contract_sha256', '')) <> 64 THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
    IF NEW.entry_exit_plan_json::jsonb IS DISTINCT FROM OLD.entry_exit_plan_json::jsonb
       OR ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
              NEW.position_cycle_id, NEW.portfolio_epoch_id, NEW.trading_epoch_id,
              NEW.entry_style_selected, NEW.entry_reason, NEW.entry_decision_family,
              NEW.entry_rule_version, NEW.entry_thesis, NEW.trade_horizon,
              NEW.eod_action, NEW.force_eod_close, NEW.exit_policy_family,
              NEW.policy_source, NEW.policy_version, NEW.initial_stop_price,
              NEW.initial_risk_r, NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
          IS DISTINCT FROM
          ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
              OLD.position_cycle_id, OLD.portfolio_epoch_id, OLD.trading_epoch_id,
              OLD.entry_style_selected, OLD.entry_reason, OLD.entry_decision_family,
              OLD.entry_rule_version, OLD.entry_thesis, OLD.trade_horizon,
              OLD.eod_action, OLD.force_eod_close, OLD.exit_policy_family,
              OLD.policy_source, OLD.policy_version, OLD.initial_stop_price,
              OLD.initial_risk_r, OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
    THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_CONTRACT_IMMUTABLE';
    END IF;

    -- Dynamic entry_meta fields (TP completion, watermark, peak, trail) are
    -- intentionally writable. Only immutable source identities are frozen.
    FOREACH frozen_key IN ARRAY ARRAY[
        'entry_contract_sha256', 'entry_contract_version',
        'entry_exit_plan_sha256', 'source_buy_order_id',
        'source_buy_client_order_key', 'entry_style_selected',
        'entry_reason', 'entry_decision_family', 'entry_rule_version',
        'entry_thesis', 'trade_horizon', 'exit_policy_family',
        'eod_action', 'force_eod_close', 'initial_stop_price',
        'initial_risk_r', 'policy_source', 'policy_version'
    ] LOOP
        IF new_meta -> frozen_key IS DISTINCT FROM old_meta -> frozen_key THEN
            RAISE EXCEPTION USING MESSAGE = 'KR_SIGNED_POSITION_SOURCE_META_IMMUTABLE:' || frozen_key;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$kr_position_0063$;

DROP TRIGGER IF EXISTS kr_signed_position_guard_0063 ON public.positions;
CREATE TRIGGER kr_signed_position_guard_0063
BEFORE UPDATE OR DELETE ON public.positions FOR EACH ROW
EXECUTE FUNCTION public.kr_signed_position_guard_0063();

-- Broker ACK fields such as kis_odno, response_json and order status remain mutable.
-- No backfill, no old-row mutation, no role bypass and no KIS side effects.
-- Rollback plan: DROP TRIGGER IF EXISTS kr_signed_*_0063; then DROP FUNCTION...
