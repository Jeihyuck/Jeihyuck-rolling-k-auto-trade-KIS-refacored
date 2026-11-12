-- 0063: immutable signed KR PB1 BUY source contracts and linked positions.
-- No existing trade rows are rewritten. KIS ACK, fill status, position qty,
-- peak/trailing stop, and staged-TP runtime state remain mutable.
-- Note: the application's _assert_kr_buy_entry_contract validates the actual
-- canonical SHA; PostgreSQL checks signature *format* and invariant identity.
-- This migration executes inside trader.db.migrate's own transaction.
-- DB superusers/migration owners can disable triggers; no WORM claim.

CREATE OR REPLACE FUNCTION public.kr_signed_buy_order_guard_0063()
RETURNS trigger LANGUAGE plpgsql AS $kr_buy_0063$
DECLARE
    old_req jsonb;
    new_req jsonb;
    signed_old boolean;
BEGIN
    IF TG_OP = 'INSERT' THEN
        new_req := COALESCE(NEW.request_json::jsonb, '{}'::jsonb);
        IF NEW.side = 'BUY' AND NEW.strategy = 'pb1_pullback_close'
           AND new_req ->> 'enforce_entry_contract' = 'true' THEN
            IF COALESCE(new_req ->> 'entry_contract_sha256', '') !~ '^[0-9a-f]{64}$'
               OR COALESCE(new_req ->> 'entry_contract_version', '') = ''
               OR COALESCE(new_req -> 'entry_exit_plan', '{}'::jsonb) = '{}'::jsonb
               OR COALESCE(new_req ->> 'client_order_key', '') = ''
               OR COALESCE(new_req ->> 'position_cycle_id', '') = ''
               OR COALESCE(new_req ->> 'portfolio_epoch_id', '') = ''
               OR COALESCE(new_req ->> 'trading_epoch_id', '') = ''
               OR new_req ->> 'client_order_key' IS DISTINCT FROM NEW.client_order_key
               OR new_req ->> 'position_cycle_id' IS DISTINCT FROM NEW.position_cycle_id
               OR new_req ->> 'portfolio_epoch_id' IS DISTINCT FROM NEW.portfolio_epoch_id
               OR new_req ->> 'trading_epoch_id' IS DISTINCT FROM NEW.trading_epoch_id
               OR COALESCE(new_req ->> 'requested_qty', '') !~ '^[0-9]+$'
               OR COALESCE(new_req ->> 'submitted_qty', '') !~ '^[0-9]+$'
               OR COALESCE((new_req ->> 'requested_qty')::integer, 0) <= 0
               OR COALESCE((new_req ->> 'submitted_qty')::integer, 0) <= 0
               OR (new_req ->> 'requested_qty')::integer IS DISTINCT FROM NEW.qty
            THEN
                RAISE EXCEPTION 'KR_SIGNED_BUY_INTENT_CONTRACT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;

    old_req := COALESCE(OLD.request_json::jsonb, '{}'::jsonb);
    signed_old := OLD.side = 'BUY'
        AND OLD.strategy = 'pb1_pullback_close'
        AND (old_req ->> 'enforce_entry_contract' = 'true'
             OR old_req ->> 'entry_contract_sha256' IS NOT NULL);

    IF OLD.side = 'BUY' AND OLD.strategy = 'pb1_pullback_close'
       AND NOT signed_old AND TG_OP = 'UPDATE' THEN
        new_req := COALESCE(NEW.request_json::jsonb, '{}'::jsonb);
        IF new_req ->> 'enforce_entry_contract' = 'true'
           OR new_req ->> 'entry_contract_sha256' IS NOT NULL THEN
            RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_BUY_REBIND_REQUIRES_VERIFIED_REPAIR';
        END IF;
    END IF;
    IF NOT signed_old THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_ORDER_DELETE_BLOCKED';
    END IF;
    IF COALESCE(old_req ->> 'entry_contract_sha256', '') !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'KR_SIGNED_BUY_OLD_CONTRACT_CORRUPT_UNVERIFIED';
    END IF;
    new_req := COALESCE(NEW.request_json::jsonb, '{}'::jsonb);
    IF ROW(NEW.env, NEW.strategy, NEW.sid, NEW.mode, NEW.code, NEW.market,
           NEW.side, NEW.client_order_key, NEW.position_cycle_id,
           NEW.portfolio_epoch_id, NEW.trading_epoch_id, NEW.qty,
           NEW.ord_type, NEW.entry_reason, NEW.entry_style_selected,
           NEW.entry_decision_family, NEW.entry_rule_version,
           NEW.stop_price_at_entry, NEW.pivot_price_at_entry)
       IS DISTINCT FROM
       ROW(OLD.env, OLD.strategy, OLD.sid, OLD.mode, OLD.code, OLD.market,
           OLD.side, OLD.client_order_key, OLD.position_cycle_id,
           OLD.portfolio_epoch_id, OLD.trading_epoch_id, OLD.qty,
           OLD.ord_type, OLD.entry_reason, OLD.entry_style_selected,
           OLD.entry_decision_family, OLD.entry_rule_version,
           OLD.stop_price_at_entry, OLD.pivot_price_at_entry)
       OR new_req -> 'entry_exit_plan' IS DISTINCT FROM old_req -> 'entry_exit_plan'
       OR new_req -> 'entry_contract_sha256' IS DISTINCT FROM old_req -> 'entry_contract_sha256'
       OR new_req -> 'entry_contract_version' IS DISTINCT FROM old_req -> 'entry_contract_version'
       OR new_req -> 'enforce_entry_contract' IS DISTINCT FROM old_req -> 'enforce_entry_contract'
       OR new_req -> 'client_order_key' IS DISTINCT FROM old_req -> 'client_order_key'
       OR new_req -> 'requested_qty' IS DISTINCT FROM old_req -> 'requested_qty'
       OR new_req -> 'submitted_qty' IS DISTINCT FROM old_req -> 'submitted_qty'
       OR new_req -> 'pre_order_holding_qty' IS DISTINCT FROM old_req -> 'pre_order_holding_qty'
       OR new_req -> 'balance_snapshot_id' IS DISTINCT FROM old_req -> 'balance_snapshot_id'
       OR new_req -> 'strategy_owner' IS DISTINCT FROM old_req -> 'strategy_owner'
       OR new_req -> 'position_cycle_id' IS DISTINCT FROM old_req -> 'position_cycle_id'
       OR new_req -> 'portfolio_epoch_id' IS DISTINCT FROM old_req -> 'portfolio_epoch_id'
       OR new_req -> 'trading_epoch_id' IS DISTINCT FROM old_req -> 'trading_epoch_id'
       OR new_req -> 'parent_entry_contract_sha256'
          IS DISTINCT FROM old_req -> 'parent_entry_contract_sha256'
       OR new_req -> 'parent_contract_binding_sha256'
          IS DISTINCT FROM old_req -> 'parent_contract_binding_sha256'
       OR new_req -> 'parent_position_cycle_id'
          IS DISTINCT FROM old_req -> 'parent_position_cycle_id'
       OR new_req -> 'parent_portfolio_epoch_id'
          IS DISTINCT FROM old_req -> 'parent_portfolio_epoch_id'
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
           AND new_meta ->> 'entry_contract_sha256' IS NOT NULL THEN
            IF COALESCE(new_meta ->> 'entry_contract_sha256', '') !~ '^[0-9a-f]{64}$'
               OR COALESCE(new_meta ->> 'entry_exit_plan_sha256', '') !~ '^[0-9a-f]{64}$'
               OR COALESCE(NEW.entry_exit_plan_json::jsonb, '{}'::jsonb) = '{}'::jsonb
               OR COALESCE(new_meta ->> 'source_buy_order_id', '') = ''
               OR COALESCE(NEW.position_cycle_id, '') = ''
               OR COALESCE(NEW.portfolio_epoch_id, '') = ''
               OR COALESCE(NEW.trading_epoch_id, '') = ''
            THEN
                RAISE EXCEPTION 'KR_SIGNED_POSITION_INSERT_INCOMPLETE';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    old_meta := COALESCE(OLD.entry_meta_json::jsonb, '{}'::jsonb);
    IF OLD.strategy = 'pb1_pullback_close'
       AND old_meta ->> 'entry_contract_sha256' IS NULL AND TG_OP = 'UPDATE' THEN
        new_meta := COALESCE(NEW.entry_meta_json::jsonb, '{}'::jsonb);
        IF new_meta ->> 'entry_contract_sha256' IS NOT NULL THEN
            RAISE EXCEPTION 'KR_LEGACY_UNSIGNED_POSITION_REBIND_REQUIRES_VERIFIED_REPAIR';
        END IF;
    END IF;
    IF OLD.strategy <> 'pb1_pullback_close'
       OR old_meta ->> 'entry_contract_sha256' IS NULL THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_DELETE_BLOCKED';
    END IF;
    IF COALESCE(old_meta ->> 'entry_contract_sha256', '') !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'KR_SIGNED_POSITION_OLD_CONTRACT_CORRUPT_UNVERIFIED';
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
