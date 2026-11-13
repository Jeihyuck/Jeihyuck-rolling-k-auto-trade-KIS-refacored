"""Evidence checks for continuing a partially filled US_STANDARD TP.

The caller must hold the stage and execution-claim locks while reading these
rows. This function does not query KIS or grant authority by itself.
"""
from __future__ import annotations


def _raw_qty(raw, names):
    for name in names:
        value = raw.get(name)
        if value not in (None, ""):
            return int(value)
    raise ValueError("missing broker quantity")


def proven_partial_tp_remainder(*, claim, current_attempt, prior_attempts,
                                prior_orders, stage, intent_meta, account,
                                cycle, symbol, stage_name, qty):
    """Require every previous submitted attempt to have terminal KIS proof."""
    try:
        target = int(claim["target_qty"])
        filled = int(claim["filled_qty_before_attempt"])
        stage_filled = int(stage["cumulative_filled_qty"])
        contract = str(intent_meta.get("source_entry_contract_sha256") or "")
        if (not contract or filled <= 0 or stage_filled != filled
                or int(stage["requested_qty"]) != target
                or int(current_attempt["requested_qty"]) != int(qty)
                or target - filled != int(qty)
                or str(claim["lifecycle_id"]) != cycle
                or str(claim["action"]).lower() != stage_name
                or not prior_attempts):
            return False
        orders = {str(order["client_order_key"]): order for order in prior_orders}
        if len(orders) != len(prior_orders):
            return False
        total = 0
        used = set()
        for attempt in prior_attempts:
            key = str(attempt.get("client_order_key") or "")
            if (not key or key in used or not attempt.get("authoritative")
                    or attempt.get("attempt_state") != "CANCELLED_PARTIAL_FILL"):
                return False
            used.add(key)
            order = orders.get(key)
            if not order:
                return False
            meta = order.get("meta") or {}
            raw = meta.get("broker_raw_row") or {}
            requested = int(attempt["requested_qty"])
            executed = int(attempt["cumulative_filled_qty"])
            if (not 0 < executed < requested
                    or order.get("status") != "CANCELLED"
                    or str(order.get("symbol") or "").upper() != symbol
                    or str(order.get("side") or "").upper() != "SELL"
                    or not str(order.get("order_no") or "").strip()
                    or int(order["qty_requested"]) != requested
                    or int(order["qty_filled"]) != executed
                    or meta.get("fill_evidence_type") not in {"KIS_ORDER_CUMULATIVE_ACTUAL", "KIS_ORDER_DETAIL_ACTUAL"}
                    or int(meta["broker_open_qty"]) != 0
                    or raw.get("filled_qty_present") is not True
                    or raw.get("remaining_qty_present") is not True
                    or _raw_qty(raw, ("filled_qty", "qty_filled", "tot_ccld_qty", "ft_ccld_qty", "ccld_qty")) != executed
                    or _raw_qty(raw, ("requested_qty", "qty_requested", "qty", "ord_qty", "ft_ord_qty", "ORD_QTY")) != requested
                    or _raw_qty(raw, ("broker_open_qty", "remaining_qty", "rmn_qty", "ord_remn_qty", "nccs_qty")) != 0
                    or str(meta.get("submit_attempt_id") or "") != str(attempt["attempt_id"])
                    or str(meta.get("execution_action_key") or "") != str(claim["action_key"])
                    or str(meta.get("broker_account_hash") or "") != account
                    or str(meta.get("position_lifecycle_id") or "") != cycle
                    or str(meta.get("profit_capture_stage") or "").lower() != stage_name
                    or str(meta.get("strategy_owner") or "").upper() != "US_STANDARD"
                    or str(meta.get("source_entry_contract_sha256") or "") != contract):
                return False
            for field in ("portfolio_epoch_id", "position_cycle_id"):
                value = str(intent_meta.get(field) or "")
                if not value or str(meta.get(field) or "") != value:
                    return False
            total += executed
        return len(orders) == len(used) and total == filled
    except (KeyError, TypeError, ValueError, OverflowError):
        return False
