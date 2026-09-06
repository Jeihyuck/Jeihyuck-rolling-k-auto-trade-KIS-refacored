from __future__ import annotations

from typing import Any

from trader.execution_state import SELL_GUARD_STATES, BalanceFreshness, legal_next_exit_stage

CONFIRMED_SELL_STATUSES = {"FILLED", "PARTIAL_FILLED", "FILLED_QTY_CONFIRMED_PRICE_UNRESOLVED"}


def durable_sell_row_decision(
    *,
    row: dict[str, Any],
    strategy_name: str,
    position_cycle_id: str | None,
    exit_stage: str | None,
    session: str,
    balance_fresh: bool,
    remaining_qty: int,
) -> tuple[bool, str | None]:
    if str(row.get("strategy") or "") != str(strategy_name):
        return False, None
    row_cycle = str(row.get("position_cycle_id") or "")
    if position_cycle_id and row_cycle and row_cycle != str(position_cycle_id):
        return False, None
    request = row.get("request_json") if isinstance(row.get("request_json"), dict) else {}
    row_session = str(request.get("trade_session") or "").lower()
    if row_session and row_session != str(session).lower():
        return False, None

    prior_stage = str(row.get("stage") or request.get("exit_stage") or "")
    prior_status = str(row.get("status") or "").upper()
    if prior_stage == "TP1" and str(exit_stage or "").upper() == "TP2" and prior_status not in CONFIRMED_SELL_STATUSES:
        return True, "execution_unconfirmed"
    if prior_status not in SELL_GUARD_STATES:
        return False, None
    if prior_status in CONFIRMED_SELL_STATUSES:
        prior_submitted = int(request.get("submitted_qty") or row.get("qty") or 0)
        prior_pre_qty = int(request.get("pre_order_holding_qty") or 0)
        prior_was_partial = (
            prior_stage in {"TP1", "TP2", "PROFIT_PROTECT_PARTIAL_1", "DEFENSE_TRIM_1"}
            or (prior_pre_qty > 0 and 0 < prior_submitted < prior_pre_qty)
        )
        if balance_fresh and remaining_qty > 0 and prior_was_partial and str(exit_stage or "").upper() == "FULL_EXIT":
            return False, None
        if balance_fresh and remaining_qty > 0 and prior_stage and exit_stage and legal_next_exit_stage(prior_stage, exit_stage):
            return False, None
    return True, "durable_skip"


def durable_sell_block(
    *,
    orders_repo: Any,
    env: str,
    code: str,
    strategy_name: str,
    authoritative_balance: Any,
    window_internal: str | None,
    position_cycle_id: str | None = None,
    exit_stage: str | None = None,
    logger: Any | None = None,
) -> tuple[bool, dict[str, Any] | None]:
    try:
        rows = orders_repo.list_today_orders(env, side="SELL", code=str(code).zfill(6), status_exclude=())
    except Exception:
        if logger is not None:
            logger.exception("[SELL_SESSION_BLOCK][DURABLE_LOOKUP_FAIL] code=%s action=fail_closed", code)
        return True, {"status": "LOOKUP_FAILED"}

    session = str(window_internal or "day").lower()
    balance_fresh = bool(authoritative_balance and authoritative_balance.freshness is BalanceFreshness.FRESH)
    remaining = authoritative_balance.holding_qty(code) if authoritative_balance else 0

    for row in rows or []:
        blocked, reason = durable_sell_row_decision(
            row=row,
            strategy_name=strategy_name,
            position_cycle_id=position_cycle_id,
            exit_stage=exit_stage,
            session=session,
            balance_fresh=balance_fresh,
            remaining_qty=remaining,
        )
        if not blocked:
            continue
        if reason == "execution_unconfirmed":
            if logger is not None:
                logger.info(
                    "[SELL_STAGE_BLOCK] code=%s prior_stage=TP1 prior_status=%s requested_stage=TP2 reason=execution_unconfirmed",
                    str(code).zfill(6),
                    str(row.get("status") or "").upper(),
                )
            return True, dict(row)
        if logger is not None:
            logger.info(
                "[SELL_SESSION_BLOCK][DURABLE_SKIP] code=%s cycle=%s prior_order_id=%s",
                str(code).zfill(6),
                str(row.get("position_cycle_id") or ""),
                row.get("kis_odno") or row.get("order_id"),
            )
        return True, dict(row)
    return False, None
