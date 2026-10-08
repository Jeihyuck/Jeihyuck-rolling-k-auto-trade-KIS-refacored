"""Opt-in, broker-proven cancellation of a conflicting US_STANDARD TP SELL.

The existing execution claim remains fenced after a cancel ACK. The next tick
may route a protective SELL *only after* normal KIS terminal reconciliation
settles prior fills, open quantity, TP stage, and the execution claim.

Never cancel on local stale status alone; never release a claim on cancel ACK.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Callable

logger = logging.getLogger(__name__)

_PROTECTIVE_EXITS = frozenset({
    "hard_stop", "hard_stop_loss", "hard_stop_full_exit",
    "soft_stop", "soft_stop_loss", "persistent_soft_stop", "persistent_soft_stop_full_exit",
    "profit_trailing_stop", "trailing_stop", "giveback", "profit_protect",
})


def protective_symbols(intents: list[dict]) -> set[str]:
    """Identify only STANDARD protective sells from their actual owner/producer.

    The production us_exit_engine._make_exit_intent emits strategy='us_pb1_exit'
    without a top-level strategy_owner.  Never require an owner tag that this
    producer does not write; never infer standard ownership for TQQQ or an
    unknown producer. The persisted TP order owner/lifecycle is checked again
    before any cancel request.
    """
    result: set[str] = set()
    for intent in intents:
        if str(intent.get("side") or "").upper() != "SELL":
            continue
        if str(intent.get("exit_type") or "").lower() not in _PROTECTIVE_EXITS:
            continue
        meta = intent.get("meta") if isinstance(intent.get("meta"), dict) else {}
        owner = str(intent.get("strategy_owner") or meta.get("strategy_owner") or "").upper()
        producer = str(intent.get("strategy") or meta.get("strategy") or "").lower()
        if owner not in {"", "US_STANDARD"}:
            continue
        if owner != "US_STANDARD" and producer != "us_pb1_exit":
            continue
        symbol = str(intent.get("symbol") or "").upper()
        if symbol and symbol != "TQQQ":
            result.add(symbol)
    return result


def broker_proves_open_tp_order(order: dict, broker: dict) -> bool:
    """An ACK or local OPEN row alone is never an actionable cancel proof."""
    if not isinstance(order, dict) or not isinstance(broker, dict):
        return False
    try:
        requested = int(order.get("qty_requested") or 0)
        original_filled = int(order.get("qty_filled") or 0)
        broker_requested = int(broker.get("requested_qty"))
        broker_filled = int(broker.get("filled_qty"))
        remaining = int(broker.get("remaining_qty"))
    except (ValueError, TypeError):
        return False
    if requested <= 0 or requested != broker_requested:
        return False
    if remaining <= 0 or broker_filled < original_filled:
        return False
    if broker_filled + remaining != requested:
        return False
    if broker.get("filled_qty_present") is not True:
        return False
    if str(broker.get("status") or "").upper() not in {"OPEN", "PARTIALLY_FILLED"}:
        return False
    if str(broker.get("normalization_result") or "").lower() != "normalized":
        return False
    broker_no = str(broker.get("order_no") or "").lstrip("0")
    local_no = str(order.get("order_no") or "").lstrip("0")
    return bool(local_no and broker_no and broker_no == local_no)


def broker_proves_terminal_after_cancel(order: dict, broker: dict) -> bool:
    """Only an explicit broker terminal fill/zero-open observation permits reconciliation."""
    if not isinstance(order, dict) or not isinstance(broker, dict):
        return False
    if str(broker.get("status") or "").upper() not in {"CANCELLED", "CANCELED", "FILLED"}:
        return False
    if broker.get("filled_qty_present") is not True:
        return False
    try:
        requested = int(order.get("qty_requested"))
        broker_requested = int(broker.get("requested_qty"))
        filled = int(broker.get("filled_qty"))
        remaining = int(broker.get("remaining_qty"))
    except (TypeError, ValueError):
        return False
    if requested <= 0 or requested != broker_requested or filled < int(order.get("qty_filled") or 0):
        return False
    if remaining != 0 or (str(broker.get("status") or "").upper() == "FILLED" and filled != requested):
        return False
    if str(order.get("order_no") or "").lstrip("0") != str(broker.get("order_no") or "").lstrip("0"):
        return False
    return str(broker.get("normalization_result") or "").lower() == "normalized"


def load_conflicting_open_tp_orders(symbol: str, trade_date: str, *, env: str) -> list[dict]:
    """Only the active epoch and matching strategy-owned broker-order rows."""
    from sqlalchemy import text
    from trader.us.db.repos import _active_us_epoch, _get_engine_or_none

    engine = _get_engine_or_none()
    if engine is None:
        raise RuntimeError("US TP recovery requires durable database")
    with engine.connect() as conn:
        epoch = _active_us_epoch(conn)
        if not epoch:
            raise RuntimeError("US TP recovery requires an active trading epoch")
        return [dict(row) for row in conn.execute(text("""
            SELECT o.id,o.trade_date,o.symbol,o.order_no,o.status,
                   o.qty_requested,o.qty_filled,o.client_order_key,
                   o.exchange,o.meta,o.trading_epoch_id,
                   c.action_state AS claim_state
            FROM us_orders o
            LEFT JOIN us_execution_claims c
              ON c.trading_epoch_id=o.trading_epoch_id
             AND c.trade_date=o.trade_date
             AND c.env=o.env AND c.market='US'
             AND c.strategy_owner='US_STANDARD'
             AND c.lifecycle_id=o.meta->>'position_lifecycle_id'
             AND c.action=UPPER(o.meta->>'profit_capture_stage')
            WHERE o.symbol=:symbol AND o.side='SELL' AND o.env=:env
              AND o.trade_date<=:trade_date AND o.trading_epoch_id=:epoch
              AND COALESCE(o.meta->>'profit_capture_stage','') IN ('tp1','tp2','tp3')
              AND o.qty_requested > o.qty_filled AND NULLIF(o.order_no,'') IS NOT NULL
              AND (
                o.status IN ('ACK','OPEN','PARTIALLY_FILLED','RECONCILE_PENDING')
                OR (o.meta->>'protective_tp_cancel_requested_at' IS NOT NULL
                    AND c.action_state IN ('IN_FLIGHT','UNCERTAIN'))
              )
            ORDER BY o.created_at
        """), {"symbol": symbol, "env": env,
                "trade_date": trade_date, "epoch": epoch}).mappings()]


def reserve_cancel_request(order: dict) -> bool:
    """One durable write before KIS cancel. No repeated cancel after ambiguity."""
    from sqlalchemy import text
    from trader.us.db.repos import _get_engine_or_none

    engine = _get_engine_or_none()
    if engine is None:
        return False
    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE us_orders
            SET meta = COALESCE(meta,'{}'::jsonb)
                || jsonb_build_object(
                    'protective_tp_cancel_requested_at', NOW()::text,
                    'protective_tp_cancel_reason', 'BROKER_PROVEN_OPEN_CONFLICT'),
                updated_at=NOW()
            WHERE id=:id AND trading_epoch_id=:epoch
              AND symbol=:symbol AND client_order_key=:order_key
              AND meta->>'position_lifecycle_id'=:lifecycle
              AND meta->>'strategy_owner'='US_STANDARD'
              AND status IN ('ACK','OPEN','PARTIALLY_FILLED','RECONCILE_PENDING')
              AND meta->>'protective_tp_cancel_requested_at' IS NULL
              AND EXISTS (
                  SELECT 1 FROM us_execution_claims c
                  WHERE c.trading_epoch_id=us_orders.trading_epoch_id
                    AND c.trade_date=us_orders.trade_date
                    AND c.env=us_orders.env
                    AND c.market='US'
                    AND c.strategy_owner='US_STANDARD'
                    AND c.lifecycle_id=:lifecycle
                    AND c.action=UPPER(us_orders.meta->>'profit_capture_stage')
                    AND c.action_state IN ('IN_FLIGHT','UNCERTAIN','PARTIALLY_SATISFIED')
              )
            RETURNING id
        """), {"id": order["id"], "epoch": order["trading_epoch_id"],
                 "symbol": order["symbol"], "order_key": order["client_order_key"],
                 "lifecycle": str((order.get("meta") or {}).get("position_lifecycle_id") or "")})
        return result.first() is not None


def request_protective_tp_cancel(
    *, intents: list[dict], provider: Any, kis_client: Any,
    trade_date: str, env: str,
    find_open: Callable[..., list[dict]] = load_conflicting_open_tp_orders,
    reserve: Callable[[dict], bool] = reserve_cancel_request,
    apply_observation: Callable[..., dict] | None = None,
) -> list[dict]:
    """One cancel request per tick at most, with independently verified order proof."""
    if os.getenv("US_PROTECTIVE_TP_CANCEL_RECOVERY_ENABLED", "0") != "1":
        return []
    if kis_client is None or provider is None:
        return [{"status": "RECOVERY_UNAVAILABLE", "reason": "broker_provider_missing"}]
    results: list[dict] = []
    # Bind a broker TP reservation to the exact protective lifecycle, not merely
    # the ticker (which could have been closed and repurchased).
    lifecycle_by_symbol = {
        str(intent.get("symbol") or "").upper():
            str(intent.get("position_lifecycle_id")
                or (intent.get("meta") or {}).get("position_lifecycle_id") or "")
        for intent in intents
        if str(intent.get("side") or "").upper() == "SELL"
        and str(intent.get("exit_type") or "").lower() in _PROTECTIVE_EXITS
    }
    for symbol in sorted(protective_symbols(intents)):
        try:
            orders = find_open(symbol, trade_date, env=env)
            if not orders:
                # No pending TP row for this epoch/lifecycle. Ordinary router
                # broker-order/risk fences still apply to every protective SELL.
                continue
            if len(orders) != 1:
                results.append({"symbol": symbol, "status": "FENCED", "reason": "multiple_tp_open_or_claims"})
                continue
            order = orders[0]
            meta = order.get("meta") if isinstance(order.get("meta"), dict) else {}
            if (
                str(meta.get("strategy_owner") or "").upper() != "US_STANDARD"
                or not lifecycle_by_symbol.get(symbol)
                or str(meta.get("position_lifecycle_id") or "") != lifecycle_by_symbol[symbol]
            ):
                results.append({"symbol": symbol, "status": "FENCED",
                                "reason": "owner_or_lifecycle_unverified"})
                continue
            evidence = provider.get_fills_by_order_no(
                order_no=str(order["order_no"]), symbol=symbol,
                trade_date=str(order["trade_date"]),
            )
            if order.get("claim_state") not in {"IN_FLIGHT", "UNCERTAIN", "PARTIALLY_SATISFIED"}:
                results.append({"symbol": symbol, "status": "FENCED", "reason": "tp_claim_unverified"})
                continue
            if meta.get("protective_tp_cancel_requested_at"):
                # Revisit the original trade date, even after the local session
                # rolls forward. Never infer terminal status from the cancel ACK.
                if not broker_proves_terminal_after_cancel(order, evidence):
                    results.append({"symbol": symbol, "status": "CANCEL_PENDING_RECONCILE_REQUIRED"})
                    break
                if apply_observation is None:
                    from trader.us.db.repos import apply_broker_order_observation as _apply
                else:
                    _apply = apply_observation
                from trader.us.db.repos import normalize_us_order_no
                status = str(evidence["status"]).upper().replace("CANCELED", "CANCELLED")
                settled = _apply(
                    trade_date=str(order["trade_date"]),
                    client_order_key=str(order["client_order_key"]),
                    raw_order_no=str(order["order_no"]),
                    canonical_order_no=normalize_us_order_no(order["order_no"]),
                    symbol=symbol, side="SELL",
                    requested_qty=int(order["qty_requested"]),
                    filled_qty=int(evidence["filled_qty"]), remaining_qty=0,
                    broker_status=status,
                    evidence_type="KIS_TERMINAL_CANCEL" if status == "CANCELLED" else "KIS_ORDER_CUMULATIVE_ACTUAL",
                    raw_row=evidence, broker_open_qty=0,
                )
                accepted = (
                    isinstance(settled, dict)
                    and settled.get("status") == "OK"
                    and settled.get("authoritative") is True
                    and not settled.get("requires_reconcile")
                )
                results.append({"symbol": symbol,
                                "status": "CANCEL_TERMINAL_RECONCILED" if accepted
                                          else "CANCEL_PENDING_RECONCILE_REQUIRED"})
                break
            if not broker_proves_open_tp_order(order, evidence):
                results.append({"symbol": symbol, "status": "FENCED", "reason": "broker_open_proof_missing"})
                continue
            if not reserve(order):
                results.append({"symbol": symbol, "status": "FENCED", "reason": "cancel_already_reserved"})
                continue
            # KIS cancel ACK is NOT terminal cancellation / permission to resell.
            reply = kis_client.cancel_us_order(
                symbol=symbol, exchange=str(order.get("exchange") or ""),
                order_no=str(order["order_no"]),
            )
            status = "CANCEL_ACK_RECONCILE_REQUIRED" if isinstance(reply, dict) and str(reply.get("rt_cd")) == "0" else "CANCEL_RESULT_UNKNOWN_RECONCILE_REQUIRED"
            results.append({"symbol": symbol, "status": status, "broker_submit": False})
            logger.warning("[US_PROTECTIVE_TP][%s] symbol=%s order_no=%s next=broker_terminal_reconcile", status, symbol, order["order_no"])
            break
        except Exception as exc:
            logger.error("[US_PROTECTIVE_TP][RECOVERY_FAILED_FENCED] symbol=%s err=%s", symbol, exc)
            results.append({"symbol": symbol, "status": "FENCED", "reason": "recovery_exception"})
    return results
