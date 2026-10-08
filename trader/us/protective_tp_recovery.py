"""Opt-in, broker-proven cancellation of a conflicting US_STANDARD TP SELL.

The existing execution claim remains fenced after a cancel ACK. The next tick
may route a protective SELL *only after* normal KIS terminal reconciliation
settles prior fills, open quantity, TP stage, and the execution claim.

Never cancel on local stale status alone; never release a claim on cancel ACK.
"""
from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from typing import Any, Callable

logger = logging.getLogger(__name__)

_PROTECTIVE_EXITS = frozenset({
    "hard_stop", "hard_stop_loss", "hard_stop_full_exit",
    "soft_stop", "soft_stop_loss", "persistent_soft_stop", "persistent_soft_stop_full_exit",
    "profit_trailing_stop", "trailing_stop", "giveback",
})


def protective_symbols(intents: list[dict]) -> set[str]:
    result: set[str] = set()
    for intent in intents:
        if (
            str(intent.get("side") or "").upper() == "SELL"
            and str(intent.get("exit_type") or "").lower() in _PROTECTIVE_EXITS
            and str(intent.get("strategy_owner") or (intent.get("meta") or {}).get("strategy_owner") or "").upper()
                == "US_STANDARD"
        ):
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
    if str(broker.get("normalization_result") or "normalized").lower() != "normalized":
        return False
    broker_no = str(broker.get("order_no") or "").lstrip("0")
    local_no = str(order.get("order_no") or "").lstrip("0")
    return bool(local_no and broker_no and broker_no == local_no)


def load_conflicting_open_tp_orders(symbol: str, trade_date: str, *, env: str) -> list[dict]:
    """Only the active epoch and matching strategy-owned broker-order rows."""
    from sqlalchemy import text
    from trader.us.db.repos import _active_us_epoch, _get_engine_or_none

    engine = _get_engine_or_none()
    if engine is None:
        raise RuntimeError("US TP recovery requires durable database")
    earliest = (date.fromisoformat(trade_date) - timedelta(days=10)).isoformat()
    with engine.connect() as conn:
        epoch = _active_us_epoch(conn)
        if not epoch:
            raise RuntimeError("US TP recovery requires an active trading epoch")
        return [dict(row) for row in conn.execute(text("""
            SELECT id,trade_date,symbol,order_no,qty_requested,qty_filled,
                   client_order_key,exchange,meta,trading_epoch_id
            FROM us_orders
            WHERE symbol=:symbol AND side='SELL' AND env=:env
              AND trade_date BETWEEN :earliest AND :trade_date
              AND trading_epoch_id=:epoch
              AND status IN ('ACK','OPEN','PARTIALLY_FILLED','RECONCILE_PENDING')
              AND COALESCE(meta->>'profit_capture_stage','') IN ('tp1','tp2','tp3')
              AND qty_requested > qty_filled AND NULLIF(order_no,'') IS NOT NULL
            ORDER BY created_at
        """), {"symbol": symbol, "env": env, "earliest": earliest,
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
              AND status IN ('ACK','OPEN','PARTIALLY_FILLED','RECONCILE_PENDING')
              AND meta->>'protective_tp_cancel_requested_at' IS NULL
            RETURNING id
        """), {"id": order["id"], "epoch": order["trading_epoch_id"]})
        return result.first() is not None


def request_protective_tp_cancel(
    *, intents: list[dict], provider: Any, kis_client: Any,
    trade_date: str, env: str,
    find_open: Callable[..., list[dict]] = load_conflicting_open_tp_orders,
    reserve: Callable[[dict], bool] = reserve_cancel_request,
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
            if len(orders) != 1:
                # Multiple broker-open orders are ambiguous, not safe to cancel automatically.
                results.append({"symbol": symbol, "status": "FENCED", "reason": "open_tp_count_not_one"})
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
