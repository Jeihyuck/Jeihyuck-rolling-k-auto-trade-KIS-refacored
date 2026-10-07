# -*- coding: utf-8 -*-
"""US Positions Reconcile.

KIS 잔고와 로컬 DB 잔고를 비교 검증.
"""
from __future__ import annotations

import inspect
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

_FILL_CONTRACT_ERROR_STATUSES = {
    "EVIDENCE_QUANTITY_REGRESSION",
    "EVIDENCE_QUANTITY_CONFLICT",
    "EVIDENCE_QUANTITY_OVERFLOW",
    "FILL_ACCOUNTING_INVARIANT_FAILED",
    "RECONCILE_UPDATE_FAILED",
    "CONTRACT_ERROR",
    "ERROR",
}

_BROKER_CANCEL_STATUSES = frozenset({
    "CANCELED", "CANCELLED", "CANCEL_COMPLETE", "CANCELLED_COMPLETE",
    "취소", "취소완료",
})
_BROKER_REJECT_STATUSES = frozenset({"REJECT", "REJECTED", "거부"})


def _terminal_cancel_quantities(observation: dict | None) -> tuple[str, int | None, int | None, int | None]:
    row = observation or {}
    status = str(row.get("status") or row.get("order_status") or row.get("ord_status") or "").strip().upper()
    if row.get("requested_qty_present") is False:
        return status, None, None, None
    requested_raw = next(
        (row[key] for key in ("requested_qty", "qty_requested", "ord_qty", "ft_ord_qty", "ORD_QTY")
         if row.get(key) not in (None, "")),
        None,
    )
    filled_raw = next(
        (row[key] for key in ("filled_qty", "tot_ccld_qty", "ft_ccld_qty", "ccld_qty")
         if row.get(key) not in (None, "")),
        None,
    )
    remaining_raw = next(
        (row[key] for key in ("broker_open_qty", "remaining_qty", "rmn_qty", "ord_remn_qty", "nccs_qty")
         if row.get(key) not in (None, "")),
        None,
    )
    if (
        row.get("filled_qty_present") is False
        or row.get("remaining_qty_present") is False
        or row.get("broker_open_qty_present") is False
    ):
        return status, None, None, None
    if requested_raw is None or filled_raw is None or remaining_raw is None:
        return status, None, None, None
    try:
        requested = int(float(requested_raw))
        filled = int(float(filled_raw))
        remaining = int(float(remaining_raw))
    except (TypeError, ValueError):
        return status, None, None, None
    if (
        requested <= 0 or float(requested_raw) != requested
        or filled < 0 or float(filled_raw) != filled
        or remaining < 0 or float(remaining_raw) != remaining
        or filled > requested
    ):
        return status, None, None, None
    return status, requested, filled, remaining


def is_terminal_cancel_observation(observation: dict | None) -> bool:
    """Require explicit KIS request, cumulative-fill, terminal-status and open-qty evidence."""
    status, requested, filled, broker_open_qty = _terminal_cancel_quantities(observation)
    return bool(
        status in _BROKER_CANCEL_STATUSES
        and requested is not None and filled is not None and broker_open_qty == 0
    )


def is_terminal_zero_fill_cancel(observation: dict | None) -> bool:
    """Recognize a terminal broker cancellation with explicit zero cumulative fills."""
    status, requested, filled, broker_open_qty = _terminal_cancel_quantities(observation)
    return bool(
        status in _BROKER_CANCEL_STATUSES
        and requested is not None and filled == 0 and broker_open_qty == 0
    )


def is_terminal_zero_fill_rejection(observation: dict | None) -> bool:
    """Require explicit request and zero-fill evidence for a terminal rejection."""
    row = observation or {}
    status = str(row.get("status") or row.get("order_status") or row.get("ord_status") or "").strip().upper()
    if status not in _BROKER_REJECT_STATUSES:
        return False
    if row.get("requested_qty_present") is False or row.get("filled_qty_present") is False:
        return False
    requested_raw = next(
        (row[key] for key in ("requested_qty", "qty_requested", "ord_qty", "ft_ord_qty", "ORD_QTY")
         if row.get(key) not in (None, "")),
        None,
    )
    filled_raw = next(
        (row[key] for key in ("filled_qty", "tot_ccld_qty", "ft_ccld_qty", "ccld_qty")
         if row.get(key) not in (None, "")),
        None,
    )
    try:
        requested = float(requested_raw)
        filled = float(filled_raw)
    except (TypeError, ValueError):
        return False
    return bool(
        requested > 0 and requested.is_integer()
        and filled == 0
    )


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _normalize_meta(meta: Any) -> dict:
    if isinstance(meta, dict):
        return meta
    if isinstance(meta, str) and meta:
        try:
            loaded = json.loads(meta)
            return loaded if isinstance(loaded, dict) else {}
        except Exception:
            return {}
    return {}


def _resolve_order_fill_price(order: dict) -> tuple[float, str | None]:
    avg_price = _safe_float(order.get("avg_price_usd"))
    if avg_price > 0:
        return avg_price, "order_avg_price_usd"

    meta = _normalize_meta(order.get("meta"))
    for key in ("limit_price_usd", "submitted_price_usd", "order_price_usd"):
        price = _safe_float(meta.get(key))
        if price > 0:
            return price, f"order_meta_{key}"

    for key in ("submitted_price_usd", "order_price_usd"):
        price = _safe_float(order.get(key))
        if price > 0:
            return price, key

    intent_meta = _normalize_meta(order.get("intent_meta"))
    price = _safe_float(intent_meta.get("limit_price_usd"))
    if price > 0:
        return price, "intent_limit_price_usd"

    return 0.0, None


def _ny_trade_date() -> str:
    from trader.us.market_calendar import now_ny
    return now_ny().strftime("%Y-%m-%d")


def reconcile_positions(provider: Any | None = None, *, trade_date: str | None = None) -> dict:
    """잔고 조회 및 reconcile.

    Args:
        provider: USDataProvider 인스턴스

    Returns:
        {
            "status": "OK"|"MISMATCH"|"ERROR"|"CONTRACT_ERROR",
            "positions": [...],
            "position_count": int,
            "total_pvs": str,
            "raw_output1_count": int,
            "normalized_position_count": int,
            "position_symbols": list[str],
            "balance_parse_status": str,
            "block_new_entry": bool,
            ...
        }
    """
    if provider is None:
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)
    if not str(trade_date or "").strip():
        trade_date = _ny_trade_date()
        logger.warning("[US_RECONCILE][TRADE_DATE_FALLBACK] source=ny_clock trade_date=%s", trade_date)

    try:
        balance = _get_balance_force_refresh(provider)
    except Exception as exc:
        logger.error("[US_RECONCILE][ERROR] balance fetch failed: %s", exc)
        return {
            "status": "TEMP_ERROR",
            "balance_fetch_status": "FAILED",
            "authoritative_positions": False,
            "preserve_previous_positions": True,
            "error": str(exc),
            "positions": [],
            "position_count": 0,
            "block_new_entry": True,
        }

    positions = balance.get("positions", [])
    total_pvs = balance.get("total_pvs", "0")
    total_pvs_source = balance.get("total_pvs_source", "unknown")
    total_pvs_semantics = balance.get("total_pvs_semantics", "holdings_market_value_usd")
    holdings_market_value_usd = balance.get("holdings_market_value_usd", total_pvs)
    account_equity_usd = balance.get("account_equity_usd")
    account_equity_source = balance.get("account_equity_source", "unavailable_from_current_kis_balance_contract")
    raw_output1_count = balance.get("raw_output1_count", 0)
    normalized_position_count = balance.get("normalized_position_count", 0)
    position_symbols = balance.get("position_symbols", [])
    balance_parse_status = balance.get("balance_parse_status", "UNKNOWN")
    balance_parse_error = balance.get("balance_parse_error")

    if balance.get("balance_complete") is False or balance.get("balance_authoritative") is False:
        logger.warning("[US_RECONCILE][BALANCE_INCOMPLETE] failed_exchanges=%s action=preserve_last_good_block_buy",
                       balance.get("failed_exchanges"))
        return {
            "status": "TEMP_ERROR", "reason": "balance_incomplete",
            "balance_fetch_status": "INCOMPLETE", "authoritative_positions": False,
            "preserve_previous_positions": True, "block_new_entry": True,
            "positions": [], "position_count": 0,
            "failed_exchanges": balance.get("failed_exchanges") or {},
        }

    logger.info("[US_RECONCILE][BALANCE_RAW] output1_count=%d", raw_output1_count)

    if positions:
        logger.info(
            "[US_RECONCILE][POSITIONS_NORMALIZED] count=%d symbols=%s",
            len(positions),
            ",".join(position_symbols),
        )
    else:
        logger.info("[US_RECONCILE][POSITIONS_NORMALIZED] count=0")

    if balance_parse_status not in ("OK", "UNKNOWN"):
        logger.error(
            "[US_RECONCILE][CONTRACT_ERROR] balance_parse_status=%s error=%s",
            balance_parse_status,
            balance_parse_error,
        )
        return {
            "status": "CONTRACT_ERROR",
            "reason": "balance_position_parse_error",
            "position_count": 0,
            "raw_output1_count": raw_output1_count,
            "normalized_position_count": normalized_position_count,
            "positions": [],
            "balance_parse_status": balance_parse_status,
            "balance_parse_error": balance_parse_error,
            "balance_fetch_status": "FAILED",
            "authoritative_positions": False,
            "preserve_previous_positions": True,
            "block_new_entry": True,
        }

    if raw_output1_count > 0 and normalized_position_count == 0:
        logger.error(
            "[US_RECONCILE][CONTRACT_ERROR] raw_output1_count=%d normalized_position_count=0",
            raw_output1_count,
        )
        return {
            "status": "CONTRACT_ERROR",
            "reason": "balance_position_parse_error",
            "position_count": 0,
            "raw_output1_count": raw_output1_count,
            "normalized_position_count": 0,
            "positions": [],
            "balance_parse_status": "CONTRACT_ERROR",
            "balance_parse_error": "raw_output1_nonzero_positions_zero",
            "balance_fetch_status": "FAILED",
            "authoritative_positions": False,
            "preserve_previous_positions": True,
            "block_new_entry": True,
        }

    logger.info(
        "[US_RECONCILE][OK] position_count=%d total_pvs=%s total_pvs_source=%s total_pvs_semantics=%s account_equity_source=%s",
        len(positions),
        total_pvs,
        total_pvs_source,
        total_pvs_semantics,
        account_equity_source,
    )

    logger.info(
        "[US_RECONCILE][AUTHORITATIVE] source=kis_balance positions=%d symbols=%s",
        len(positions),
        ",".join(position_symbols),
    )
    try:
        from trader.us.db.repos import save_position_snapshot
        saved = save_position_snapshot(
            positions,
            trade_date=trade_date,
            balance_fetch_status="OK",
            balance_parse_status="OK",
            authoritative_positions=True,
            preserve_previous_positions=False,
            close_source="kis_reconcile_balance",
        )
        if positions and int(saved or 0) < len(positions):
            raise RuntimeError(
                f"authoritative position persistence incomplete saved={saved} expected={len(positions)}"
            )
        logger.info(
            "[US_RECONCILE][UPSERT_POSITIONS] count=%d source=kis_balance_authoritative",
            saved,
        )
    except Exception as exc:
        logger.error("[US_RECONCILE][UPSERT_ERROR] failed to persist authoritative positions: %s", exc)
        return {
            "status": "POSITION_PERSIST_ERROR",
            "reason": "authoritative_position_persist_failed",
            "error": str(exc),
            "position_count": len(positions),
            "total_pvs": total_pvs,
            "positions": positions,
            "position_symbols": position_symbols,
            "balance_parse_status": balance_parse_status,
            "balance_fetch_status": "OK",
            "authoritative_positions": True,
            "preserve_previous_positions": True,
            "block_new_entry": True,
        }

    return {
        "status": "OK",
        "position_count": len(positions),
        # total_pvs is retained only as a legacy holdings-MV alias.
        "total_pvs": total_pvs,
        "total_pvs_source": total_pvs_source,
        "total_pvs_semantics": total_pvs_semantics,
        "holdings_market_value_usd": holdings_market_value_usd,
        "account_equity_usd": account_equity_usd,
        "account_equity_source": account_equity_source,
        "positions": positions,
        "position_symbols": position_symbols,
        "balance_source": "kis_balance_authoritative",
        "raw_output1_count": raw_output1_count,
        "normalized_position_count": normalized_position_count,
        "balance_parse_status": balance_parse_status,
        "balance_fetch_status": "OK",
        "authoritative_positions": True,
        "preserve_previous_positions": False,
        "block_new_entry": False,
    }


def _first_present(row: dict, keys: list[str], default=None):
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return default


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _build_kis_position_by_symbol(positions: list[dict]) -> dict[str, dict]:
    by_symbol: dict[str, dict] = {}
    for pos in positions or []:
        symbol = str(_first_present(pos, ["symbol", "ovrs_pdno", "pdno", "ticker", "code"], "") or "").strip().upper()
        if not symbol:
            continue
        qty = _safe_int(_first_present(pos, ["qty", "quantity", "ovrs_cblc_qty", "hldg_qty", "cblc_qty", "ord_psbl_qty"], 0))
        avg_price = _safe_float(_first_present(pos, ["avg_price", "average_price", "pchs_avg_pric", "frcr_pchs_avg_pric", "avg_buy_price", "avg_cost", "entry_price"], 0.0))
        last_price = _safe_float(_first_present(pos, ["last_price", "current_price", "current_price_usd", "ovrs_now_pric", "ovrs_prpr", "current_px"], 0.0))
        by_symbol[symbol] = {
            "qty": qty,
            "orderable_qty": _safe_int(_first_present(pos, ["orderable_qty", "sellable_qty", "ord_psbl_qty", "ovrs_ord_psbl_qty"], qty)),
            "avg_price": avg_price,
            "last_price": last_price,
            "raw": pos,
        }
    return by_symbol


def _order_meta(order: dict) -> dict:
    import json as _json

    meta = order.get("meta") or {}
    if isinstance(meta, dict):
        return meta
    if isinstance(meta, str) and meta.strip():
        try:
            parsed = _json.loads(meta)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _nested_get(row: dict, dotted_key: str):
    cur = row
    for part in dotted_key.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _extract_pre_order_position_qty(order: dict) -> tuple[int | None, str]:
    meta = _order_meta(order)
    candidates = [
        (order, "pre_order_holding_qty", "order_pre_order_holding_qty"),
        (meta, "pre_order_holding_qty", "meta_pre_order_holding_qty"),
        (order, "pre_order_position_qty", "order_pre_order_position_qty"),
        (meta, "pre_order_position_qty", "meta_pre_order_position_qty"),
        (meta, "pre_order_position_snapshot.qty", "meta_pre_order_position_snapshot_qty"),
        (order, "pre_sell_qty", "order_pre_sell_qty"),
        (meta, "pre_sell_qty", "meta_pre_sell_qty"),
        (meta, "pre_sell_position_snapshot.qty", "meta_pre_sell_position_snapshot_qty"),
        (order, "position_snapshot_qty", "order_position_snapshot_qty"),
        (meta, "position_snapshot_qty", "meta_position_snapshot_qty"),
        (order, "holding_qty", "order_holding_qty"),
        (meta, "holding_qty", "meta_holding_qty"),
        (order, "pre_order_qty", "order_pre_order_qty"),
        (order, "pre_buy_position_qty", "order_pre_buy_position_qty"),
        (order, "position_qty_before_order", "order_position_qty_before_order"),
        (order, "position_qty_before", "order_position_qty_before"),
        (order, "existing_position_qty", "order_existing_position_qty"),
        (meta, "pre_order_qty", "meta_pre_order_qty"),
        (meta, "pre_buy_position_qty", "meta_pre_buy_position_qty"),
        (meta, "position_qty_before_order", "meta_position_qty_before_order"),
        (meta, "position_qty_before", "meta_position_qty_before"),
        (meta, "existing_position_qty", "meta_existing_position_qty"),
        (order, "orderable_qty", "order_orderable_qty"),
        (meta, "orderable_qty", "meta_orderable_qty"),
    ]
    for row, key, source in candidates:
        value = _nested_get(row, key) if "." in key else row.get(key)
        if value in (None, ""):
            continue
        try:
            return int(float(value)), source
        except (TypeError, ValueError):
            continue
    if meta.get("was_new_position_before_order") is True or meta.get("pre_order_was_new_position") is True:
        return 0, "meta_was_new_position"
    return None, "missing"


def _buy_balance_reconcile_allowed(order: dict, *, current_qty: int, order_qty: int) -> tuple[bool, str, int | None]:
    pre_qty, source = _extract_pre_order_position_qty(order)
    if order_qty <= 0:
        return False, "invalid_order_qty", pre_qty
    if pre_qty is not None:
        delta = current_qty - pre_qty
        if delta >= order_qty:
            return True, f"position_delta source={source} pre_qty={pre_qty} current_qty={current_qty} delta={delta}", pre_qty
        return False, f"position_delta_insufficient source={source} pre_qty={pre_qty} current_qty={current_qty} delta={delta}", pre_qty
    return False, "missing_pre_order_qty_snapshot", None


def _accepts_keyword(func: Any, keyword: str) -> bool:
    try:
        params = inspect.signature(func).parameters.values()
    except (TypeError, ValueError):
        return True
    return any(
        param.name == keyword or param.kind == inspect.Parameter.VAR_KEYWORD
        for param in params
    )


def _get_balance_force_refresh(provider: Any) -> dict:
    method = provider.get_balance
    if _accepts_keyword(method, "force_refresh"):
        return method(force_refresh=True)
    logger.warning("[US_RECONCILE][BALANCE_FORCE_REFRESH_UNSUPPORTED] provider=%s", type(provider).__name__)
    return method()


def confirm_order_by_balance_delta(side: str, order_qty: int, pre_qty: int | None, post_qty: int | None) -> dict:
    """Classify an ACK order from explicit pre/post balance quantities without fake fills."""
    if pre_qty is None or post_qty is None or order_qty <= 0:
        return {"status": "RECONCILE_NEEDS_RECHECK", "pending": True, "filled_qty_by_balance": 0, "remaining_qty": order_qty}
    delta = int(post_qty) - int(pre_qty)
    side_u = str(side or "").upper()
    filled = -delta if side_u == "SELL" else delta if side_u == "BUY" else 0
    if filled == order_qty:
        return {"status": f"BALANCE_CONFIRMED_{side_u}", "pending": False, "filled_qty_by_balance": filled, "remaining_qty": 0}
    if 0 < filled < order_qty:
        return {"status": "BALANCE_CONFIRMED_PARTIAL", "pending": True, "filled_qty_by_balance": filled, "remaining_qty": order_qty - filled}
    return {"status": "RECONCILE_NEEDS_RECHECK", "pending": True, "filled_qty_by_balance": max(0, filled), "remaining_qty": order_qty}


def validate_reconcile_identity(*, trade_date: str, order_no: str = "", client_order_key: str = "",
                                requested_symbol: str, requested_side: str, matches: list[dict]) -> dict:
    """Strict lookup result gate; callers must not create fills on non-OK."""
    if not str(trade_date or "").strip(): return {"status": "RECONCILE_TRADE_DATE_REQUIRED"}
    if not str(order_no or "").strip() and not str(client_order_key or "").strip(): return {"status": "RECONCILE_ORDER_IDENTITY_REQUIRED"}
    if not matches: return {"status": "RECONCILE_ORDER_NOT_FOUND"}
    if len(matches) != 1: return {"status": "RECONCILE_ORDER_AMBIGUOUS"}
    row = matches[0]
    if str(row.get("symbol") or "").upper() != str(requested_symbol or "").upper(): return {"status": "RECONCILE_SYMBOL_MISMATCH"}
    if str(row.get("side") or "").upper() != str(requested_side or "").upper(): return {"status": "RECONCILE_SIDE_MISMATCH"}
    return {"status": "OK", "order": row}


def _record_reconcile_failure(
    *,
    symbol: str,
    mark_result: Any,
    symbols_by_status: dict[str, list[str]],
) -> str:
    status = str(mark_result.get("status") if isinstance(mark_result, dict) else "INVALID_RECONCILE_RESULT")
    symbols_by_status["unresolved"].append(symbol)
    logger.error("[US_RECONCILE][MARK_FAILED] symbol=%s status=%s result=%r", symbol, status, mark_result)
    return status


def reconcile_ack_orders_with_balance(
    *,
    provider: Any | None = None,
    trade_date: str,
    env: str = "practice",
) -> dict:
    """ACK 상태이나 qty_filled=0인 주문의 체결 여부를 KIS fills + 잔고로 확인."""
    from trader.us.db.repos import (load_pending_ack_orders, mark_order_filled_by_reconcile,
                                    apply_broker_order_observation, _get_engine_or_none)
    from trader.us.utils.order_no import normalize_us_order_no

    settlement_shadow_engine = None
    try:
        from trader.settlement.shadow import enabled as settlement_shadow_enabled
        if settlement_shadow_enabled():
            settlement_shadow_engine = _get_engine_or_none()
    except Exception:
        logger.exception("[SETTLEMENT_SHADOW][US_ENGINE_ERROR]")

    if provider is None:
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)
    if not str(trade_date or "").strip():
        trade_date = _ny_trade_date()
        logger.warning("[US_RECONCILE][TRADE_DATE_FALLBACK] source=ny_clock trade_date=%s", trade_date)

    try:
        pending_orders = load_pending_ack_orders(trade_date=trade_date, env=env)
    except Exception as exc:
        logger.error("[US_RECONCILE][ACK_RECONCILE][ERROR] load_pending_ack_orders failed: %s", exc)
        return {"status": "ERROR", "error": str(exc)}

    if not pending_orders:
        logger.info("[US_RECONCILE][ACK_RECONCILE] no pending ACK orders for trade_date=%s", trade_date)
        return {
            "status": "OK",
            "pending_count": 0,
            "confirmed_count": 0,
            "balance_reconcile_count": 0,
            "unresolved_count": 0,
            "open_order_pending_count": 0,
            "cancelled_count": 0,
            "rejected_count": 0,
            "expired_count": 0,
            "unresolved_error_count": 0,
            "failed_count": 0,
            "confirmed_orders": [],
            "symbols_by_status": {"confirmed": [], "balance_confirmed": [], "open_order_pending": [], "cancelled": [], "rejected": [], "expired": [], "unresolved_error": []},
            "order_nos_by_status": {"open_order_pending": [], "unresolved_error": []},
        }

    logger.info(
        "[US_RECONCILE][ACK_RECONCILE][START] pending_count=%d trade_date=%s",
        len(pending_orders), trade_date,
    )

    kis_position_by_symbol: dict[str, dict] = {}
    try:
        balance = _get_balance_force_refresh(provider)
        kis_position_by_symbol = _build_kis_position_by_symbol(balance.get("positions", []))
    except Exception as exc:
        logger.warning("[US_RECONCILE][ACK_RECONCILE][WARN] balance fetch failed: %s", exc)

    confirmed_count = 0
    balance_reconcile_count = 0
    unresolved_count = 0
    open_order_pending_count = 0
    expired_count = 0
    failed_count = 0
    canceled_count = 0
    rejected_count = 0
    confirmed_orders: list[dict] = []
    symbols_by_status: dict[str, list[str]] = {
        "confirmed": [], "fill_api_confirmed": [], "balance_confirmed": [],
        "open_order_pending": [], "cancelled": [], "rejected": [], "expired": [],
        "unresolved_error": [], "unresolved": [],
    }
    order_nos_by_status: dict[str, list[str]] = {"open_order_pending": [], "unresolved_error": []}

    for order in pending_orders:
        symbol = str(order.get("symbol", "")).strip().upper()
        side = str(order.get("side", "")).upper()
        order_no = str(order.get("order_no") or order.get("ack_no") or "")
        client_order_key = str(order.get("client_order_key") or "")
        identity = validate_reconcile_identity(
            trade_date=trade_date, order_no=order_no, client_order_key=client_order_key,
            requested_symbol=symbol, requested_side=side, matches=[order],
        )
        if identity.get("status") != "OK":
            unresolved_count += 1
            symbols_by_status["unresolved"].append(symbol)
            logger.error("[US_RECONCILE][STRICT_IDENTITY] status=%s symbol=%s", identity.get("status"), symbol)
            continue

        qty = int(
            order.get("qty_requested")
            or order.get("qty")
            or order.get("filled_qty")
            or order.get("qty_filled")
            or 0
        )
        if qty <= 0:
            logger.warning("[US_RECONCILE][ACK_RECONCILE][INVALID_QTY] symbol=%s order_no=%s side=%s qty=%d reason=missing_qty_requested", symbol, order_no, side, qty)
        fallback_fill_price, fallback_price_source = _resolve_order_fill_price(order)
        pre_qty_for_delta, _pre_source = _extract_pre_order_position_qty(order)
        post_qty_for_delta = int((kis_position_by_symbol.get(symbol) or {}).get("qty") or 0)
        delta_confirmation = confirm_order_by_balance_delta(side, qty, pre_qty_for_delta, post_qty_for_delta)

        fill_confirmed = False
        fill_price = fallback_fill_price
        fill_price_source = fallback_price_source or "unavailable"
        fill_qty = qty

        fills_resp: dict = {}
        try:
            fills_resp = provider.get_fills_by_order_no(order_no=order_no, symbol=symbol, trade_date=trade_date)
            if fills_resp and isinstance(fills_resp, dict):
                terminal_status = str(fills_resp.get("status") or "").upper()
                if terminal_status in {"EXPIRED", "LAPSED", "DAY_ORDER_LAPSED", "만료"}:
                    expired_count += 1
                    symbols_by_status["expired"].append(symbol)
                    logger.info("[US_RECONCILE][ACK_EXPIRED] symbol=%s order_no=%s", symbol, order_no)
                    continue
                terminal_status = str(fills_resp.get("status") or "").strip().upper()
                is_rejection = terminal_status in _BROKER_REJECT_STATUSES
                if terminal_status in _BROKER_CANCEL_STATUSES or is_rejection:
                    terminal_status = str(fills_resp.get("status") or "CANCELLED").upper()
                    broker_status = "REJECTED" if is_rejection else "CANCELLED"
                    terminal_quantities_are_explicit = (
                        is_terminal_zero_fill_rejection(fills_resp)
                        if is_rejection else is_terminal_cancel_observation(fills_resp)
                    )
                    cumulative_filled_qty = None
                    if fills_resp.get("filled_qty_present") is not False:
                        cumulative_filled_qty = next(
                            (
                                fills_resp[name]
                                for name in (
                                    "filled_qty", "cumulative_filled_qty", "tot_ccld_qty",
                                    "ft_ccld_qty", "ccld_qty",
                                )
                                if fills_resp.get(name) not in (None, "")
                            ),
                            None,
                        )
                    broker_open_qty = None
                    if (
                        fills_resp.get("remaining_qty_present") is not False
                        and fills_resp.get("broker_open_qty_present") is not False
                    ):
                        broker_open_qty = next(
                            (
                                fills_resp[name]
                                for name in (
                                    "broker_open_qty", "remaining_qty", "rmn_qty",
                                    "ord_remn_qty", "nccs_qty",
                                )
                                if fills_resp.get(name) not in (None, "")
                            ),
                            None,
                        )
                    try:
                        semantic_remaining_qty = (
                            max(0, qty - int(cumulative_filled_qty))
                            if cumulative_filled_qty is not None else None
                        )
                    except (TypeError, ValueError):
                        semantic_remaining_qty = None
                    mark_result = apply_broker_order_observation(
                        trade_date=trade_date, client_order_key=client_order_key,
                        raw_order_no=order_no, canonical_order_no=normalize_us_order_no(order_no),
                        symbol=symbol, side=side, requested_qty=qty,
                        filled_qty=cumulative_filled_qty,
                        remaining_qty=semantic_remaining_qty,
                        broker_open_qty=broker_open_qty, broker_status=broker_status,
                        evidence_type="KIS_TERMINAL_CANCEL", observed_at=fills_resp.get("observed_at"),
                        raw_row=fills_resp,
                    )
                    if (
                        mark_result.get("status") == "OK"
                        and not mark_result.get("requires_reconcile")
                        and mark_result.get("authoritative") is True
                    ):
                        if is_rejection:
                            rejected_count += 1
                            symbols_by_status["rejected"].append(symbol)
                        else:
                            canceled_count += 1
                            symbols_by_status["cancelled"].append(symbol)
                        logger.info("[US_RECONCILE][TERMINAL_NO_FILL] symbol=%s order_no=%s status=%s",
                                    symbol, order_no, broker_status)
                        continue
                    unresolved_count += 1
                    symbols_by_status["unresolved"].append(symbol)
                    if mark_result.get("status") != "OK":
                        failed_count += 1
                        symbols_by_status["unresolved_error"].append(symbol)
                    logger.error(
                        "[US_RECONCILE][TERMINAL_NOT_AUTHORITATIVE] symbol=%s order_no=%s status=%s explicit_quantities=%s result=%r",
                        symbol, order_no, broker_status, terminal_quantities_are_explicit, mark_result,
                    )
                    continue
                fill_contract_status = str(
                    fills_resp.get("evidence_status") or fills_resp.get("status") or "OK"
                ).upper()
                if fill_contract_status in _FILL_CONTRACT_ERROR_STATUSES:
                    logger.error(
                        "[US_RECONCILE][ACK_RECONCILE][FILL_CONTRACT_ERROR] symbol=%s status=%s",
                        symbol, fill_contract_status,
                    )
                    failed_count += 1
                    unresolved_count += 1
                    symbols_by_status["unresolved"].append(symbol)
                    continue
                if fills_resp.get("filled_qty", 0) > 0:
                    fill_symbol = str(fills_resp.get("symbol") or symbol).upper()
                    fill_side = str(fills_resp.get("side") or side).upper()
                    fill_order_no = str(fills_resp.get("order_no") or order_no)
                    if fill_symbol != symbol or fill_side != side or normalize_us_order_no(fill_order_no) != normalize_us_order_no(order_no):
                        logger.error("[US_RECONCILE][IDENTITY_MISMATCH] order_no=%s symbol=%s/%s side=%s/%s", order_no, symbol, fill_symbol, side, fill_side)
                        failed_count += 1
                        unresolved_count += 1
                        symbols_by_status["unresolved"].append(symbol)
                        continue
                    fill_price = float(fills_resp.get("avg_price", 0) or 0)
                    fill_price_source = "fills_by_order_no"
                    fill_qty = int(fills_resp.get("filled_qty", qty))
                    fill_confirmed = True
        except AttributeError:
            pass
        except TypeError as exc:
            logger.error("[US_RECONCILE][ACK_RECONCILE][CONTRACT_ERROR] fills_by_order_no signature error symbol=%s: %s", symbol, exc)
            failed_count += 1
            unresolved_count += 1
            symbols_by_status["unresolved"].append(symbol)
            continue
        except Exception as exc:
            logger.warning(
                "[US_RECONCILE][ACK_RECONCILE][WARN] fills_by_order_no failed symbol=%s: %s",
                symbol, exc,
            )

        if fill_confirmed:
            # Phase 3: read-only cross-market settlement projection.
            try:
                from trader.settlement.shadow import shadow_us_reconcile
                shadow_us_reconcile(
                    order=order, trade_date=trade_date,
                    cumulative_qty=fill_qty,
                    broker_fill_price=fill_price if fill_price and fill_price > 0 else None,
                    evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
                    engine=settlement_shadow_engine,
                )
            except Exception:
                logger.exception("[SETTLEMENT_SHADOW][US_ACTUAL_PROBE_ERROR] symbol=%s", symbol)
            logger.info(
                "[US_RECONCILE][FILL_CONFIRMED] symbol=%s order_no=%s qty=%d price=%.4f",
                symbol, order_no, fill_qty, fill_price,
            )
            try:
                mark_result = apply_broker_order_observation(
                    trade_date=trade_date, client_order_key=client_order_key,
                    raw_order_no=order_no, canonical_order_no=normalize_us_order_no(order_no),
                    symbol=symbol, side=side, requested_qty=qty, filled_qty=fill_qty,
                    remaining_qty=max(0, qty-fill_qty),
                    broker_status="FILLED" if fill_qty >= qty else "PARTIALLY_FILLED",
                    evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
                    observed_at=fills_resp.get("observed_at"),
                    raw_row={**fills_resp, "avg_price": fill_price},
                )
                if mark_result.get("status") in {"ORDER_IDENTITY_NOT_UNIQUE", "ORDER_NOT_FOUND"}:
                    # Compatibility for injected/provider-only reconciliation;
                    # production pending orders always have a durable DB identity.
                    mark_result = mark_order_filled_by_reconcile(
                        order_no=order_no, client_order_key=client_order_key,
                        symbol=symbol, side=side, filled_qty=fill_qty,
                        requested_qty=qty, cumulative_filled_qty=fill_qty,
                        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL", avg_price_usd=fill_price,
                        source="fills_reconcile", trade_date=trade_date, meta=_order_meta(order))
                if isinstance(mark_result, dict) and mark_result.get("status") == "OK":
                    confirmed_count += 1
                    symbols_by_status["fill_api_confirmed"].append(symbol)
                    symbols_by_status["confirmed"].append(symbol)
                    confirmed_orders.append({
                        **order,
                        "symbol": symbol, "side": side, "order_no": order_no,
                        "client_order_key": client_order_key,
                        "filled_qty": fill_qty, "cumulative_filled_qty": fill_qty,
                        "avg_price_usd": fill_price, "reconcile_source": "fills_reconcile",
                        "meta": _order_meta(order),
                    })
                else:
                    _record_reconcile_failure(symbol=symbol, mark_result=mark_result, symbols_by_status=symbols_by_status)
                    failed_count += 1
                    unresolved_count += 1
            except Exception as exc:
                logger.error(
                    "[US_RECONCILE][ACK_RECONCILE][ERROR] mark_order_filled failed symbol=%s: %s",
                    symbol, exc,
                )
                failed_count += 1
                unresolved_count += 1
                symbols_by_status["unresolved"].append(symbol)
            continue

        if delta_confirmation["status"] in {"BALANCE_CONFIRMED_BUY", "BALANCE_CONFIRMED_SELL", "BALANCE_CONFIRMED_PARTIAL"}:
            filled_by_balance = int(delta_confirmation.get("filled_qty_by_balance") or 0)
            try:
                from trader.settlement.shadow import shadow_us_reconcile
                shadow_us_reconcile(
                    order=order, trade_date=trade_date,
                    cumulative_qty=filled_by_balance,
                    broker_fill_price=None,
                    evidence_type="BALANCE_DELTA_SYNTHETIC",
                    pre_holding_qty=pre_qty_for_delta,
                    post_holding_qty=post_qty_for_delta,
                    exclusive_order_proof=False,
                    engine=settlement_shadow_engine,
                )
            except Exception:
                logger.exception("[SETTLEMENT_SHADOW][US_BALANCE_PROBE_ERROR] symbol=%s", symbol)
            source_name = "balance_reconcile_partial" if delta_confirmation["status"] == "BALANCE_CONFIRMED_PARTIAL" else f"balance_reconcile_{side.lower()}"
            logger.info(
                "[US_RECONCILE][BALANCE_DELTA_CONFIRMED] symbol=%s side=%s order_qty=%d filled_qty=%d pre_qty=%s post_qty=%s status=%s",
                symbol, side, qty, filled_by_balance, pre_qty_for_delta, post_qty_for_delta, delta_confirmation["status"],
            )
            try:
                mark_result = mark_order_filled_by_reconcile(
                    order_no=order_no,
                    client_order_key=client_order_key,
                    symbol=symbol,
                    side=side,
                    filled_qty=filled_by_balance,
                    requested_qty=qty,
                    cumulative_filled_qty=filled_by_balance,
                    evidence_type="BALANCE_DELTA_SYNTHETIC",
                    avg_price_usd=fallback_fill_price,
                    source=source_name,
                    trade_date=trade_date,
                    meta={**_order_meta(order), "balance_delta_status": delta_confirmation["status"], "remaining_qty": delta_confirmation.get("remaining_qty", 0)},
                )
                if isinstance(mark_result, dict) and mark_result.get("status") == "OK":
                    balance_reconcile_count += 1
                    symbols_by_status["balance_confirmed"].append(symbol)
                    confirmed_orders.append({
                        **order,
                        "symbol": symbol, "side": side, "order_no": order_no,
                        "client_order_key": client_order_key,
                        "filled_qty": filled_by_balance, "cumulative_filled_qty": filled_by_balance,
                        "avg_price_usd": fallback_fill_price,
                        "reconcile_source": source_name,
                        "meta": _order_meta(order),
                    })
                else:
                    _record_reconcile_failure(symbol=symbol, mark_result=mark_result, symbols_by_status=symbols_by_status)
                    failed_count += 1
                    unresolved_count += 1
            except Exception as exc:
                logger.error("[US_RECONCILE][ACK_RECONCILE][ERROR] balance_delta_confirm failed symbol=%s: %s", symbol, exc)
                failed_count += 1
                unresolved_count += 1
                symbols_by_status["unresolved"].append(symbol)
            continue

        if side == "BUY" and symbol in kis_position_by_symbol:
            position = kis_position_by_symbol[symbol]
            position_qty = int(position.get("qty") or 0)
            position_avg_price = float(position.get("avg_price") or 0.0)
            fill_price_candidate = position_avg_price if position_avg_price > 0 else fallback_fill_price
            price_source = "kis_balance_avg_price" if position_avg_price > 0 else fill_price_source
            buy_allowed, buy_reason, pre_qty = _buy_balance_reconcile_allowed(
                order,
                current_qty=position_qty,
                order_qty=qty,
            )
            if buy_allowed and fill_price_candidate > 0:
                logger.info(
                    "[US_RECONCILE][BALANCE_RECONCILE_FILL] symbol=%s side=BUY qty=%d price_source=%s price=%.4f source=balance_reconcile_buy reason=%s",
                    symbol,
                    qty,
                    price_source,
                    fill_price_candidate,
                    buy_reason,
                )
                try:
                    mark_result = mark_order_filled_by_reconcile(
                        order_no=order_no,
                        client_order_key=client_order_key,
                        symbol=symbol,
                        side="BUY",
                        filled_qty=qty,
                        requested_qty=qty,
                        cumulative_filled_qty=qty,
                        evidence_type="BALANCE_DELTA_SYNTHETIC",
                        avg_price_usd=fill_price_candidate,
                        source="balance_reconcile_buy",
                        trade_date=trade_date,
                        meta=_order_meta(order),
                    )
                    if isinstance(mark_result, dict) and mark_result.get("status") == "OK":
                        balance_reconcile_count += 1
                        symbols_by_status["balance_confirmed"].append(symbol)
                        confirmed_orders.append({
                            **order,
                            "symbol": symbol, "side": "BUY", "order_no": order_no,
                            "client_order_key": client_order_key,
                            "filled_qty": qty, "cumulative_filled_qty": qty,
                            "avg_price_usd": fill_price_candidate,
                            "reconcile_source": "balance_reconcile_buy",
                            "meta": _order_meta(order),
                        })
                    else:
                        _record_reconcile_failure(symbol=symbol, mark_result=mark_result, symbols_by_status=symbols_by_status)
                        failed_count += 1
                        unresolved_count += 1
                except Exception as exc:
                    logger.error(
                        "[US_RECONCILE][ACK_RECONCILE][ERROR] balance_reconcile_buy failed symbol=%s: %s",
                        symbol, exc,
                    )
                    failed_count += 1
                    unresolved_count += 1
                    symbols_by_status["unresolved"].append(symbol)
                continue
            logger.warning(
                "[US_RECONCILE][BUY_BALANCE_RECONCILE][SKIP] symbol=%s order_no=%s qty=%d current_qty=%d pre_qty=%s reason=%s",
                symbol,
                order_no,
                qty,
                position_qty,
                pre_qty,
                buy_reason if fill_price_candidate > 0 else "missing_fill_price",
            )

        position = kis_position_by_symbol.get(symbol) or {}
        current_qty = int(position.get("qty") or 0)
        current_orderable = int(position.get("orderable_qty") if position.get("orderable_qty") is not None else current_qty)
        remaining_qty = int((fills_resp or {}).get("remaining_qty") or order.get("remaining_qty") or qty or 0)
        broker_status = str((fills_resp or {}).get("status") or order.get("broker_status") or "").upper()
        explicit_open = bool(
            (fills_resp or {}).get("open_order") or (fills_resp or {}).get("is_open")
            or order.get("open_order") or order.get("open_order_pending")
            or broker_status in {"OPEN", "PENDING", "ACK_OPEN", "WORKING", "PARTIALLY_FILLED"}
        )
        reservation = bool(
            side == "SELL" and pre_qty_for_delta is not None and current_qty == pre_qty_for_delta
            and qty > 0 and current_orderable <= max(0, pre_qty_for_delta - qty)
        )
        if remaining_qty > 0 and (explicit_open or reservation):
            open_order_pending_count += 1
            symbols_by_status["open_order_pending"].append(symbol)
            order_nos_by_status["open_order_pending"].append(order_no)
            logger.info(
                "[US_RECONCILE][ACK_OPEN_ORDER_PENDING] symbol=%s order_no=%s requested_qty=%s "
                "filled_qty=0 remaining_qty=%s orderable_delta=%s action=mark_open_order_pending",
                symbol, order_no, qty, remaining_qty,
                max(0, int(pre_qty_for_delta or 0) - current_orderable),
            )
            continue
        logger.error(
            "[US_RECONCILE][ACK_UNRESOLVED_ERROR] symbol=%s order_no=%s requested_qty=%s "
            "filled_qty=0 remaining_qty=%s reason=no_fill_no_open_order_no_balance_delta manual_reconcile_required=1",
            symbol, order_no, qty, remaining_qty or "unknown",
        )
        unresolved_count += 1
        symbols_by_status["unresolved"].append(symbol)
        symbols_by_status["unresolved_error"].append(symbol)
        order_nos_by_status["unresolved_error"].append(order_no)

    # Compatibility `unresolved` includes every true error path; mirror it to
    # the explicit contract even when failure occurred before broker lookup.
    symbols_by_status["unresolved_error"] = list(dict.fromkeys(
        [*symbols_by_status["unresolved_error"], *symbols_by_status["unresolved"]]
    ))
    logger.info(
        "[US_RECONCILE][ACK_RECONCILE][DONE] pending=%d confirmed=%d balance_reconcile=%d open_order_pending=%d unresolved_error=%d failed=%d",
        len(pending_orders), confirmed_count, balance_reconcile_count, open_order_pending_count, unresolved_count, failed_count,
    )

    return {
        "status": "ERROR" if failed_count > 0 else ("WARN" if unresolved_count > 0 or open_order_pending_count > 0 else "OK"),
        "pending_count": len(pending_orders),
        "confirmed_count": confirmed_count,
        "balance_reconcile_count": balance_reconcile_count,
        "unresolved_count": unresolved_count,
        "open_order_pending_count": open_order_pending_count,
        "cancelled_count": canceled_count,
        "rejected_count": rejected_count,
        "expired_count": expired_count,
        "unresolved_error_count": unresolved_count,
        "failed_count": failed_count,
        "canceled_count": canceled_count,
        "confirmed_orders": confirmed_orders,
        "symbols_by_status": symbols_by_status,
        "order_nos_by_status": order_nos_by_status,
        "manual_reconcile_required": int(unresolved_count > 0),
    }


def _load_close_order_attempt_truth(*, provider: Any, trade_date: str, env: str) -> list[dict]:
    """Union active DB, durable journal/claims, and KIS rows by individual attempt.

    Do not collapse two different broker orders merely because their semantic
    strategy action is the same.  Failure of any truth source is NOT an empty
    position/order book.
    """
    from trader.us.db.repos import (
        load_pending_ack_orders_result, load_active_execution_claim_attempts,
    )
    from trader.us.execution.order_journal import load_order_events
    from trader.us.utils.order_no import normalize_us_order_no

    pending = load_pending_ack_orders_result(trade_date, env=env)
    if pending.get("status") != "OK":
        raise RuntimeError("pending_attempts_db_unavailable: " + str(pending.get("error") or "unknown"))
    attempts: list[dict] = []

    def _order_no(row):
        return str(row.get("order_no") or row.get("broker_order_no") or
                   row.get("raw_broker_order_no") or row.get("raw_order_no") or
                   row.get("canonical_order_no") or "")

    def _add(row: dict, source: str):
        if not isinstance(row, dict):
            return
        symbol = str(row.get("symbol") or "").upper().strip()
        side = str(row.get("side") or "").upper().strip()
        if not symbol or side not in {"BUY", "SELL"}:
            return
        key = str(row.get("client_order_key") or "")
        attempt_id = str(row.get("submit_attempt_id") or row.get("active_attempt_id") or "")
        order_no = _order_no(row)
        canonical = normalize_us_order_no(order_no) if order_no else ""
        found = None
        for existing in attempts:
            if existing["symbol"] != symbol or existing["side"] != side:
                continue
            prior_no = normalize_us_order_no(existing.get("order_no")) if existing.get("order_no") else ""
            prior_attempt = str(existing.get("submit_attempt_id") or "")
            if canonical and prior_no and canonical == prior_no:
                # Colliding order numbers with different durable attempts must
                # remain separate rather than silently combining their fills.
                if attempt_id and prior_attempt and attempt_id != prior_attempt:
                    continue
                found = existing
                break
            if attempt_id and prior_attempt and attempt_id == prior_attempt:
                found = existing
                break
            if key and key == existing.get("client_order_key") and not (
                (canonical and prior_no and canonical != prior_no)
                or (attempt_id and prior_attempt and attempt_id != prior_attempt)
            ):
                found = existing
                break
        if found is None:
            found = {
                "symbol": symbol, "side": side, "order_no": order_no,
                "client_order_key": key, "submit_attempt_id": attempt_id,
                "_sources": [],
            }
            attempts.append(found)
        if source not in found["_sources"]:
            found["_sources"].append(source)
        if not found.get("client_order_key") and key:
            found["client_order_key"] = key
        if not found.get("submit_attempt_id") and attempt_id:
            found["submit_attempt_id"] = attempt_id
        if not found.get("order_no") and order_no:
            found["order_no"] = order_no
        if source == "broker":
            found["_broker_observation"] = dict(row)
            # Broker quantity is evidence; never silently overwrite local
            # metadata or identity by joining on a strategy symbol alone.
            found["broker_remaining_qty"] = row.get("remaining_qty")
            found["broker_filled_qty"] = row.get("filled_qty")
            found["broker_status"] = row.get("status")
            found["broker_filled_qty_present"] = row.get("filled_qty_present")
            found["broker_normalization_result"] = row.get("normalization_result")
            found["broker_requested_qty"] = row.get("requested_qty")
            found["open_order"] = row.get("open_order")
        else:
            for field in ("qty_requested", "qty_filled", "status", "created_at", "meta"):
                if row.get(field) not in (None, "") and found.get(field) in (None, ""):
                    found[field] = row[field]
            if source == "claim":
                found["active_claim"] = True

    for row in pending["orders"]:
        _add(row, "local")
    # The production US provider implements this and returns a normalized,
    # account-scoped complete order book for the trade date.
    if callable(getattr(provider, "get_today_orders", None)):
        events = load_order_events(trade_date)
        aborted = {
            str(event.get("submit_attempt_id") or "")
            for event in events
            if str(event.get("event_type") or "") in {
                "BROKER_SUBMIT_ABORTED_BEFORE_BOUNDARY", "BROKER_SUBMIT_ABORTED_PRE_IO",
            }
        }
        relevant_events = {
            "BROKER_SUBMIT_STARTED", "BROKER_ACK_RECEIVED", "BROKER_ACK_RECOVERED",
            "ORDER_PARTIALLY_FILLED", "ORDER_FILLED", "ORDER_CANCELLED", "ORDER_REJECTED",
            "JOURNAL_REPLAY_UNRESOLVED",
        }
        for event in events:
            event_type = str(event.get("event_type") or "")
            attempt_id = str(event.get("submit_attempt_id") or "")
            if event_type not in relevant_events or (attempt_id and attempt_id in aborted):
                continue
            _add(event, "journal")
        for claim in load_active_execution_claim_attempts():
            if str(claim.get("trade_date") or "")[:10] != str(trade_date):
                continue
            # Claims contain attempt key but not necessarily a symbol. Attach to
            # matching client attempt, or retain a sentinel for unresolved claim.
            claim_key = str(claim.get("client_order_key") or "")
            claim_attempt = str(claim.get("active_attempt_id") or "")
            matches = [
                row for row in attempts
                if ((claim_attempt and row.get("submit_attempt_id") == claim_attempt)
                    or (claim_key and row.get("client_order_key") == claim_key))
            ]
            if len(matches) == 1:
                matches[0]["active_claim"] = True
                matches[0]["_sources"].append("claim")
            else:
                attempts.append({
                    "symbol": "UNKNOWN", "side": "UNKNOWN", "status": "ACTIVE_CLAIM_UNKNOWN",
                    "client_order_key": claim_key, "submit_attempt_id": claim_attempt,
                    "_sources": ["claim"], "active_claim": True,
                })
        broker_rows = provider.get_today_orders(trade_date)
        if not isinstance(broker_rows, list):
            raise RuntimeError("broker_order_book_unavailable_or_invalid")
        for broker_row in broker_rows:
            _add(broker_row, "broker")
    return attempts


def classify_ack_orders_with_final_balance(
    *,
    provider: Any,
    trade_date: str,
    env: str = "practice",
    orders: list[dict] | None = None,
) -> dict:
    """Classify same-day ACK orders using final close balance deltas.

    Intended for close reports, after the broker's balance has had time to settle.
    """
    if orders is None:
        try:
            orders = _load_close_order_attempt_truth(
                provider=provider, trade_date=trade_date, env=env
            )
        except Exception as exc:
            return {"status": "ERROR", "error": str(exc), "orders": [],
                    "pending_order_count": 1, "manual_reconcile_required": 1}

    try:
        balance = _get_balance_force_refresh(provider)
        if not isinstance(balance, dict) or str(balance.get("balance_parse_status") or "OK").upper() != "OK":
            raise RuntimeError("close_broker_balance_not_authoritative")
        final_positions = _build_kis_position_by_symbol(balance.get("positions", []))
    except Exception as exc:
        return {"status": "ERROR", "error": str(exc), "orders": [], "pending_order_count": max(1, len(orders or [])),
                "manual_reconcile_required": 1}

    classified: list[dict] = []
    pending = 0
    open_order_pending = 0
    counts: dict[str, int] = {}
    for order in orders or []:
        symbol = str(order.get("symbol") or "").upper().strip()
        side = str(order.get("side") or "").upper()
        qty = int(order.get("qty_requested") or order.get("qty") or order.get("filled_qty") or order.get("qty_filled") or 0)
        pre_qty, _source = _extract_pre_order_position_qty(order)
        final_qty = int((final_positions.get(symbol) or {}).get("qty") or 0)
        final_orderable = int((final_positions.get(symbol) or {}).get("orderable_qty") or 0)
        raw_status = str(order.get("status") or "").upper()
        fill_qty = int(order.get("qty_filled") or order.get("filled_qty") or 0)
        broker = order.get("_broker_observation") if isinstance(order.get("_broker_observation"), dict) else {}
        broker_status = str(broker.get("status") or "").strip().upper()
        broker_present = bool(broker)
        broker_normalized = str(broker.get("normalization_result") or "").lower() != "quarantined"
        broker_fill_explicit = (
            broker.get("filled_qty_present") is not False
            and broker.get("filled_qty") not in (None, "")
        )
        broker_fill = int(broker.get("filled_qty") or 0) if broker_fill_explicit else None
        broker_remaining = (
            int(broker["remaining_qty"])
            if broker.get("remaining_qty") not in (None, "") else None
        )
        if broker.get("requested_qty") not in (None, ""):
            broker_requested = int(broker["requested_qty"])
            if qty and qty != broker_requested:
                order["broker_local_quantity_conflict"] = True
            if not qty:
                qty = broker_requested
        if broker_fill is not None:
            fill_qty = max(fill_qty, broker_fill)
        identity = validate_reconcile_identity(
            trade_date=trade_date, order_no=str(order.get("order_no") or ""),
            client_order_key=str(order.get("client_order_key") or ""),
            requested_symbol=symbol, requested_side=side, matches=[order],
        )
        if identity.get("status") != "OK":
            final_status = identity["status"]
            pending += 1
            counts[final_status] = counts.get(final_status, 0) + 1
            classified.append({"symbol": symbol, "side": side, "final_status": final_status})
            continue
        # Broker-open evidence wins over local terminal/partial status.  A
        # partial fill is NOT an order-complete event while remaining > 0.
        if order.get("broker_local_quantity_conflict") or (broker_present and not broker_normalized):
            final_status = "ack_unresolved_error"
        elif broker_present and broker_remaining is not None and broker_remaining > 0:
            final_status = "partial_fill_open" if fill_qty > 0 else "ack_open_order_pending"
        elif broker_present and broker_status in {"CANCELLED", "CANCELED", "EXPIRED", "LAPSED", "REJECTED", "REJECT"}:
            if not broker_fill_explicit:
                final_status = "ack_unresolved_error"
            elif broker_fill > 0:
                final_status = "partial_fill_cancelled" if broker_status in {"CANCELLED", "CANCELED"} else "partial_fill_terminal"
            else:
                final_status = "rejected" if broker_status in {"REJECTED", "REJECT"} else "cancelled_or_expired"
        elif broker_present and broker_fill_explicit and qty > 0 and broker_fill >= qty and broker_remaining == 0:
            final_status = "broker_fill_confirmed"
        elif broker_present and broker_remaining == 0 and fill_qty > 0 and fill_qty < qty:
            # Remaining zero but only a partial quantity: terminal proof is
            # missing, so keep reconciliation pending rather than inventing it.
            final_status = "ack_unresolved_error"
        elif broker_present and broker_remaining is None and broker_fill_explicit and 0 < broker_fill < qty:
            final_status = "ack_unresolved_error"
        elif broker_present and order.get("_sources") == ["broker"]:
            final_status = "ack_unresolved_error"  # broker-only identity is unattributed
        elif raw_status in {"REJECT", "REJECTED"}:
            final_status = "rejected"
        elif raw_status in {"BLOCKED", "WARN_DUPLICATE_EXIT_BLOCKED"}:
            final_status = "DUPLICATE_OR_ALREADY_CLOSED" if raw_status == "WARN_DUPLICATE_EXIT_BLOCKED" else "BLOCKED"
        elif qty > 0 and fill_qty >= qty and (not broker_present or broker_fill_explicit):
            final_status = "broker_fill_confirmed"
        elif fill_qty > 0 or raw_status in {"PARTIALLY_FILLED"}:
            final_status = "ack_open_order_pending" if broker_remaining is None else "ack_unresolved_error"
        elif order.get("active_claim") and not broker_present:
            final_status = "ack_unresolved_error"
        elif side == "BUY" and pre_qty is not None and qty > 0 and final_qty - pre_qty >= qty:
            final_status = "balance_delta_confirmed"
        elif side == "SELL" and pre_qty is not None and qty > 0 and pre_qty - final_qty == qty:
            final_status = "balance_delta_confirmed"
        elif (
            side == "SELL" and qty > 0 and final_qty == 0
            and (
                (pre_qty is not None and pre_qty > 0)
                or raw_status in {"ACK", "ACKED", "ACCEPTED"}
            )
        ):
            # A final authoritative balance with no symbol is sufficient SELL
            # evidence even when the pre-order snapshot was unavailable.
            final_status = "position_absent_confirmed_sell"
        elif (
            qty > 0
            and (
                bool(order.get("open_order") or order.get("open_order_pending"))
                or raw_status in {"OPEN", "PENDING", "WORKING", "ACK_OPEN"}
                or (side == "SELL" and pre_qty is not None and final_qty == pre_qty
                    and final_orderable <= max(0, pre_qty - qty))
            )
        ):
            final_status = "ack_open_order_pending"
        else:
            final_status = "ack_unresolved_error"
        if final_status == "ack_unresolved_error":
            pending += 1
        elif final_status in {"ack_open_order_pending", "partial_fill_open"}:
            open_order_pending += 1
            logger.warning(
                "[US_CLOSE][ORDER_RECONCILE][OPEN_ORDER_PENDING] symbol=%s order_no=%s requested_qty=%s "
                "filled_qty=%s remaining_qty=%s action=warn_not_fail",
                symbol, order.get("order_no") or order.get("ack_no") or "",
                qty, fill_qty, broker_remaining if broker_remaining is not None else max(0, qty - fill_qty),
            )
        counts[final_status] = counts.get(final_status, 0) + 1
        classified.append({
            "time": str(order.get("created_at") or order.get("time") or ""),
            "side": side,
            "symbol": symbol,
            "qty": qty,
            "order_no": str(order.get("order_no") or order.get("ack_no") or ""),
            "client_order_key": str(order.get("client_order_key") or ""),
            "ack_status": raw_status,
            "broker_status": broker_status,
            "submit_attempt_id": str(order.get("submit_attempt_id") or ""),
            "broker_filled_qty": broker_fill,
            "broker_remaining_qty": broker_remaining,
            "evidence_sources": list(order.get("_sources") or []),
            "fill_api_status": "broker_fill_confirmed" if final_status == "broker_fill_confirmed" else "NOT_CONFIRMED_BY_FILL_API",
            "balance_delta_status": ("balance_delta_confirmed_" + side.lower()) if final_status == "balance_delta_confirmed" else ("position_absent_confirmed_sell" if final_status == "position_absent_confirmed_sell" else "NOT_CONFIRMED_BY_BALANCE_DELTA"),
            "final_status": final_status,
            "order_final_classification": "BALANCE_DELTA_CONFIRMED" if final_status == "balance_delta_confirmed" else final_status.upper(),
            "balance_delta_confirmed": final_status == "balance_delta_confirmed",
            "price_source": str((order.get("meta") or {}).get("price_source") or order.get("price_source") or ""),
            "pnl_if_sell": (order.get("meta") or {}).get("pnl_if_sell") if isinstance(order.get("meta") or {}, dict) else None,
            "pre_order_position_qty": pre_qty,
            "pre_order_holding_qty": pre_qty,
            "requested_sell_qty": qty if side == "SELL" else None,
            "expected_post_order_qty": (pre_qty - qty) if side == "SELL" and pre_qty is not None else None,
            "final_position_qty": final_qty,
        })
    return {
        "status": "DEGRADED_ACK_UNRESOLVED" if pending > 0 else ("WARNING_OPEN_ORDER_PENDING" if open_order_pending > 0 else "OK"),
        "orders": classified,
        "counts": counts,
        "pending_order_count": pending,
        "open_order_pending_count": open_order_pending,
        "unresolved_error_count": pending,
        "manual_reconcile_required": int(pending > 0),
        "reason": "ACK_UNRESOLVED_ERROR" if pending > 0 else ("OPEN_ORDER_PENDING_AT_CLOSE" if open_order_pending > 0 else ""),
    }
