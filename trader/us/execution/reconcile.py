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
    raw_output1_count = balance.get("raw_output1_count", 0)
    normalized_position_count = balance.get("normalized_position_count", 0)
    position_symbols = balance.get("position_symbols", [])
    balance_parse_status = balance.get("balance_parse_status", "UNKNOWN")
    balance_parse_error = balance.get("balance_parse_error")

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
        "[US_RECONCILE][OK] position_count=%d total_pvs=%s total_pvs_source=%s",
        len(positions),
        total_pvs,
        total_pvs_source,
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
        "total_pvs": total_pvs,
        "total_pvs_source": total_pvs_source,
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
    from trader.us.db.repos import load_pending_ack_orders, mark_order_filled_by_reconcile

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
            "failed_count": 0,
            "symbols_by_status": {},
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
    failed_count = 0
    symbols_by_status: dict[str, list[str]] = {"fill_api_confirmed": [], "balance_confirmed": [], "unresolved": []}

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

        try:
            fills_resp = provider.get_fills_by_order_no(order_no=order_no, symbol=symbol, trade_date=trade_date)
            if fills_resp and isinstance(fills_resp, dict):
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
                    from trader.us.utils.order_no import normalize_us_order_no
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
            logger.info(
                "[US_RECONCILE][FILL_CONFIRMED] symbol=%s order_no=%s qty=%d price=%.4f",
                symbol, order_no, fill_qty, fill_price,
            )
            try:
                mark_result = mark_order_filled_by_reconcile(
                    order_no=order_no,
                    client_order_key=client_order_key,
                    symbol=symbol,
                    side=side,
                    filled_qty=fill_qty,
                    requested_qty=qty,
                    cumulative_filled_qty=fill_qty,
                    evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
                    avg_price_usd=fill_price,
                    source="fills_reconcile",
                    trade_date=trade_date,
                    meta=_order_meta(order),
                )
                if isinstance(mark_result, dict) and mark_result.get("status") == "OK":
                    confirmed_count += 1
                    symbols_by_status["fill_api_confirmed"].append(symbol)
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

        logger.warning(
            "[US_RECONCILE][ACK_RECONCILE][UNRESOLVED] symbol=%s order_no=%s side=%s",
            symbol, order_no, side,
        )
        unresolved_count += 1
        symbols_by_status["unresolved"].append(symbol)

    logger.info(
        "[US_RECONCILE][ACK_RECONCILE][DONE] pending=%d confirmed=%d balance_reconcile=%d unresolved=%d failed=%d",
        len(pending_orders), confirmed_count, balance_reconcile_count, unresolved_count, failed_count,
    )

    return {
        "status": "ERROR" if failed_count > 0 else ("OK" if unresolved_count == 0 else "WARN"),
        "pending_count": len(pending_orders),
        "confirmed_count": confirmed_count,
        "balance_reconcile_count": balance_reconcile_count,
        "unresolved_count": unresolved_count,
        "failed_count": failed_count,
        "symbols_by_status": symbols_by_status,
    }


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
    from trader.us.db.repos import load_pending_ack_orders

    if orders is None:
        orders = load_pending_ack_orders(trade_date=trade_date, env=env)

    try:
        balance = _get_balance_force_refresh(provider)
        final_positions = _build_kis_position_by_symbol(balance.get("positions", []))
    except Exception as exc:
        return {"status": "ERROR", "error": str(exc), "orders": [], "pending_order_count": len(orders or [])}

    classified: list[dict] = []
    pending = 0
    counts: dict[str, int] = {}
    for order in orders or []:
        symbol = str(order.get("symbol") or "").upper().strip()
        side = str(order.get("side") or "").upper()
        qty = int(order.get("qty_requested") or order.get("qty") or order.get("filled_qty") or order.get("qty_filled") or 0)
        pre_qty, _source = _extract_pre_order_position_qty(order)
        final_qty = int((final_positions.get(symbol) or {}).get("qty") or 0)
        raw_status = str(order.get("status") or "").upper()
        fill_qty = int(order.get("qty_filled") or order.get("filled_qty") or 0)
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
        if raw_status in {"REJECT", "REJECTED"}:
            final_status = "rejected"
        elif raw_status in {"BLOCKED", "WARN_DUPLICATE_EXIT_BLOCKED"}:
            final_status = "DUPLICATE_OR_ALREADY_CLOSED" if raw_status == "WARN_DUPLICATE_EXIT_BLOCKED" else "BLOCKED"
        elif fill_qty > 0 or raw_status in {"FILLED", "PARTIALLY_FILLED"}:
            final_status = "broker_fill_confirmed"
        elif side == "BUY" and pre_qty is not None and qty > 0 and final_qty - pre_qty >= qty:
            final_status = "balance_delta_confirmed"
        elif side == "SELL" and pre_qty is not None and qty > 0 and pre_qty - final_qty >= qty:
            final_status = "balance_delta_confirmed"
        else:
            final_status = "ack_only_unresolved"
        if final_status == "ack_only_unresolved":
            pending += 1
        counts[final_status] = counts.get(final_status, 0) + 1
        classified.append({
            "time": str(order.get("created_at") or order.get("time") or ""),
            "side": side,
            "symbol": symbol,
            "qty": qty,
            "order_no": str(order.get("order_no") or order.get("ack_no") or ""),
            "client_order_key": str(order.get("client_order_key") or ""),
            "ack_status": raw_status,
            "fill_api_status": "broker_fill_confirmed" if final_status == "broker_fill_confirmed" else "NOT_CONFIRMED_BY_FILL_API",
            "balance_delta_status": ("balance_delta_confirmed_" + side.lower()) if final_status == "balance_delta_confirmed" else "NOT_CONFIRMED_BY_BALANCE_DELTA",
            "final_status": final_status,
            "price_source": str((order.get("meta") or {}).get("price_source") or order.get("price_source") or ""),
            "pnl_if_sell": (order.get("meta") or {}).get("pnl_if_sell") if isinstance(order.get("meta") or {}, dict) else None,
            "pre_order_position_qty": pre_qty,
            "final_position_qty": final_qty,
        })
    return {
        "status": "OK" if pending == 0 else "DEGRADED_ACK_UNRESOLVED",
        "orders": classified,
        "counts": counts,
        "pending_order_count": pending,
        "reason": "" if pending == 0 else "ACK_ONLY_UNRESOLVED",
    }
