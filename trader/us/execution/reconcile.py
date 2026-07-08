# -*- coding: utf-8 -*-
"""US Positions Reconcile.

KIS 잔고와 로컬 DB 잔고를 비교 검증.
"""
from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


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


def reconcile_positions(provider: Any | None = None) -> dict:
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

    # Extract fields from normalized balance
    positions = balance.get("positions", [])
    total_pvs = balance.get("total_pvs", "0")
    total_pvs_source = balance.get("total_pvs_source", "unknown")
    raw_output1_count = balance.get("raw_output1_count", 0)
    normalized_position_count = balance.get("normalized_position_count", 0)
    position_symbols = balance.get("position_symbols", [])
    balance_parse_status = balance.get("balance_parse_status", "UNKNOWN")
    balance_parse_error = balance.get("balance_parse_error")
    
    # Log raw and normalized counts
    logger.info(
        "[US_RECONCILE][BALANCE_RAW] output1_count=%d",
        raw_output1_count,
    )
    
    if positions:
        logger.info(
            "[US_RECONCILE][POSITIONS_NORMALIZED] count=%d symbols=%s",
            len(positions),
            ",".join(position_symbols),
        )
    else:
        logger.info("[US_RECONCILE][POSITIONS_NORMALIZED] count=0")
    
    # Check balance parse status
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
    
    # Check contract error: raw > 0 but normalized == 0
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

    # KIS balance authoritative: us_positions DB 동기화
    if positions:
        logger.info(
            "[US_RECONCILE][AUTHORITATIVE] source=kis_balance positions=%d symbols=%s",
            len(positions),
            ",".join(position_symbols),
        )
        try:
            from trader.us.db.repos import save_position_snapshot
            saved = save_position_snapshot(positions)
            logger.info(
                "[US_RECONCILE][UPSERT_POSITIONS] count=%d source=kis_balance_authoritative",
                saved,
            )
        except Exception as exc:
            logger.warning("[US_RECONCILE][UPSERT_WARN] failed to upsert positions: %s", exc)

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
        # Priority 1: canonical pre-order snapshot.
        (order, "pre_order_position_qty", "order_pre_order_position_qty"),
        (meta, "pre_order_position_qty", "meta_pre_order_position_qty"),
        (meta, "pre_order_position_snapshot.qty", "meta_pre_order_position_snapshot_qty"),
        # Priority 2: SELL-specific partial-sell snapshot aliases.
        (order, "pre_sell_qty", "order_pre_sell_qty"),
        (meta, "pre_sell_qty", "meta_pre_sell_qty"),
        (meta, "pre_sell_position_snapshot.qty", "meta_pre_sell_position_snapshot_qty"),
        # Priority 3: generic position snapshot quantity aliases.
        (order, "position_snapshot_qty", "order_position_snapshot_qty"),
        (meta, "position_snapshot_qty", "meta_position_snapshot_qty"),
        # Priority 4: holding quantity aliases.
        (order, "holding_qty", "order_holding_qty"),
        (meta, "holding_qty", "meta_holding_qty"),
        # Existing BUY/generic aliases.
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
        # Priority 5: orderable_qty is last because it can be lower than holding_qty.
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

    # Without an explicit pre-order snapshot (or explicit was_new_position metadata
    # handled above), total current quantity is not safe evidence of a BUY fill.
    return False, "missing_pre_order_qty_snapshot", None



def _get_balance_force_refresh(provider: Any) -> dict:
    try:
        return provider.get_balance(force_refresh=True)
    except TypeError as exc:
        if "force_refresh" not in str(exc):
            raise
        logger.warning("[US_RECONCILE][BALANCE_FORCE_REFRESH_UNSUPPORTED] provider=%s", type(provider).__name__)
        return provider.get_balance()

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

def reconcile_ack_orders_with_balance(
    *,
    provider: Any | None = None,
    trade_date: str,
    env: str = "practice",
) -> dict:
    """ACK 상태이나 qty_filled=0인 주문의 체결 여부를 KIS fills + 잔고로 확인.

    1. us_orders에서 ACK + qty_filled=0 주문 로드
    2. KIS 체결조회로 fill 확인 → 확인되면 us_fills 저장 + us_orders.status=FILLED
    3. fill 미확인 시 잔고 변화 확인 → SELL이면 포지션 소멸 여부로 판단
    4. 결과 반환

    Returns:
        {
            "status": "OK"|"WARN"|"ERROR",
            "pending_count": int,
            "confirmed_count": int,
            "balance_reconcile_count": int,
            "unresolved_count": int,
        }
    """
    from trader.us.db.repos import load_pending_ack_orders, mark_order_filled_by_reconcile

    if provider is None:
        from trader.us.data_provider import USDataProvider
        provider = USDataProvider(offline=True)

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
            "symbols_by_status": {},
        }

    logger.info(
        "[US_RECONCILE][ACK_RECONCILE][START] pending_count=%d trade_date=%s",
        len(pending_orders), trade_date,
    )

    # KIS 잔고 조회 (BUY 생성 / SELL 소멸 확인용)
    kis_position_by_symbol: dict[str, dict] = {}
    kis_position_symbols: set[str] = set()
    try:
        balance = _get_balance_force_refresh(provider)
        kis_position_by_symbol = _build_kis_position_by_symbol(balance.get("positions", []))
        kis_position_symbols = set(kis_position_by_symbol.keys())
    except Exception as exc:
        logger.warning("[US_RECONCILE][ACK_RECONCILE][WARN] balance fetch failed: %s", exc)

    confirmed_count = 0
    balance_reconcile_count = 0
    unresolved_count = 0
    symbols_by_status: dict[str, list[str]] = {"fill_api_confirmed": [], "balance_confirmed": [], "unresolved": []}

    for order in pending_orders:
        symbol = str(order.get("symbol", "")).strip().upper()
        side = str(order.get("side", "")).upper()
        order_no = str(order.get("order_no") or order.get("ack_no") or "")
        client_order_key = str(order.get("client_order_key") or "")
        qty = int(
            order.get("qty_requested")
            or order.get("qty")
            or order.get("filled_qty")
            or order.get("qty_filled")
            or 0
        )
        fallback_fill_price, fallback_price_source = _resolve_order_fill_price(order)
        pre_qty_for_delta, _pre_source = _extract_pre_order_position_qty(order)
        post_qty_for_delta = int((kis_position_by_symbol.get(symbol) or {}).get("qty") or 0)
        delta_confirmation = confirm_order_by_balance_delta(side, qty, pre_qty_for_delta, post_qty_for_delta)

        # KIS 체결조회 시도
        fill_confirmed = False
        fill_price = fallback_fill_price
        fill_price_source = fallback_price_source or "unavailable"
        fill_qty = qty

        try:
            fills_resp = provider.get_fills_by_order_no(order_no=order_no, symbol=symbol)
            if fills_resp and isinstance(fills_resp, dict):
                if fills_resp.get("filled_qty", 0) > 0:
                    fill_price = float(fills_resp.get("avg_price", 0) or 0)
                    fill_price_source = "fills_by_order_no"
                    fill_qty = int(fills_resp.get("filled_qty", qty))
                    fill_confirmed = True
        except AttributeError:
            # provider does not support get_fills_by_order_no (offline/mock)
            pass
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
                mark_order_filled_by_reconcile(
                    order_no=order_no,
                    client_order_key=client_order_key,
                    symbol=symbol,
                    side=side,
                    filled_qty=fill_qty,
                    avg_price_usd=fill_price,
                    source="fills_reconcile",
                    trade_date=trade_date,
                    meta=_order_meta(order),
                )
                confirmed_count += 1
                symbols_by_status["fill_api_confirmed"].append(symbol)
            except Exception as exc:
                logger.error(
                    "[US_RECONCILE][ACK_RECONCILE][ERROR] mark_order_filled failed symbol=%s: %s",
                    symbol, exc,
                )
            continue

        # fill 미확인: explicit 주문 전/후 잔고 delta가 명확하면 balance-confirmed 처리
        if delta_confirmation["status"] in {"BALANCE_CONFIRMED_BUY", "BALANCE_CONFIRMED_SELL", "BALANCE_CONFIRMED_PARTIAL"}:
            filled_by_balance = int(delta_confirmation.get("filled_qty_by_balance") or 0)
            source_name = "balance_reconcile_partial" if delta_confirmation["status"] == "BALANCE_CONFIRMED_PARTIAL" else f"balance_reconcile_{side.lower()}"
            logger.info(
                "[US_RECONCILE][BALANCE_DELTA_CONFIRMED] symbol=%s side=%s order_qty=%d filled_qty=%d pre_qty=%s post_qty=%s status=%s",
                symbol, side, qty, filled_by_balance, pre_qty_for_delta, post_qty_for_delta, delta_confirmation["status"],
            )
            try:
                mark_order_filled_by_reconcile(
                    order_no=order_no,
                    client_order_key=client_order_key,
                    symbol=symbol,
                    side=side,
                    filled_qty=filled_by_balance,
                    avg_price_usd=fallback_fill_price,
                    source=source_name,
                    trade_date=trade_date,
                    meta={**_order_meta(order), "balance_delta_status": delta_confirmation["status"], "remaining_qty": delta_confirmation.get("remaining_qty", 0)},
                )
                balance_reconcile_count += 1
                symbols_by_status["balance_confirmed"].append(symbol)
            except Exception as exc:
                logger.error("[US_RECONCILE][ACK_RECONCILE][ERROR] balance_delta_confirm failed symbol=%s: %s", symbol, exc)
            continue

        # fill 미확인: BUY이면 KIS 잔고 증가분으로만 balance reconcile
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
                    mark_order_filled_by_reconcile(
                        order_no=order_no,
                        client_order_key=client_order_key,
                        symbol=symbol,
                        side="BUY",
                        filled_qty=qty,
                        avg_price_usd=fill_price_candidate,
                        source="balance_reconcile_buy",
                        trade_date=trade_date,
                        meta=_order_meta(order),
                    )
                    balance_reconcile_count += 1
                    symbols_by_status["balance_confirmed"].append(symbol)
                except Exception as exc:
                    logger.error(
                        "[US_RECONCILE][ACK_RECONCILE][ERROR] balance_reconcile_buy failed symbol=%s: %s",
                        symbol, exc,
                    )
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

        # fill 미확인: SELL이면 KIS 잔고에 포지션 없으면 balance reconcile
        if side == "SELL" and symbol not in kis_position_symbols:
            if qty <= 0:
                logger.warning(
                    "[US_RECONCILE][ACK_RECONCILE][INVALID_QTY] symbol=%s order_no=%s side=SELL qty=%d reason=missing_qty_requested",
                    symbol,
                    order_no,
                    qty,
                )
                unresolved_count += 1
                symbols_by_status["unresolved"].append(symbol)
                continue

            if fallback_fill_price > 0:
                logger.info(
                    "[US_RECONCILE][BALANCE_RECONCILE_FILL] symbol=%s side=SELL qty=%d price_source=%s price=%.4f source=balance_reconcile_sell",
                    symbol,
                    qty,
                    fill_price_source,
                    fallback_fill_price,
                )
            else:
                logger.warning(
                    "[US_RECONCILE][BALANCE_RECONCILE_FILL_PRICE_MISSING] symbol=%s side=SELL qty=%d source=balance_reconcile_sell",
                    symbol,
                    qty,
                )

            logger.info(
                "[US_RECONCILE][BALANCE_RECONCILE_TRIGGER] symbol=%s side=SELL qty=%d",
                symbol,
                qty,
            )
            try:
                mark_order_filled_by_reconcile(
                    order_no=order_no,
                    client_order_key=client_order_key,
                    symbol=symbol,
                    side="SELL",
                    filled_qty=qty,
                    avg_price_usd=fallback_fill_price,
                    source="balance_reconcile_sell",
                    trade_date=trade_date,
                    meta=_order_meta(order),
                )
                balance_reconcile_count += 1
                symbols_by_status["balance_confirmed"].append(symbol)
            except Exception as exc:
                logger.error(
                    "[US_RECONCILE][ACK_RECONCILE][ERROR] balance_reconcile_fill failed symbol=%s: %s",
                    symbol, exc,
                )
            continue

        # 미해결
        logger.warning(
            "[US_RECONCILE][ACK_RECONCILE][UNRESOLVED] symbol=%s order_no=%s side=%s",
            symbol, order_no, side,
        )
        unresolved_count += 1
        symbols_by_status["unresolved"].append(symbol)

    logger.info(
        "[US_RECONCILE][ACK_RECONCILE][DONE] pending=%d confirmed=%d balance_reconcile=%d unresolved=%d",
        len(pending_orders), confirmed_count, balance_reconcile_count, unresolved_count,
    )

    return {
        "status": "OK" if unresolved_count == 0 else "WARN",
        "pending_count": len(pending_orders),
        "confirmed_count": confirmed_count,
        "balance_reconcile_count": balance_reconcile_count,
        "unresolved_count": unresolved_count,
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
        elif raw_status in {"ACK", "ACKED", "ACCEPTED", "SENT", "ACK_DB_FAILED"}:
            final_status = "ack_only_unresolved"
        else:
            final_status = "ack_only_unresolved"
        if final_status in {"ack_only_unresolved"}:
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
    return {"status": "OK", "orders": classified, "counts": counts, "pending_order_count": pending}
