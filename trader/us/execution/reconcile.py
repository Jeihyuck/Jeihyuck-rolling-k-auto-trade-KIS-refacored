# -*- coding: utf-8 -*-
"""US Positions Reconcile.

KIS 잔고와 로컬 DB 잔고를 비교 검증.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


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
        balance = provider.get_balance()
    except Exception as exc:
        logger.error("[US_RECONCILE][ERROR] balance fetch failed: %s", exc)
        return {
            "status": "ERROR",
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
        "block_new_entry": False,
    }


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
        }

    logger.info(
        "[US_RECONCILE][ACK_RECONCILE][START] pending_count=%d trade_date=%s",
        len(pending_orders), trade_date,
    )

    # KIS 잔고 조회 (포지션 소멸 확인용)
    kis_position_symbols: set[str] = set()
    try:
        balance = provider.get_balance()
        for pos in balance.get("positions", []):
            sym = str(pos.get("symbol", "") or pos.get("ovrs_pdno", "")).strip().upper()
            if sym:
                kis_position_symbols.add(sym)
    except Exception as exc:
        logger.warning("[US_RECONCILE][ACK_RECONCILE][WARN] balance fetch failed: %s", exc)

    confirmed_count = 0
    balance_reconcile_count = 0
    unresolved_count = 0

    for order in pending_orders:
        symbol = str(order.get("symbol", "")).strip().upper()
        side = str(order.get("side", "")).upper()
        order_no = str(order.get("order_no") or order.get("ack_no") or "")
        client_order_key = str(order.get("client_order_key") or "")
        qty = int(order.get("qty", 0) or 0)

        # KIS 체결조회 시도
        fill_confirmed = False
        fill_price = 0.0
        fill_qty = qty

        try:
            fills_resp = provider.get_fills_by_order_no(order_no=order_no, symbol=symbol)
            if fills_resp and isinstance(fills_resp, dict):
                if fills_resp.get("filled_qty", 0) > 0:
                    fill_price = float(fills_resp.get("avg_price", 0) or 0)
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
                    filled_qty=fill_qty,
                    avg_price_usd=fill_price,
                    source="fills_reconcile",
                )
                confirmed_count += 1
            except Exception as exc:
                logger.error(
                    "[US_RECONCILE][ACK_RECONCILE][ERROR] mark_order_filled failed symbol=%s: %s",
                    symbol, exc,
                )
            continue

        # fill 미확인: SELL이면 KIS 잔고에 포지션 없으면 balance reconcile
        if side == "SELL" and symbol not in kis_position_symbols:
            logger.info(
                "[US_RECONCILE][BALANCE_RECONCILE_FILL] symbol=%s side=SELL qty=%d source=balance_reconcile",
                symbol, qty,
            )
            try:
                mark_order_filled_by_reconcile(
                    order_no=order_no,
                    client_order_key=client_order_key,
                    filled_qty=qty,
                    avg_price_usd=0.0,  # 가격 미확인 — realized PNL 계산 제외
                    source="balance_reconcile",
                )
                balance_reconcile_count += 1
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
    }
