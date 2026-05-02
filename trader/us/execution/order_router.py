# -*- coding: utf-8 -*-
"""US Order Router.

intent를 risk gate에 통과시키고 DB에 저장한 후
DRY_RUN 또는 KIS 모의주문으로 라우팅한다.

처리 순서:
1. intent normalize
2. save_order_intent()
3. DB 기반 + memory 기반 중복 key 합산
4. risk gate 실행
5. BLOCK → mark_order_intent_blocked, return BLOCKED
6. DRY_RUN=1 → save_dry_run_order, mark_order_intent_sent, return DRY_RUN
7. DRY_RUN=0 → KIS 주문 → save_order_ack, mark_order_intent_sent, return ACK
8. KIS REJECT → save_order_reject, mark_order_intent_rejected, return REJECT
"""
from __future__ import annotations

import logging
import os
from typing import Any

from trader.us import config as us_cfg
from trader.us.execution.risk_gate import RiskGateBlocked, assert_order_allowed

logger = logging.getLogger(__name__)

# in-process 중복 키 (DB fallback 없을 때도 동일 process 내 중복 차단)
_SENT_ORDER_KEYS: set[str] = set()


def route_order(
    intent: dict,
    *,
    current_daily_notional_usd: float = 0.0,
    current_position_count: int = 0,
    total_portfolio_usd: float = 1000.0,
    available_cash_usd: float = 1000.0,
    kis_client: Any | None = None,
) -> dict:
    """Order intent를 라우팅한다.

    Returns:
        {"status": "DRY_RUN"|"ACK"|"BLOCKED"|"REJECT", ...}
    """
    from trader.us.db.repos import (
        save_order_intent, save_dry_run_order, save_order_ack, save_order_reject,
        mark_order_intent_sent, mark_order_intent_blocked, mark_order_intent_rejected,
        load_today_order_keys,
    )
    from trader.utils.env import env_bool

    symbol = intent.get("symbol", "")
    side = intent.get("side", "BUY")
    qty = int(intent.get("qty", 0))
    price = float(intent.get("limit_price", 0.0))
    exchange = intent.get("exchange", "NASDAQ")
    order_key = intent.get("client_order_key") or intent.get("order_key", "")

    logger.info(
        "[US_ORDER][INTENT] symbol=%s side=%s qty=%s notional_usd=%.2f key=%s",
        symbol, side, qty, float(intent.get("notional_usd", 0)), order_key,
    )

    # 1. intent DB 저장
    save_order_intent(intent)

    # 2. 중복 key: DB + in-memory 합산
    try:
        db_keys = load_today_order_keys()
    except Exception:
        db_keys = set()
    existing_keys = _SENT_ORDER_KEYS | db_keys

    # 3. Risk Gate
    gate_intent = {**intent, "client_order_key": order_key}
    try:
        assert_order_allowed(
            gate_intent,
            current_daily_notional_usd=current_daily_notional_usd,
            current_position_count=current_position_count,
            total_portfolio_usd=total_portfolio_usd,
            available_cash_usd=available_cash_usd,
            existing_order_keys=existing_keys,
        )
    except RiskGateBlocked as exc:
        logger.warning("[US_ORDER][BLOCKED] %s", exc)
        if order_key:
            mark_order_intent_blocked(order_key, reason=str(exc))
        return {"status": "BLOCKED", "reason": str(exc), "intent": intent}

    # 4. DRY_RUN
    if env_bool("DRY_RUN", default=True):
        logger.info("[US_ORDER][DRY_RUN] symbol=%s side=%s qty=%s", symbol, side, qty)
        dry_intent = {**intent, "client_order_key": order_key}
        save_dry_run_order(dry_intent)
        if order_key:
            mark_order_intent_sent(order_key)
            _SENT_ORDER_KEYS.add(order_key)
        return {
            "status": "DRY_RUN",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "intent": intent,
        }

    # 5. Paper order via KIS
    if kis_client is None:
        from trader.us.execution.kis_us_client import KisUSClient
        kis_client = KisUSClient(env="practice")

    logger.info("[US_ORDER][SEND] symbol=%s side=%s qty=%s price=%.4f", symbol, side, qty, price)
    try:
        if side == "BUY":
            resp = kis_client.place_us_buy_order(symbol, exchange, qty, price)
        else:
            resp = kis_client.place_us_sell_order(symbol, exchange, qty, price)

        from trader.us.execution.kis_us_response_parser import extract_order_no
        order_no = extract_order_no(resp)

        ack_result = {
            "client_order_key": order_key,
            "symbol": symbol,
            "exchange": exchange,
            "side": side,
            "qty_requested": qty,
            "qty_filled": 0,
            "avg_price_usd": price or None,
            "order_no": order_no,
            "status": "ACK",
            "dry_run": False,
            "meta": {"raw_response": resp},
        }
        save_order_ack(ack_result)
        if order_key:
            mark_order_intent_sent(order_key)
            _SENT_ORDER_KEYS.add(order_key)

        logger.info("[US_ORDER][ACK] symbol=%s order_no=%s", symbol, order_no)
        return {"status": "ACK", "symbol": symbol, "side": side, "qty": qty,
                "order_no": order_no, "response": resp, "intent": intent}

    except Exception as exc:
        logger.error("[US_ORDER][REJECT] symbol=%s error=%s", symbol, exc)
        reject_result = {
            "client_order_key": order_key,
            "symbol": symbol,
            "exchange": exchange,
            "side": side,
            "qty": qty,
            "reason": str(exc),
        }
        save_order_reject(reject_result)
        if order_key:
            mark_order_intent_rejected(order_key, reason=str(exc))
        return {"status": "REJECT", "reason": str(exc), "intent": intent}


def clear_sent_order_keys() -> None:
    """테스트 cleanup 용. in-memory주문 스토어도 함께 초기화."""
    _SENT_ORDER_KEYS.clear()
    try:
        from trader.us.db.repos import reset_memory_stores
        reset_memory_stores()
    except Exception:
        pass
