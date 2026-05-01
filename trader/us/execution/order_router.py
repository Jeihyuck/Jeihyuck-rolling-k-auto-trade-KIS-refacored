# -*- coding: utf-8 -*-
"""US Order Router.

strategy intent를 risk gate에 통과시키고,
DRY_RUN 여부에 따라 ledger 기록 또는 KIS 모의주문 실행.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from trader.us import config as us_cfg
from trader.us.execution.risk_gate import RiskGateBlocked, assert_order_allowed

logger = logging.getLogger(__name__)

# in-process 중복 주문 키 저장소 (DB 없을 때 사용)
_SENT_ORDER_KEYS: set[str] = set()


def _log_intent(intent: dict) -> None:
    logger.info(
        "[US_ORDER][INTENT] symbol=%s side=%s qty=%s notional_usd=%.2f key=%s",
        intent.get("symbol"),
        intent.get("side"),
        intent.get("qty"),
        float(intent.get("notional_usd", 0)),
        intent.get("client_order_key", ""),
    )


def _log_dry_run(intent: dict) -> None:
    logger.info(
        "[US_ORDER][DRY_RUN] symbol=%s side=%s qty=%s",
        intent.get("symbol"),
        intent.get("side"),
        intent.get("qty"),
    )


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
        결과 dict: {"status": "DRY_RUN"|"ACK"|"BLOCKED", ...}
    """
    symbol = intent.get("symbol", "")
    side = intent.get("side", "BUY")
    qty = int(intent.get("qty", 0))
    price = float(intent.get("limit_price", 0.0))
    exchange = intent.get("exchange", "NASDAQ")
    order_key = intent.get("client_order_key", "")

    _log_intent(intent)

    # --- Risk Gate ---
    try:
        assert_order_allowed(
            intent,
            current_daily_notional_usd=current_daily_notional_usd,
            current_position_count=current_position_count,
            total_portfolio_usd=total_portfolio_usd,
            available_cash_usd=available_cash_usd,
            existing_order_keys=_SENT_ORDER_KEYS,
        )
    except RiskGateBlocked as exc:
        logger.warning("[US_ORDER][BLOCKED] %s", exc)
        return {"status": "BLOCKED", "reason": str(exc), "intent": intent}

    # --- DRY_RUN ---
    from trader.utils.env import env_bool
    if env_bool("DRY_RUN", default=True):
        _log_dry_run(intent)
        return {
            "status": "DRY_RUN",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "intent": intent,
        }

    # --- Paper order via KIS ---
    if kis_client is None:
        from trader.us.execution.kis_us_client import KisUSClient
        kis_client = KisUSClient(env="practice")

    logger.info("[US_ORDER][SEND] symbol=%s side=%s qty=%s price=%.4f", symbol, side, qty, price)
    try:
        if side == "BUY":
            resp = kis_client.place_us_buy_order(symbol, exchange, qty, price)
        else:
            resp = kis_client.place_us_sell_order(symbol, exchange, qty, price)

        if order_key:
            _SENT_ORDER_KEYS.add(order_key)

        logger.info("[US_ORDER][ACK] symbol=%s order_no=%s", symbol, resp.get("output", {}).get("KRX_FWDG_ORD_ORGNO", ""))
        return {"status": "ACK", "symbol": symbol, "side": side, "qty": qty, "response": resp, "intent": intent}

    except Exception as exc:
        logger.error("[US_ORDER][REJECT] symbol=%s error=%s", symbol, exc)
        return {"status": "REJECT", "reason": str(exc), "intent": intent}


def clear_sent_order_keys() -> None:
    """테스트 cleanup 용."""
    _SENT_ORDER_KEYS.clear()
