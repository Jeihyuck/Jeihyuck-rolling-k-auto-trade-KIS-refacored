# -*- coding: utf-8 -*-
"""US Risk Gate.

모든 주문 intent는 이 gate를 통과해야 한다.
환경변수/포지션 한도/현금 버퍼/중복 주문 등을 검증한다.
"""
from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any

from trader.us import config as us_cfg
from trader.us.symbols import is_known_symbol, resolve_exchange
from trader.us.execution.kis_us_registry import get_order_exchange_code_for_api

logger = logging.getLogger(__name__)


class RiskGateBlocked(Exception):
    """위험 게이트 통과 실패."""


def _block(reason: str, symbol: str = "", **kw: Any) -> None:
    parts = [f"[US_RISK][BLOCK] reason={reason}"]
    if symbol:
        parts.append(f"symbol={symbol}")
    for k, v in kw.items():
        parts.append(f"{k}={v}")
    msg = " ".join(parts)
    logger.warning(msg)
    raise RiskGateBlocked(msg)


def _pass(symbol: str, notional_usd: float) -> None:
    logger.info(f"[US_RISK][PASS] symbol={symbol} notional_usd={notional_usd:.2f}")


def check_env_flags() -> None:
    """환경변수 guard (매 호출마다 os.getenv로 직접 읽는다)."""
    from trader.utils.env import env_bool
    if not env_bool("US_AGENT_ENABLED", default=False):
        _block("us_agent_not_enabled")
    if os.getenv("TRADING_REGION", "").upper() != "US":
        _block("trading_region_not_us")
    if os.getenv("KIS_ENV", "practice").lower() != "practice":
        _block("kis_env_not_practice")
    if not env_bool("US_PAPER_TRADING_ENABLED", default=False):
        _block("paper_trading_not_enabled")
    if env_bool("US_LIVE_TRADING_ENABLED", default=False):
        _block("live_trading_flag_enabled")
    if not env_bool("DISABLE_REAL_TRADING", default=True):
        _block("real_trading_not_disabled")


def check_symbol(symbol: str) -> None:
    """symbol이 universe에 존재하는지 확인."""
    if not is_known_symbol(symbol):
        _block("symbol_not_in_universe", symbol=symbol)


def check_exchange(exchange: str) -> None:
    """exchange가 registry에 존재하는지 확인."""
    try:
        get_order_exchange_code_for_api(exchange)
    except ValueError:
        _block("exchange_not_in_registry", exchange=exchange)


def check_qty(qty: int) -> None:
    if qty <= 0:
        _block("invalid_qty", qty=qty)


def check_notional(notional_usd: float, symbol: str = "") -> None:
    limit = float(os.getenv("US_MAX_ORDER_USD", "100"))
    if notional_usd > limit:
        _block(
            "notional_exceeds_order_limit",
            symbol=symbol,
            notional_usd=notional_usd,
            limit=limit,
        )


def check_daily_notional(
    new_notional_usd: float,
    current_daily_notional_usd: float,
    symbol: str = "",
) -> None:
    total = current_daily_notional_usd + new_notional_usd
    limit = float(os.getenv("US_MAX_DAILY_NOTIONAL_USD", "500"))
    if total > limit:
        _block(
            "daily_notional_exceeded",
            symbol=symbol,
            new_notional=new_notional_usd,
            current_total=current_daily_notional_usd,
            limit=limit,
        )


def check_position_count(current_count: int, symbol: str = "") -> None:
    limit = int(os.getenv("US_MAX_POSITIONS", "10"))
    if current_count >= limit:
        _block("max_positions_reached", symbol=symbol, count=current_count, limit=limit)


def check_position_weight(
    notional_usd: float,
    total_portfolio_usd: float,
    symbol: str = "",
) -> None:
    if total_portfolio_usd <= 0:
        return
    weight = notional_usd / total_portfolio_usd
    limit = float(os.getenv("US_MAX_POSITION_WEIGHT", "0.10"))
    if weight > limit:
        _block(
            "position_weight_exceeded",
            symbol=symbol,
            weight=round(weight, 4),
            limit=limit,
        )


def check_cash_buffer(
    available_cash_usd: float,
    order_notional_usd: float,
    symbol: str = "",
) -> None:
    buffer = float(os.getenv("US_MIN_CASH_BUFFER_USD", "50"))
    remaining = available_cash_usd - order_notional_usd
    if remaining < buffer:
        _block(
            "cash_below_buffer",
            symbol=symbol,
            remaining=remaining,
            buffer=buffer,
        )


def check_duplicate(client_order_key: str, existing_keys: set[str]) -> None:
    if client_order_key in existing_keys:
        msg = f"[US_DUPLICATE][BLOCK] client_order_key={client_order_key!r} already exists"
        logger.warning(msg)
        raise RiskGateBlocked(msg)


def assert_order_allowed(
    intent: dict,
    *,
    current_daily_notional_usd: float = 0.0,
    current_position_count: int = 0,
    total_portfolio_usd: float = 1000.0,
    available_cash_usd: float = 1000.0,
    existing_order_keys: set[str] | None = None,
) -> None:
    """Order intent의 전체 위험 점검.

    모든 체크를 통과하면 [US_RISK][PASS] 로그.
    하나라도 실패하면 RiskGateBlocked 예외.
    """
    symbol = intent.get("symbol", "")
    exchange = intent.get("exchange", "")
    qty = int(intent.get("qty", 0))
    notional_usd = float(intent.get("notional_usd", 0.0))
    client_order_key = intent.get("client_order_key", "")

    check_env_flags()
    check_symbol(symbol)
    check_exchange(exchange)
    check_qty(qty)
    check_notional(notional_usd, symbol=symbol)
    check_daily_notional(notional_usd, current_daily_notional_usd, symbol=symbol)
    check_position_count(current_position_count, symbol=symbol)
    check_position_weight(notional_usd, total_portfolio_usd, symbol=symbol)
    check_cash_buffer(available_cash_usd, notional_usd, symbol=symbol)

    if existing_order_keys is not None and client_order_key:
        check_duplicate(client_order_key, existing_order_keys)

    _pass(symbol, notional_usd)
