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


def check_us_capital_budget(
    order_notional_usd: float,
    available_cash_usd: float,
    symbol: str = "",
) -> None:
    """미국장 5천만원 환산 예산 한도 초과 여부 검증.

    [US_RISK][BUDGET] 로그 출력 후
    effective_order_budget_usd 를 초과하면 us_capital_budget_exceeded 차단.
    """
    from trader.us.budget import resolve_us_order_budget

    budget = resolve_us_order_budget(available_cash_usd)
    effective = budget["effective_order_budget_usd"]

    logger.info(
        "[US_RISK][BUDGET] symbol=%s effective_budget=%.2f notional=%.2f cap_usd=%.2f",
        symbol, effective, order_notional_usd, budget["capital_usd_cap"],
    )

    if order_notional_usd > effective:
        _block(
            "us_capital_budget_exceeded",
            symbol=symbol,
            notional_usd=order_notional_usd,
            effective_budget=effective,
        )


def check_same_day_rebuy(symbol: str, side: str) -> None:
    """당일 매도 후 재매수 차단.

    US_BLOCK_REBUY_AFTER_SELL_SAME_DAY=1 일 때만 활성화.
    """
    from trader.utils.env import env_bool
    if not env_bool("US_BLOCK_REBUY_AFTER_SELL_SAME_DAY", default=False):
        return

    if side.upper() != "BUY":
        return

    try:
        from trader.us.db.repos import load_today_symbols_sold
        sold_today = load_today_symbols_sold()
        if symbol in sold_today:
            _block("same_day_rebuy_block", symbol=symbol)
    except RiskGateBlocked:
        raise
    except Exception as exc:
        logger.warning("[US_RISK][WARN] same_day_rebuy check failed: %s", exc)


def check_pending_order(symbol: str, side: str) -> None:
    """미체결 주문 존재 시 추가 주문 차단.

    US_ORDER_ACCEPTED_IS_NOT_FILLED=1 일 때만 활성화.
    """
    from trader.utils.env import env_bool
    if not env_bool("US_ORDER_ACCEPTED_IS_NOT_FILLED", default=False):
        return

    try:
        from trader.us.db.repos import has_pending_order
        if has_pending_order(symbol):
            _block("pending_order_exists", symbol=symbol, side=side)
    except RiskGateBlocked:
        raise
    except Exception as exc:
        logger.warning("[US_RISK][WARN] pending_order check failed: %s", exc)


def check_entry_cutoff(side: str, now: Any = None) -> None:
    """15:45 ET 이후 신규 매수 차단.

    US_BLOCK_NEW_ENTRY_AFTER_ET 환경변수가 명시적으로 설정된 경우에만 활성화한다.
    """
    if side.upper() != "BUY":
        return

    cutoff_str = os.getenv("US_BLOCK_NEW_ENTRY_AFTER_ET", "")
    if not cutoff_str:
        # 명시적으로 설정되지 않으면 검사하지 않음
        return
    try:
        from zoneinfo import ZoneInfo
        from datetime import time as dtime
        NY_TZ = ZoneInfo("America/New_York")

        if now is None:
            from trader.us.market_calendar import now_ny
            now_dt = now_ny()
        else:
            if hasattr(now, "astimezone"):
                now_dt = now.astimezone(NY_TZ)
            else:
                now_dt = now

        h, m = cutoff_str.split(":")
        cutoff = dtime(int(h), int(m))

        if now_dt.time() >= cutoff:
            _block(
                "after_entry_cutoff",
                cutoff=cutoff_str,
                current_et=now_dt.strftime("%H:%M:%S"),
            )
    except RiskGateBlocked:
        raise
    except Exception as exc:
        logger.warning("[US_RISK][WARN] entry cutoff check failed: %s", exc)


def assert_order_allowed(
    intent: dict,
    *,
    current_daily_notional_usd: float = 0.0,
    current_position_count: int = 0,
    total_portfolio_usd: float = 1000.0,
    available_cash_usd: float = 1000.0,
    existing_order_keys: set[str] | None = None,
    now: Any = None,
) -> None:
    """Order intent의 전체 위험 점검.

    모든 체크를 통과하면 [US_RISK][PASS] 로그.
    하나라도 실패하면 RiskGateBlocked 예외.
    """
    symbol = intent.get("symbol", "")
    exchange = intent.get("exchange", "")
    side = intent.get("side", "BUY")
    qty = int(intent.get("qty", 0))
    notional_usd = float(intent.get("notional_usd", 0.0))
    client_order_key = intent.get("client_order_key", "")

    check_env_flags()
    check_symbol(symbol)
    check_exchange(exchange)
    check_qty(qty)

    # SELL: 보유 수량 초과 차단
    if side.upper() == "SELL":
        available_qty = intent.get("available_qty")
        if available_qty is not None:
            try:
                if qty > int(available_qty):
                    _block("sell_qty_exceeds_position",
                           symbol=symbol, qty=qty, available_qty=available_qty)
            except (TypeError, ValueError):
                pass
        # SELL 전용: 미체결 주문 존재 시 매도 차단
        check_pending_order(symbol, side)
    else:
        # BUY 전용 체크
        # 예산 기반 차단 (US_PAPER_MAX_CAPITAL_KRW 기준 5천만원 환산)
        check_us_capital_budget(notional_usd, available_cash_usd, symbol=symbol)

        check_notional(notional_usd, symbol=symbol)
        check_daily_notional(notional_usd, current_daily_notional_usd, symbol=symbol)
        check_position_count(current_position_count, symbol=symbol)
        check_position_weight(notional_usd, total_portfolio_usd, symbol=symbol)
        check_cash_buffer(available_cash_usd, notional_usd, symbol=symbol)

        if existing_order_keys is not None and client_order_key:
            check_duplicate(client_order_key, existing_order_keys)

        check_same_day_rebuy(symbol, side)
        check_pending_order(symbol, side)
        check_entry_cutoff(side, now=now)

    _pass(symbol, notional_usd)
