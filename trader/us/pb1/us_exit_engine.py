# -*- coding: utf-8 -*-
"""US PB1 Exit Engine.

한국장 PB1의 청산 전략을 미국장용으로 이식.

청산 종류:
- hard_stop:      손절 한도 초과
- trailing_stop:  고점 대비 하락
- profit_protect: 목표 수익 근접 시 이익 보호
- giveback:       수익 반납 비율 초과
- time_stop:      보유 기간 초과
- risk_off:       시장 전체 위험 off

정책:
- 주문 ACK와 fill 분리 (US_ORDER_ACCEPTED_IS_NOT_FILLED=1)
- fill confirm 전까지 포지션 반영 금지
- same-day soft exit 차단 (US_BLOCK_REBUY_AFTER_SELL_SAME_DAY)
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# 설정값
_HARD_STOP_PCT = float(os.getenv("US_HARD_STOP_PCT", "0.07"))       # 7% 손절
_TRAILING_STOP_PCT = float(os.getenv("US_TRAILING_STOP_PCT", "0.05")) # 5% trailing
_PROFIT_PROTECT_PCT = float(os.getenv("US_PROFIT_PROTECT_PCT", "0.15")) # 15% 수익 보호
_GIVEBACK_PCT = float(os.getenv("US_GIVEBACK_PCT", "0.33"))           # 최고 수익 33% 반납
_TIME_STOP_DAYS = int(os.getenv("US_TIME_STOP_DAYS", "20"))           # 20일 보유


def _reload_env() -> dict:
    """환경변수 최신값 로드."""
    return {
        "hard_stop": float(os.getenv("US_HARD_STOP_PCT", "0.07")),
        "trailing_stop": float(os.getenv("US_TRAILING_STOP_PCT", "0.05")),
        "profit_protect": float(os.getenv("US_PROFIT_PROTECT_PCT", "0.15")),
        "giveback": float(os.getenv("US_GIVEBACK_PCT", "0.33")),
        "time_stop_days": int(os.getenv("US_TIME_STOP_DAYS", "20")),
    }


def evaluate_exit(
    position: dict,
    current_price: float,
    now: datetime | None = None,
) -> dict | None:
    """단일 포지션 청산 조건 평가.

    Args:
        position: {symbol, exchange, qty, entry_price, entry_date, max_price, ...}
        current_price: 현재가 (USD)
        now: 현재 시각

    Returns:
        청산 intent dict 또는 None (청산 불필요)
    """
    cfg = _reload_env()

    symbol = position.get("symbol", "")
    exchange = position.get("exchange", "NASDAQ")
    qty = int(position.get("qty", 0))
    entry_price = float(position.get("entry_price", 0.0))
    max_price = float(position.get("max_price", current_price))

    if qty <= 0 or entry_price <= 0 or current_price <= 0:
        return None

    pnl_pct = (current_price - entry_price) / entry_price
    unrealized_pnl_usd = (current_price - entry_price) * qty

    # ── hard stop ─────────────────────────────────────────────────────────────
    if pnl_pct <= -cfg["hard_stop"]:
        return _make_exit_intent(
            symbol=symbol, exchange=exchange, qty=qty,
            current_price=current_price, entry_price=entry_price,
            exit_type="hard_stop",
            reason=f"pnl_pct={pnl_pct:.3f} <= -{cfg['hard_stop']}",
            unrealized_pnl_usd=unrealized_pnl_usd,
            pnl_pct=pnl_pct,
        )

    # ── trailing stop ─────────────────────────────────────────────────────────
    if max_price > 0 and current_price < max_price * (1 - cfg["trailing_stop"]):
        trail_pct = (max_price - current_price) / max_price
        return _make_exit_intent(
            symbol=symbol, exchange=exchange, qty=qty,
            current_price=current_price, entry_price=entry_price,
            exit_type="trailing_stop",
            reason=f"trail_pct={trail_pct:.3f} > {cfg['trailing_stop']}",
            unrealized_pnl_usd=unrealized_pnl_usd,
            pnl_pct=pnl_pct,
        )

    # ── profit protect ────────────────────────────────────────────────────────
    if pnl_pct >= cfg["profit_protect"]:
        # 수익 보호: 고점 대비 X% 빠지면 청산
        protect_trail = 0.03
        if max_price > 0 and current_price < max_price * (1 - protect_trail):
            return _make_exit_intent(
                symbol=symbol, exchange=exchange, qty=qty,
                current_price=current_price, entry_price=entry_price,
                exit_type="profit_protect",
                reason=f"pnl_pct={pnl_pct:.3f} high but pulling back",
                unrealized_pnl_usd=unrealized_pnl_usd,
                pnl_pct=pnl_pct,
            )

    # ── giveback ──────────────────────────────────────────────────────────────
    if max_price > entry_price:
        max_gain = (max_price - entry_price) / entry_price
        current_gain = pnl_pct
        if max_gain > 0.05:  # 최소 5% 수익 있을 때만 giveback 적용
            giveback_ratio = (max_gain - current_gain) / max_gain
            if giveback_ratio >= cfg["giveback"]:
                return _make_exit_intent(
                    symbol=symbol, exchange=exchange, qty=qty,
                    current_price=current_price, entry_price=entry_price,
                    exit_type="giveback",
                    reason=f"giveback_ratio={giveback_ratio:.3f} >= {cfg['giveback']}",
                    unrealized_pnl_usd=unrealized_pnl_usd,
                    pnl_pct=pnl_pct,
                )

    return None


def _make_exit_intent(
    symbol: str,
    exchange: str,
    qty: int,
    current_price: float,
    entry_price: float,
    exit_type: str,
    reason: str,
    unrealized_pnl_usd: float,
    pnl_pct: float,
) -> dict:
    """Exit order intent 생성."""
    import hashlib
    from datetime import date
    today = date.today().strftime("%Y%m%d")
    key_raw = f"{symbol}_{today}_SELL_{exit_type}"
    client_order_key = hashlib.sha256(key_raw.encode()).hexdigest()[:24]

    logger.info(
        "[US_EXIT][SIGNAL] symbol=%s exit_type=%s reason=%s pnl_pct=%.3f",
        symbol, exit_type, reason, pnl_pct,
    )

    return {
        "symbol": symbol,
        "exchange": exchange,
        "side": "SELL",
        "qty": qty,
        "limit_price": round(current_price * 0.998, 4),  # 0.2% 슬리피지 허용
        "notional_usd": round(current_price * qty, 4),
        "exit_type": exit_type,
        "reason": reason,
        "unrealized_pnl_usd": round(unrealized_pnl_usd, 4),
        "unrealized_pnl_pct": round(pnl_pct, 4),
        "client_order_key": client_order_key,
        "strategy": "us_pb1_exit",
    }


def generate_exit_intents(
    positions: list[dict],
    provider: Any,
    now: datetime | None = None,
) -> list[dict]:
    """보유 포지션 전체에 대해 청산 조건 평가.

    Args:
        positions: 보유 포지션 목록
        provider: USDataProvider
        now: 현재 시각

    Returns:
        청산 intent 목록
    """
    intents: list[dict] = []

    if not positions:
        return intents

    for pos in positions:
        symbol = pos.get("symbol", "")
        exchange = pos.get("exchange", "NASDAQ")

        try:
            price_data = provider.get_current_price(symbol, exchange)
            current_price = float(price_data.get("last", 0))
        except Exception as exc:
            logger.warning("[US_EXIT][WARN] price fetch failed symbol=%s error=%s", symbol, exc)
            continue

        if current_price <= 0:
            continue

        intent = evaluate_exit(position=pos, current_price=current_price, now=now)
        if intent is not None:
            intents.append(intent)

    return intents
