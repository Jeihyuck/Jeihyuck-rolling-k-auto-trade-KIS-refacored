# -*- coding: utf-8 -*-
"""US PB1 Entry Engine.

한국장 PB1 진입 전략을 미국장용으로 이식.

진입 조건:
1. MA20/MA50 위 추세
2. breakout / momentum / pullback entry_style
3. RS / VCP / momentum / pullback score
4. 거래량 조건
5. rank 기반 우선순위
6. 당일 매도 후 재매수 차단
7. 미체결 주문 존재 시 추가 주문 차단
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


def _to_float_list(rows: list[dict], key: str = "clos") -> list[float]:
    out = []
    for r in rows:
        try:
            out.append(float(r.get(key, 0) or 0))
        except (ValueError, TypeError):
            pass
    return out


def _ma(prices: list[float], n: int) -> float | None:
    if len(prices) < n:
        return None
    return sum(prices[-n:]) / n


def _momentum_pct(prices: list[float], n: int) -> float | None:
    if len(prices) < n + 1:
        return None
    old = prices[-(n + 1)]
    if old <= 0:
        return None
    return (prices[-1] - old) / old


def score_symbol(symbol: str, daily_prices: list[dict], current_price: dict) -> float | None:
    """종목 점수 계산.

    Returns:
        0.0~1.0 점수, 또는 None (진입 불가)
    """
    closes = _to_float_list(daily_prices, "clos")
    volumes = _to_float_list(daily_prices, "tvol")

    if len(closes) < 55:
        return None

    last = closes[-1]
    if last <= 0:
        return None

    ma20 = _ma(closes, 20)
    ma50 = _ma(closes, 50)

    if ma20 is None or ma50 is None:
        return None

    # ── 추세 조건 ─────────────────────────────────────────────────────────────
    if not (last > ma20 > ma50 * 0.98):
        return None

    # ── 모멘텀 ────────────────────────────────────────────────────────────────
    mom20 = _momentum_pct(closes, 20)
    mom50 = _momentum_pct(closes, 50)

    if mom20 is None or mom50 is None:
        return None

    # 음의 모멘텀이면 스킵
    if mom20 < 0 or mom50 < 0:
        return None

    # ── 눌림목 (pullback) ─────────────────────────────────────────────────────
    recent_high = max(closes[-20:])
    if recent_high <= 0:
        return None
    pullback = (recent_high - last) / recent_high  # 0 = 고점, 0.15 = 15% 조정

    # 너무 눌리면 스킵 (15% 초과)
    if pullback > 0.15:
        return None

    # ── 거래량 조건 ───────────────────────────────────────────────────────────
    if len(volumes) >= 20:
        avg_vol = sum(volumes[-20:]) / 20
        today_vol = volumes[-1]
        vol_ratio = today_vol / avg_vol if avg_vol > 0 else 1.0
    else:
        vol_ratio = 1.0

    # 거래량 지나치게 부족하면 스킵
    if vol_ratio < 0.3:
        return None

    # ── 최종 점수 계산 ────────────────────────────────────────────────────────
    # 모멘텀 기여 (40%)
    mom_score = min(1.0, (mom20 + mom50) / 0.30)  # 합산 30% 이상이면 만점

    # 눌림목 기여 (30%): 얕은 눌림목일수록 좋음
    pullback_score = 1.0 - (pullback / 0.15) if pullback > 0 else 1.0

    # 거래량 기여 (30%)
    vol_score = min(1.0, vol_ratio / 2.0)

    total = 0.4 * mom_score + 0.3 * pullback_score + 0.3 * vol_score
    return round(max(0.0, min(1.0, total)), 3)


def generate_entry_intents(
    tickers: list[str],
    provider: Any,
    sold_today: set[str],
    available_cash_usd: float,
    position_count: int,
    capital_usd_cap: float,
    now: datetime | None = None,
    max_new_entries: int | None = None,
) -> list[dict]:
    """진입 intent 목록 생성.

    Args:
        tickers: 평가 대상 ticker list
        provider: USDataProvider 인스턴스
        sold_today: 당일 매도 완료 종목 집합 (재매수 차단)
        available_cash_usd: 실제 주문 가능 잔고
        position_count: 현재 보유 포지션 수
        capital_usd_cap: 미국장 예산 USD cap
        now: 현재 시각 (None이면 실시간)
        max_new_entries: tick당 최대 신규 진입 수

    Returns:
        list of order intent dict
    """
    from trader.us.symbols import resolve_exchange
    from trader.us.pb1.us_position_sizing import calc_position_size

    if max_new_entries is None:
        max_new_entries = int(os.getenv("US_MAX_NEW_ENTRIES_PER_TICK", "3"))

    scored: list[tuple[float, str, str, float, list[dict], dict]] = []

    for symbol in tickers:
        # 당일 매도 차단
        if symbol in sold_today:
            logger.debug("[US_ENTRY][BLOCK] reason=sold_today symbol=%s", symbol)
            continue

        # DB: 미체결 주문 차단
        try:
            from trader.us.db.repos import has_pending_order, has_position
            if has_pending_order(symbol):
                logger.debug("[US_ENTRY][BLOCK] reason=pending_order symbol=%s", symbol)
                continue
            # DB: 이미 보유 중이면 차단
            if has_position(symbol):
                logger.debug("[US_ENTRY][BLOCK] reason=has_position symbol=%s", symbol)
                continue
        except Exception as exc:
            logger.debug("[US_ENTRY][WARN] DB check failed symbol=%s: %s", symbol, exc)

        try:
            exchange = resolve_exchange(symbol)
            daily = provider.get_daily_prices(symbol, exchange, count=120)
            current = provider.get_current_price(symbol, exchange)
        except Exception as exc:
            logger.debug("[US_ENTRY][SKIP] symbol=%s error=%s", symbol, exc)
            continue

        try:
            s = score_symbol(symbol, daily, current)
        except Exception as exc:
            logger.debug("[US_ENTRY][SCORE_ERR] symbol=%s error=%s", symbol, exc)
            continue

        if s is None:
            continue

        try:
            price = float(current.get("last", 0))
        except (ValueError, TypeError):
            continue

        if price <= 0:
            continue

        scored.append((s, symbol, exchange, price, daily, current))

    # rank 기반 정렬 (점수 내림차순)
    scored.sort(key=lambda x: x[0], reverse=True)

    intents: list[dict] = []
    added_count = 0

    for rank, (score, symbol, exchange, price, daily, current) in enumerate(scored):
        if added_count >= max_new_entries:
            break

        sizing = calc_position_size(
            price=price,
            available_cash_usd=available_cash_usd,
            capital_usd_cap=capital_usd_cap,
            position_count=position_count + added_count,
            score=score,
        )

        if sizing["blocked"]:
            logger.debug(
                "[US_ENTRY][BLOCK] symbol=%s reason=%s",
                symbol, sizing["reason"],
            )
            continue

        qty = sizing["qty"]
        notional = sizing["notional_usd"]

        limit_price = round(price * (1 + float(os.getenv("US_LIMIT_PRICE_BAND_PCT", "0.005"))), 4)

        import hashlib
        from datetime import date
        today = (now.date() if now else date.today()).strftime("%Y%m%d")
        key_raw = f"{symbol}_{today}_BUY"
        client_order_key = hashlib.sha256(key_raw.encode()).hexdigest()[:24]

        intent = {
            "symbol": symbol,
            "exchange": exchange,
            "side": "BUY",
            "qty": qty,
            "limit_price": limit_price,
            "notional_usd": notional,
            "score": score,
            "rank": rank + 1,
            "client_order_key": client_order_key,
            "strategy": "us_pb1",
            "entry_style": "momentum",
        }
        intents.append(intent)
        added_count += 1

        logger.info(
            "[US_ENTRY][INTENT] symbol=%s rank=%d score=%.3f qty=%d notional=%.2f",
            symbol, rank + 1, score, qty, notional,
        )

    return intents
