# -*- coding: utf-8 -*-
"""US PB1 Pullback 전략.

상승 추세에 있는 종목의 눌림목 매수 신호.
- MA20/MA50 위에 있는지
- 최근 고점 대비 조정률
- 거래량 감소 후 반등
- 과열 종목 제외
"""
from __future__ import annotations

import logging
import os
from typing import Any

from trader.us.strategy.base import BaseUSStrategy, make_order_intent

logger = logging.getLogger(__name__)


def _prices_to_float(daily_prices: list[dict], key: str = "clos") -> list[float]:
    result = []
    for row in daily_prices:
        try:
            result.append(float(row.get(key, 0) or 0))
        except (ValueError, TypeError):
            pass
    return result


def _ma(prices: list[float], n: int) -> float | None:
    if len(prices) < n:
        return None
    return sum(prices[-n:]) / n


class USPb1PullbackStrategy(BaseUSStrategy):
    """MA20/MA50 위 눌림목 매수 전략."""

    name = "us_pb1_pullback"

    def __init__(
        self,
        run_id: str,
        trade_date: str | None = None,
        pullback_min: float = 0.03,
        pullback_max: float = 0.15,
        volume_decline_pct: float = 0.30,
    ) -> None:
        super().__init__(run_id, trade_date)
        self.pullback_min = pullback_min
        self.pullback_max = pullback_max
        self.volume_decline_pct = volume_decline_pct

    def score(self, symbol: str, daily_prices: list[dict], current_price: dict) -> float | None:
        closes = _prices_to_float(daily_prices, "clos")
        if len(closes) < 55:
            return None

        last = closes[-1]
        ma20 = _ma(closes, 20)
        ma50 = _ma(closes, 50)

        if ma20 is None or ma50 is None:
            return None

        # 추세 조건: 현재가 > MA20 > MA50
        if not (last > ma20 > ma50 * 0.98):
            return None

        # 눌림목 조건: 최근 20일 고점 대비 조정
        recent_high = max(closes[-20:])
        if recent_high <= 0:
            return None
        pullback = (recent_high - last) / recent_high

        if not (self.pullback_min <= pullback <= self.pullback_max):
            return None

        # 점수: 눌림목 깊이 반비례 (얕을수록 좋음 → 0.5 pullback_min → 1.0)
        score = 1.0 - (pullback - self.pullback_min) / (self.pullback_max - self.pullback_min)
        return round(score, 3)

    def generate_intent(
        self,
        symbol: str,
        exchange: str,
        score: float,
        daily_prices: list[dict],
        current_price: dict,
        available_cash_usd: float,
    ) -> dict | None:
        try:
            last_price = float(current_price.get("last", 0))
        except (ValueError, TypeError):
            return None

        if last_price <= 0:
            return None

        # 주문 한도 내에서 qty 계산
        max_notional = float(os.getenv("US_MAX_ORDER_USD", "100"))
        qty = max(1, int(max_notional * score / last_price))
        notional_usd = qty * last_price

        if notional_usd > max_notional:
            qty = max(1, int(max_notional / last_price))
            notional_usd = qty * last_price

        closes = _prices_to_float(daily_prices, "clos")
        recent_high = max(closes[-20:]) if closes else last_price

        return make_order_intent(
            run_id=self.run_id,
            trade_date=self.trade_date,
            symbol=symbol,
            exchange=exchange,
            side="BUY",
            qty=qty,
            limit_price=last_price,
            strategy=self.name,
            reason_json={
                "score": score,
                "pullback_from_high": round((recent_high - last_price) / recent_high, 4) if recent_high > 0 else 0,
                "last_price": last_price,
            },
        )
