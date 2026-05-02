# -*- coding: utf-8 -*-
"""US Momentum 전략.

- 20일/60일 모멘텀
- 52주 고점 근접도
- 변동성 조정 수익률
- 상대강도
"""
from __future__ import annotations

import logging
import math
import os

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


def _std(prices: list[float]) -> float:
    if len(prices) < 2:
        return 1.0
    mean = sum(prices) / len(prices)
    variance = sum((x - mean) ** 2 for x in prices) / len(prices)
    return math.sqrt(variance) or 1.0


class USMomentumStrategy(BaseUSStrategy):
    """단순 모멘텀 전략."""

    name = "us_momentum"

    def __init__(self, run_id: str, trade_date: str | None = None,
                 momentum_threshold: float = 0.05) -> None:
        super().__init__(run_id, trade_date)
        self.momentum_threshold = momentum_threshold

    def score(self, symbol: str, daily_prices: list[dict], current_price: dict) -> float | None:
        closes = _prices_to_float(daily_prices, "clos")
        if len(closes) < 63:
            return None

        last = closes[-1]
        m20 = (last / closes[-21] - 1) if len(closes) >= 21 else None
        m60 = (last / closes[-61] - 1) if len(closes) >= 61 else None

        if m20 is None or m60 is None:
            return None

        # 양방향 모멘텀 조건
        if m20 < self.momentum_threshold or m60 < self.momentum_threshold:
            return None

        # 52주 고점 근접도
        high_52w = max(closes[-252:]) if len(closes) >= 252 else max(closes)
        proximity = last / high_52w if high_52w > 0 else 0

        # 변동성 조정 수익률
        vol = _std(closes[-20:])
        risk_adj = (m20 / vol) if vol > 0 else 0

        # 종합 점수 (0~1)
        score = min(1.0, max(0.0, (proximity * 0.4 + min(risk_adj, 2.0) / 2.0 * 0.6)))
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

        max_notional = float(os.getenv("US_MAX_ORDER_USD", "100"))
        qty = max(1, int(max_notional * score / last_price))
        notional_usd = qty * last_price
        if notional_usd > max_notional:
            qty = max(1, int(max_notional / last_price))

        closes = _prices_to_float(daily_prices, "clos")
        m20 = round((last_price / closes[-21] - 1), 4) if len(closes) >= 21 else 0

        return make_order_intent(
            run_id=self.run_id,
            trade_date=self.trade_date,
            symbol=symbol,
            exchange=exchange,
            side="BUY",
            qty=qty,
            limit_price=last_price,
            strategy=self.name,
            reason_json={"score": score, "momentum_20d": m20, "last_price": last_price},
        )
