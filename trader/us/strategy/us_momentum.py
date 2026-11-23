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
    """Daily close-to-close *return* volatility, dimensionless.

    Unlike the original price-dollar standard deviation this is invariant
    to nominal price scale (including mechanically equivalent stock splits).
    The caller must provide 21 completed, consistently adjusted closes for
    20 observations. Zero volatility is 0, never an arbitrary $1 divisor.
    """
    if len(prices) < 21:
        return 0.0
    window = prices[-21:]
    if any(not math.isfinite(p) or p <= 0 for p in window):
        return 0.0
    returns = [window[i] / window[i - 1] - 1.0 for i in range(1, len(window))]
    if any(not math.isfinite(r) for r in returns):
        return 0.0
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / len(returns)
    return math.sqrt(variance)


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

        # Reject invalid or inconsistently populated completed-close series.
        # Do not manufacture eligible momentum from a zero price, nan or inf.
        relevant = closes[-252:]
        if any(not math.isfinite(price) or price <= 0 for price in relevant):
            return None
        last = closes[-1]
        m20 = last / closes[-21] - 1
        m60 = last / closes[-61] - 1

        if not math.isfinite(m20) or not math.isfinite(m60):
            return None

        # 양방향 모멘텀 조건
        if m20 < self.momentum_threshold or m60 < self.momentum_threshold:
            return None

        # 52주 고점 근접도
        high_52w = max(closes[-252:]) if len(closes) >= 252 else max(closes)
        proximity = last / high_52w if high_52w > 0 else 0

        # 20 completed daily returns, compared to a 20-day return.
        # Unitless: both numerator and denominator are return fractions.
        # This is NOT an annualised Sharpe ratio. Keep the existing 0..2
        # saturation and weights until historical candidate replay verifies
        # whether re-calibration should be a separately approved policy.
        vol_daily = _std(closes[-21:])
        vol_20d = vol_daily * math.sqrt(20.0)
        risk_adj = (m20 / vol_20d) if vol_20d > 1e-12 else 0.0

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
