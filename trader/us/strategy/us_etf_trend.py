# -*- coding: utf-8 -*-
"""US ETF Trend 전략.

- SPY/QQQ/SMH/SOXX 트렌드 분석
- 미국장 risk-on/risk-off 판단
- 개별주 매수 강도 조절 (ETF 트렌드가 약하면 개별주 매수 억제)
"""
from __future__ import annotations

import logging
import os
from typing import Any

from trader.us.strategy.base import BaseUSStrategy, make_order_intent

logger = logging.getLogger(__name__)

_BENCHMARK_ETFS = ["SPY", "QQQ", "SMH", "SOXX"]


def _prices_to_float(daily_prices: list[dict], key: str = "clos") -> list[float]:
    result = []
    for row in daily_prices:
        try:
            result.append(float(row.get(key, 0) or 0))
        except (ValueError, TypeError):
            pass
    return result


def _compute_etf_trend_score(closes: list[float]) -> float:
    """ETF closes를 기반으로 trend score 계산 (0~1)."""
    if len(closes) < 20:
        return 0.5
    last = closes[-1]
    ma20 = sum(closes[-20:]) / 20
    ma50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else ma20
    # 정규장 트렌드: 현재가가 MA20, MA50 위에 있으면 강한 트렌드
    above_ma20 = 1.0 if last > ma20 else 0.0
    above_ma50 = 1.0 if last > ma50 else 0.0
    m10 = (last / closes[-11] - 1) if len(closes) >= 11 else 0.0
    momentum = min(1.0, max(0.0, (m10 + 0.05) / 0.10))
    return round((above_ma20 * 0.3 + above_ma50 * 0.3 + momentum * 0.4), 3)


class USEtfTrendStrategy(BaseUSStrategy):
    """ETF 트렌드 기반 매수 강도 조절 전략.

    ETF 트렌드가 약하면 개별주 신호를 억제한다.
    """

    name = "us_etf_trend"

    def __init__(
        self,
        run_id: str,
        trade_date: str | None = None,
        market_trend_min: float = 0.4,
    ) -> None:
        super().__init__(run_id, trade_date)
        self.market_trend_min = market_trend_min
        self._market_trend_score: float | None = None

    def compute_market_trend(self, data_provider: Any) -> float:
        """SPY/QQQ 트렌드 점수 계산 (0~1)."""
        from trader.us.symbols import resolve_exchange
        scores = []
        for etf in ["SPY", "QQQ"]:
            try:
                exchange = resolve_exchange(etf)
                daily = data_provider.get_daily_prices(etf, exchange, 60)
                closes = _prices_to_float(daily, "clos")
                scores.append(_compute_etf_trend_score(closes))
            except Exception as exc:
                logger.warning("[US_ETF_TREND][SKIP] etf=%s error=%s", etf, exc)
        if not scores:
            return 0.5
        result = sum(scores) / len(scores)
        logger.info("[US_ETF_TREND][MARKET_SCORE] score=%.3f", result)
        return result

    def score(self, symbol: str, daily_prices: list[dict], current_price: dict) -> float | None:
        closes = _prices_to_float(daily_prices, "clos")
        if not closes:
            return None

        etf_score = _compute_etf_trend_score(closes)

        # ETF 트렌드가 약하면 신호 없음
        if self._market_trend_score is not None:
            if self._market_trend_score < self.market_trend_min:
                return None

        return round(etf_score, 3)

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
        if qty * last_price > max_notional:
            qty = max(1, int(max_notional / last_price))

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
                "market_trend": self._market_trend_score,
                "last_price": last_price,
            },
        )

    def run_on_universe(self, symbols: list[str], data_provider: Any) -> list[dict]:
        """ETF 트렌드를 먼저 계산하고 개별주 신호를 생성한다."""
        self._market_trend_score = self.compute_market_trend(data_provider)
        if self._market_trend_score < self.market_trend_min:
            logger.info(
                "[US_ETF_TREND][RISK_OFF] market_trend=%.3f < min=%.3f, skipping individual signals",
                self._market_trend_score,
                self.market_trend_min,
            )
            return []
        return super().run_on_universe(symbols, data_provider)
