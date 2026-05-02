# -*- coding: utf-8 -*-
"""미국주식 Strategy 기반 클래스.

전략 출력은 직접 주문이 아니라 OrderIntent(dict)여야 한다.
"""
from __future__ import annotations

import hashlib
import json
import logging
from abc import ABC, abstractmethod
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)


def make_client_order_key(trade_date: str, symbol: str, side: str, strategy: str, seq: int = 0) -> str:
    """결정론적 client_order_key 생성."""
    raw = f"{trade_date}:{symbol}:{side}:{strategy}:{seq}"
    return "US-" + hashlib.sha1(raw.encode()).hexdigest()[:16]


def make_order_intent(
    *,
    run_id: str,
    trade_date: str,
    symbol: str,
    exchange: str,
    side: str,
    qty: int,
    limit_price: float,
    strategy: str,
    reason_json: dict | None = None,
    risk_snapshot_json: dict | None = None,
    seq: int = 0,
) -> dict:
    """표준 OrderIntent dict 생성.

    Returns:
        {
          "run_id": ...,
          "trade_date": ...,
          "symbol": ...,
          "exchange": ...,
          "side": "BUY|SELL",
          "qty": int,
          "limit_price": float,
          "notional_usd": float,
          "strategy": ...,
          "reason_json": {},
          "risk_snapshot_json": {},
          "client_order_key": "...",
        }
    """
    notional_usd = round(qty * limit_price, 4)
    key = make_client_order_key(trade_date, symbol, side, strategy, seq)
    return {
        "run_id": run_id,
        "trade_date": trade_date,
        "symbol": symbol,
        "exchange": exchange,
        "side": side,
        "qty": qty,
        "limit_price": round(limit_price, 4),
        "notional_usd": notional_usd,
        "strategy": strategy,
        "reason_json": reason_json or {},
        "risk_snapshot_json": risk_snapshot_json or {},
        "client_order_key": key,
    }


class BaseUSStrategy(ABC):
    """미국주식 전략 기반 클래스."""

    name: str = "base"

    def __init__(self, run_id: str, trade_date: str | None = None) -> None:
        self.run_id = run_id
        self.trade_date = trade_date or str(date.today())

    @abstractmethod
    def score(self, symbol: str, daily_prices: list[dict], current_price: dict) -> float | None:
        """symbol의 신호 점수 계산.

        Returns:
            float: 신호 강도 (0~1). None이면 해당 종목 건너뜀.
        """

    @abstractmethod
    def generate_intent(
        self,
        symbol: str,
        exchange: str,
        score: float,
        daily_prices: list[dict],
        current_price: dict,
        available_cash_usd: float,
    ) -> dict | None:
        """OrderIntent 생성.

        Returns:
            OrderIntent dict 또는 None (신호 없음)
        """

    def run_on_universe(
        self,
        symbols: list[str],
        data_provider: Any,
    ) -> list[dict]:
        """전체 universe에 대해 scoring + intent 생성.

        Returns:
            OrderIntent 목록
        """
        from trader.us.symbols import resolve_exchange, is_known_symbol

        intents: list[dict] = []
        cash = data_provider.get_orderable_cash()

        for symbol in symbols:
            try:
                if not is_known_symbol(symbol):
                    continue
                exchange = resolve_exchange(symbol)
                daily = data_provider.get_daily_prices(symbol, exchange)
                current = data_provider.get_current_price(symbol, exchange)
                score = self.score(symbol, daily, current)
                if score is None or score <= 0:
                    continue
                intent = self.generate_intent(symbol, exchange, score, daily, current, cash)
                if intent:
                    intents.append(intent)
                    logger.info(
                        "[US_STRATEGY][SCORED] strategy=%s symbol=%s score=%.3f",
                        self.name, symbol, score,
                    )
            except Exception as exc:
                logger.warning("[US_STRATEGY][SKIP] symbol=%s error=%s", symbol, exc)

        return intents
