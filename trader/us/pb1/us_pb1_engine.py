# -*- coding: utf-8 -*-
"""US PB1 Engine.

tick runner에서 호출하는 통합 엔진.

환경변수:
  US_STRATEGY_ENGINE=pb1  → 이 엔진 사용
  US_ENTRY_ENABLED=1      → 진입 평가
  US_EXIT_ENABLED=1       → 청산 평가
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


class USPb1Engine:
    """US PB1 통합 전략 엔진."""

    def __init__(self, env: str = "practice", offline: bool = False) -> None:
        self.env = env
        self.offline = offline

    def evaluate_exits(
        self,
        positions: list[dict],
        provider: Any,
        now: datetime | None = None,
    ) -> list[dict]:
        """보유 포지션 청산 조건 평가.

        Returns:
            청산 order intent 목록
        """
        from trader.utils.env import env_bool
        if not env_bool("US_EXIT_ENABLED", default=True):
            logger.info("[US_PB1_ENGINE] exit evaluation disabled")
            return []

        from trader.us.pb1.us_exit_engine import generate_exit_intents
        return generate_exit_intents(positions=positions, provider=provider, now=now)

    def evaluate_entries(
        self,
        tickers: list[str] | list[dict] | None,
        provider: Any,
        sold_today: set,
        available_cash_usd: float,
        position_count: int,
        now: datetime | None = None,
        watchlist_entries: list[dict] | None = None,
        current_position_symbols: set[str] | None = None,
    ) -> list[dict]:
        """진입 후보 평가.

        Args:
            tickers: symbol list 또는 dict list
            provider: USDataProvider 인스턴스
            sold_today: 당일 매도 완료 종목 집합
            available_cash_usd: 주문 가능 잔고
            position_count: 현재 보유 포지션 수
            now: 현재 시각
            watchlist_entries: locked watchlist rows (authoritative input)
            current_position_symbols: KIS balance 기반 보유 종목 집합

        Returns:
            진입 order intent 목록
        """
        from trader.utils.env import env_bool
        if not env_bool("US_ENTRY_ENABLED", default=True):
            logger.info("[US_PB1_ENGINE] entry evaluation disabled")
            return []

        # tickers 정규화: list[dict]가 들어올 경우 warning
        if tickers and isinstance(tickers, list) and len(tickers) > 0:
            if isinstance(tickers[0], dict):
                logger.warning(
                    "[US_PB1_ENGINE][ENTRY_INPUT] tickers contains dict, normalizing to symbols"
                )
        
        logger.info(
            "[US_PB1_ENGINE][ENTRY_INPUT] tickers=%s watchlist_entries=%s source=%s",
            len(tickers) if tickers else 0,
            len(watchlist_entries) if watchlist_entries else 0,
            "locked_watchlist" if watchlist_entries else "tickers"
        )

        from trader.us.budget import get_us_capital_usd_cap
        capital_usd_cap = get_us_capital_usd_cap()

        from trader.us.pb1.us_entry_engine import generate_entry_intents
        return generate_entry_intents(
            tickers=tickers,
            provider=provider,
            sold_today=sold_today,
            available_cash_usd=available_cash_usd,
            position_count=position_count,
            capital_usd_cap=capital_usd_cap,
            now=now,
            watchlist_entries=watchlist_entries,
            current_position_symbols=current_position_symbols,
        )
