# -*- coding: utf-8 -*-
"""Execution Agent.

order intent 실행. risk gate 통과 필수. KIS paper order 호출.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class ExecutionAgent:
    """Order 실행 Agent."""

    name = "execution_agent"

    def execute_intents(
        self,
        intents: list[dict],
        offline: bool = True,
        available_cash_usd: float = 1000.0,
    ) -> list[dict]:
        """intent 목록을 실행하고 결과 반환."""
        from trader.us.execution.order_router import route_order
        results = []
        daily_notional = 0.0
        position_count = 0

        for intent in intents:
            result = route_order(
                intent,
                current_daily_notional_usd=daily_notional,
                current_position_count=position_count,
                total_portfolio_usd=max(available_cash_usd, 1000.0),
                available_cash_usd=available_cash_usd,
            )
            results.append(result)
            if result["status"] in ("DRY_RUN", "ACK"):
                daily_notional += float(intent.get("notional_usd", 0))
                if intent.get("side") == "BUY":
                    position_count += 1

        logger.info("[US_EXECUTION_AGENT][OK] executed=%d", len(results))
        return results
