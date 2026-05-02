# -*- coding: utf-8 -*-
"""Risk Agent.

check_env_flags / position limit / cash buffer / real trading block 검사.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class RiskAgent:
    """Risk check Agent."""

    name = "risk_agent"

    def check(
        self,
        intent: dict,
        current_position_count: int = 0,
        total_portfolio_usd: float = 1000.0,
        available_cash_usd: float = 1000.0,
        current_daily_notional_usd: float = 0.0,
    ) -> dict:
        """단일 intent에 대해 risk gate 통과 여부 반환."""
        from trader.us.execution.risk_gate import assert_order_allowed, RiskGateBlocked

        try:
            assert_order_allowed(
                intent,
                current_daily_notional_usd=current_daily_notional_usd,
                current_position_count=current_position_count,
                total_portfolio_usd=total_portfolio_usd,
                available_cash_usd=available_cash_usd,
            )
            return {"status": "PASS", "symbol": intent.get("symbol")}
        except RiskGateBlocked as exc:
            logger.warning("[US_RISK_AGENT][BLOCK] %s", exc)
            return {"status": "BLOCK", "reason": str(exc), "symbol": intent.get("symbol")}

    def check_all(
        self,
        intents: list[dict],
        total_portfolio_usd: float = 1000.0,
        available_cash_usd: float = 1000.0,
    ) -> list[dict]:
        """intent 목록 전체를 검사."""
        results = []
        position_count = 0
        daily_notional = 0.0
        for intent in intents:
            result = self.check(
                intent,
                current_position_count=position_count,
                total_portfolio_usd=total_portfolio_usd,
                available_cash_usd=available_cash_usd,
                current_daily_notional_usd=daily_notional,
            )
            results.append(result)
            if result["status"] == "PASS" and intent.get("side") == "BUY":
                position_count += 1
                daily_notional += float(intent.get("notional_usd", 0))
        return results
