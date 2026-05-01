# -*- coding: utf-8 -*-
"""Strategy Agent.

universe/watchlist/strategy scoring 담당.
주문 직접 실행 금지. intent만 생성.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class StrategyAgent:
    """Strategy scoring + intent 생성 Agent."""

    name = "strategy_agent"

    def run(self, offline: bool = True) -> dict:
        """strategy scoring 실행, intent 목록 반환."""
        from trader.us.runner.prep_runner import run_prep
        result = run_prep(offline=offline)
        intents = result.get("intents", [])
        logger.info("[US_STRATEGY_AGENT][OK] intents=%d", len(intents))
        return {"status": result.get("status"), "intents": intents}
