# -*- coding: utf-8 -*-
"""Harness Scenario: insufficient_cash_block.

현금 부족 시 주문 차단 시나리오.
[US_RISK][BLOCK] 마커가 반드시 출력되어야 한다.
"""
from __future__ import annotations

import logging
import os
import sys

logger = logging.getLogger(__name__)


def run() -> int:
    logger.info("[US_HARNESS][SCENARIO] name=insufficient_cash_block")

    from trader.us.execution.risk_gate import check_cash_buffer, RiskGateBlocked

    # 현금 120 USD, 주문 100 USD, 버퍼 50 USD
    # remaining = 120 - 100 = 20 < 50 → BLOCK
    try:
        check_cash_buffer(
            available_cash_usd=120.0,
            order_notional_usd=100.0,
            symbol="NVDA",
        )
        logger.error("[US_HARNESS][FAIL] expected RiskGateBlocked but got none")
        return 1
    except RiskGateBlocked:
        logger.info("[US_HARNESS][PASS] scenario=insufficient_cash_block")
        return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    sys.exit(run())
