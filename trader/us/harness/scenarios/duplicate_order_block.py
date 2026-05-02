# -*- coding: utf-8 -*-
"""Harness Scenario: duplicate_order_block.

동일 client_order_key 중복 주문 차단 시나리오.
[US_DUPLICATE][BLOCK] 마커가 반드시 출력되어야 한다.
"""
from __future__ import annotations

import logging
import os
import sys

logger = logging.getLogger(__name__)


def run() -> int:
    logger.info("[US_HARNESS][SCENARIO] name=duplicate_order_block")

    from trader.us.execution.risk_gate import check_duplicate, RiskGateBlocked

    existing_keys = {"test-dup-key-scenario-001"}
    try:
        check_duplicate("test-dup-key-scenario-001", existing_keys)
        # 여기까지 오면 차단이 안 된 것 → FAIL
        logger.error("[US_HARNESS][FAIL] expected RiskGateBlocked but got none")
        return 1
    except RiskGateBlocked:
        # 정상: 차단됨 → [US_DUPLICATE][BLOCK] 이미 로깅됨
        logger.info("[US_HARNESS][PASS] scenario=duplicate_order_block")
        return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    sys.exit(run())
