# -*- coding: utf-8 -*-
"""Harness Scenario: partial_fill_reconcile.

부분 체결 reconcile 처리 시나리오.
"""
from __future__ import annotations

import logging
import sys

logger = logging.getLogger(__name__)


def run() -> int:
    logger.info("[US_HARNESS][SCENARIO] name=partial_fill_reconcile")

    from trader.us.data_provider import USDataProvider
    from trader.us.execution.reconcile import reconcile_positions

    provider = USDataProvider(offline=True)
    result = reconcile_positions(provider=provider)

    if result["status"] == "OK":
        logger.info("[US_HARNESS][PASS] scenario=partial_fill_reconcile")
        return 0
    else:
        logger.error("[US_HARNESS][FAIL] reconcile status=%s", result.get("status"))
        return 1


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    sys.exit(run())
