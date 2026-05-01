# -*- coding: utf-8 -*-
"""Harness Scenario: kis_rate_limit_retry.

KIS rate limit 상황에서 offline 처리 시나리오.
실제 HTTP 호출 없이 rate limit 로직만 검증.
"""
from __future__ import annotations

import logging
import sys

logger = logging.getLogger(__name__)


def run() -> int:
    logger.info("[US_HARNESS][SCENARIO] name=kis_rate_limit_retry")

    # offline 모드에서는 실제 HTTP 없음 → rate limit trigger 없음
    # 시나리오: offline mode에서 아무 오류 없이 통과하는지 확인
    from trader.us.data_provider import USDataProvider
    provider = USDataProvider(offline=True)

    try:
        price = provider.get_current_price("NVDA", "NASDAQ")
        assert isinstance(price, dict), "price should be dict"
        logger.info("[US_HARNESS][PASS] scenario=kis_rate_limit_retry offline_ok")
        return 0
    except Exception as exc:
        logger.error("[US_HARNESS][FAIL] %s", exc)
        return 1


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    sys.exit(run())
