# -*- coding: utf-8 -*-
"""US Trade Mid Runner.

- position 점검
- 신규 매수 제한 (mid session)
- 손절/위험 조건만 판단
"""
from __future__ import annotations

import argparse
import logging
import sys

logger = logging.getLogger(__name__)


def run_trade_mid(env: str = "practice", offline: bool = False) -> dict:
    logger.info("[US_TRADE_MID][START] env=%s offline=%s", env, offline)

    from trader.us.market_calendar import market_phase, now_ny
    phase = market_phase(now_ny())
    if phase not in ("REGULAR_MID", "REGULAR_CLOSE"):
        logger.info("[US_MARKET][CLOSED] phase=%s", phase)
        logger.info("[US_TRADE_MID][SKIP] not in mid session")
        return {"status": "SKIP", "reason": "not_mid_session", "phase": phase}

    from trader.us.data_provider import USDataProvider
    provider = USDataProvider(offline=offline)

    # mid runner: 포지션 점검만 (신규 매수 없음)
    balance = provider.get_balance()
    positions = balance.get("positions", [])
    logger.info("[US_TRADE_MID][POSITIONS] count=%d", len(positions))

    # TODO: 실제 손절 로직 연결 (Phase 4 확장)
    logger.info("[US_TRADE_MID][OK] positions_checked=%d", len(positions))
    return {"status": "OK", "positions_checked": len(positions)}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="US Trade Mid Runner")
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    result = run_trade_mid(env=args.env, offline=args.offline)
    if result["status"] == "ERROR":
        sys.exit(1)


if __name__ == "__main__":
    main()
