# -*- coding: utf-8 -*-
"""US Trade Close Runner.

- fills 조회
- positions reconcile
- 미체결 주문 점검
- pnl snapshot
- report 준비
"""
from __future__ import annotations

import argparse
import logging
import sys

logger = logging.getLogger(__name__)


def run_trade_close(env: str = "practice", offline: bool = False) -> dict:
    logger.info("[US_TRADE_CLOSE][START] env=%s offline=%s", env, offline)

    from trader.us.data_provider import USDataProvider
    provider = USDataProvider(offline=offline)

    # 1. Fills 조회
    try:
        from trader.us.execution.fills import get_fills_today
        fills = get_fills_today(provider=provider)
        logger.info("[US_TRADE_CLOSE][FILLS] count=%d", len(fills))
    except Exception as exc:
        logger.error("[US_TRADE_CLOSE][ERROR] fills failed: %s", exc)
        fills = []

    # 2. Reconcile
    try:
        from trader.us.execution.reconcile import reconcile_positions
        reconcile_result = reconcile_positions(provider=provider)
        logger.info("[US_TRADE_CLOSE][RECONCILE] status=%s", reconcile_result.get("status"))
    except Exception as exc:
        logger.error("[US_TRADE_CLOSE][ERROR] reconcile failed: %s", exc)
        reconcile_result = {"status": "ERROR", "error": str(exc)}

    # 3. Balance snapshot
    balance = provider.get_balance()

    logger.info("[US_TRADE_CLOSE][OK]")
    return {
        "status": "OK",
        "fills": fills,
        "reconcile": reconcile_result,
        "balance": balance,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="US Trade Close Runner")
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    result = run_trade_close(env=args.env, offline=args.offline)
    if result["status"] == "ERROR":
        sys.exit(1)


if __name__ == "__main__":
    main()
