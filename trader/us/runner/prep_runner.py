# -*- coding: utf-8 -*-
"""US Prep Runner.

- universe 로드
- price/daily data 로드
- strategy scoring
- watchlist 저장
- 주문 없음
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime

logger = logging.getLogger(__name__)


def run_prep(env: str = "practice", offline: bool = False) -> dict:
    """Prep 단계 실행.

    Returns:
        {"status": "OK"|"ERROR", "intents": [...], ...}
    """
    logger.info("[US_PREP][START] env=%s offline=%s", env, offline)

    # 1. Universe 로드
    try:
        from trader.us.universe import load_universe, get_all_tickers
        universe = load_universe(force=True)
        tickers = get_all_tickers()
        logger.info("[US_PREP][UNIVERSE] loaded %d tickers", len(tickers))
    except Exception as exc:
        logger.error("[US_PREP][ERROR] universe load failed: %s", exc)
        return {"status": "ERROR", "stage": "universe", "error": str(exc)}

    # 2. Data Provider
    from trader.us.data_provider import USDataProvider
    provider = USDataProvider(offline=offline)

    # 3. Strategy scoring
    from trader.us.strategy.us_pb1_pullback import USPb1PullbackStrategy
    from trader.us.strategy.us_momentum import USMomentumStrategy
    from trader.us.strategy.us_etf_trend import USEtfTrendStrategy

    run_id = f"prep-{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
    trade_date = datetime.utcnow().strftime("%Y-%m-%d")

    all_intents = []
    for StratCls in [USPb1PullbackStrategy, USMomentumStrategy, USEtfTrendStrategy]:
        strat = StratCls(run_id=run_id, trade_date=trade_date)
        try:
            intents = strat.run_on_universe(tickers, provider)
            all_intents.extend(intents)
            logger.info("[US_STRATEGY][SCORED] strategy=%s intents=%d",
                        strat.name, len(intents))
        except Exception as exc:
            logger.warning("[US_PREP][STRATEGY_ERROR] strategy=%s error=%s",
                           getattr(strat, "name", "?"), exc)

    logger.info("[US_PREP][OK] total_intents=%d", len(all_intents))
    return {
        "status": "OK",
        "run_id": run_id,
        "trade_date": trade_date,
        "tickers_loaded": len(tickers),
        "intents": all_intents,
    }


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="US Prep Runner")
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()

    result = run_prep(env=args.env, offline=args.offline)
    if result["status"] != "OK":
        sys.exit(1)


if __name__ == "__main__":
    main()
