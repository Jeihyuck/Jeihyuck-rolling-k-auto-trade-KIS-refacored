"""Offline market-state candidate backfill and crash fail-closed scenario."""
from __future__ import annotations

import logging
import sys

logger = logging.getLogger(__name__)


def run() -> int:
    from trader.us.market_state_overlay import filter_watchlist_rows_for_market_state

    rows = [
        {"symbol": "DDOG", "theme_cluster": "AI_SOFTWARE", "score_final": .9, "trend_score": 1, "rank_final30": 1},
        {"symbol": "SNOW", "theme_cluster": "AI_SOFTWARE", "score_final": .8, "trend_score": 1, "rank_final30": 2},
        {"symbol": "MPC", "theme_cluster": "ENERGY_MATERIALS", "score_final": .6063, "trend_score": 1, "rank_final30": 3},
        {"symbol": "KO", "theme_cluster": "CONSUMER_STAPLES", "score_final": .55, "trend_score": .7, "rank_final30": 4},
        {"symbol": "AMGN", "theme_cluster": "HEALTHCARE", "score_final": .5, "trend_score": .7, "rank_final30": 5},
    ]
    risk_off = {"market_state": "DEFENSE_RISK_OFF", "market_regime": "DEFENSIVE", "allow_new_buy": True, "allow_ai_tech_buy": False}
    eligible, blocked = filter_watchlist_rows_for_market_state(rows, risk_off)
    crash_eligible, _ = filter_watchlist_rows_for_market_state(rows, {**risk_off, "market_state": "DEFENSE_CRASH_CONFIRMED"})
    if [row["symbol"] for row in eligible[:3]] != ["MPC", "KO", "AMGN"] or len(blocked) != 2 or crash_eligible:
        logger.error("[US_HARNESS][FAIL] risk_off=%s blocked=%s crash=%s", eligible, blocked, crash_eligible)
        return 1
    logger.info("[US_BACKFILL][OK] accepted=MPC,KO,AMGN blocked=DDOG,SNOW crash_fail_closed=1")
    logger.info("[US_HARNESS][PASS] scenario=market_state_backfill")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    sys.exit(run())
