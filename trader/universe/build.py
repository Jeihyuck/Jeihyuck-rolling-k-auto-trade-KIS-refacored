from __future__ import annotations

import argparse
import logging
from datetime import datetime

from rolling_k_auto_trade_api.best_k_meta_strategy import run_rebalance

from trader.db.engine import make_engine
from trader.db.migrate import run_migrations
from trader.db.repos import UniverseRepo
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and persist universe members")
    parser.add_argument("--env", required=True, help="Environment (practice/real)")
    parser.add_argument("--strategy", required=True, help="Strategy name to store universe for")
    parser.add_argument("--date", help="As-of date (YYYY-MM-DD). Defaults to today.")
    return parser.parse_args()


def build_universe(as_of_date: str, env: str, strategy: str) -> str:
    engine = make_engine()
    run_migrations(engine)
    repo = UniverseRepo(engine)

    payload = run_rebalance(as_of_date, return_by_market=True)
    members = []
    selected_by_market = payload.get("selected_by_market") or {}
    for market, rows in selected_by_market.items():
        for idx, row in enumerate(rows or []):
            code = str(row.get("code") or row.get("pdno") or "").zfill(6)
            members.append(
                {
                    "code": code,
                    "market": market,
                    "weight": row.get("weight") or row.get("target_weight") or row.get("weight_pct"),
                    "rank": idx + 1,
                    "meta_json": row,
                }
            )
    universe_id = repo.store_universe(
        env=env,
        strategy=strategy,
        as_of_date=as_of_date,
        source="best_k_meta_strategy",
        params_json={"as_of": as_of_date},
        payload_json=payload,
        members=members,
    )
    logger.info("[UNIVERSE][BUILT] env=%s strategy=%s as_of=%s universe_id=%s members=%s", env, strategy, as_of_date, universe_id, len(members))
    return universe_id


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    as_of = args.date or now_kst().date().isoformat()
    build_universe(as_of_date=as_of, env=args.env, strategy=args.strategy)


if __name__ == "__main__":
    main()
