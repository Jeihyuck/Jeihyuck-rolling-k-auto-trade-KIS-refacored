"""CLI tool to build and save PB1 watchlist."""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import create_engine

from trader.config import RS_BENCHMARK_KOSPI, RS_BENCHMARK_KOSDAQ, RS_LOOKBACK_DAYS, RS_LOOKBACK2_DAYS, RS_MIN_PCTILE
from trader.data.ohlcv_provider import ChainOHLCVProvider, KISOHLCVProvider, KRXOHLCVProvider
from trader.db.repos import UniverseRepo
from trader.kis_wrapper import KisAPI
from trader.time_utils import now_kst
from trader.watchlist_builder import build_and_save_watchlist

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Build and save PB1 watchlist")
    parser.add_argument("--env", type=str, default="live", help="Environment (live/paper)")
    parser.add_argument("--strategy", type=str, default="best_k_meta", help="Strategy name")
    parser.add_argument("--as-of", type=str, help="As-of date (YYYY-MM-DD), default=today KST")
    parser.add_argument("--force", action="store_true", help="Force rebuild even if already exists")
    
    args = parser.parse_args()
    
    # Parse as_of date
    if args.as_of:
        as_of = date.fromisoformat(args.as_of)
    else:
        as_of = now_kst().date()
    
    logger.info("[WATCHLIST][CLI] env=%s strategy=%s as_of=%s force=%s", 
                args.env, args.strategy, as_of, args.force)
    
    # Database connection
    db_url = os.getenv("PBCORE_DB_URL", "postgresql+psycopg://")
    engine = create_engine(db_url, pool_pre_ping=True)
    
    # Load universe
    universe_repo = UniverseRepo(engine)
    members = universe_repo.get_current_universe_members(args.env, args.strategy)
    logger.info("[WATCHLIST][CLI] universe loaded: %s members", len(members))
    
    if not members:
        logger.error("[WATCHLIST][CLI] no universe members found")
        sys.exit(1)
    
    # Setup OHLCV provider
    kis = KisAPI(kis_env=args.env)
    kis_provider = KISOHLCVProvider(kis=kis)
    krx_provider = KRXOHLCVProvider()
    ohlcv_provider_chain = ChainOHLCVProvider([kis_provider, krx_provider])
    
    def fetch_daily(code: str, count: int = 200):
        """OHLCV provider wrapper."""
        try:
            df = ohlcv_provider_chain.fetch(code, count=count)
            meta = {"source": "chain"}
            return df, meta
        except Exception as exc:
            logger.warning("[OHLCV][FAIL] code=%s err=%s", code, exc)
            import pandas as pd
            return pd.DataFrame(), {}
    
    # Minervini config
    minervini_config = {
        "rs_benchmark_by_market": {"KOSPI": RS_BENCHMARK_KOSPI, "KOSDAQ": RS_BENCHMARK_KOSDAQ},
        "rs_lookback": RS_LOOKBACK_DAYS,
        "rs_lookback2": RS_LOOKBACK2_DAYS,
        "rs_min_pctile": RS_MIN_PCTILE,
        "vcp_lookback": 120,
        "vcp_min_score": 70.0,
    }
    
    # Build and save watchlist
    try:
        watchlist = build_and_save_watchlist(
            engine=engine,
            env=args.env,
            strategy=args.strategy,
            as_of=as_of,
            members=members,
            ohlcv_provider=fetch_daily,
            minervini_config=minervini_config,
            force_rebuild=args.force,
        )
    except Exception as exc:
        logger.error("[WATCHLIST][CLI][BUILD_FAIL] err=%s", exc, exc_info=True)
        sys.exit(1)
    
    # Print top 10
    logger.info("[WATCHLIST][CLI] Watchlist built successfully: %s members", len(watchlist))
    logger.info("[WATCHLIST][CLI] Top 10:")
    for i, item in enumerate(watchlist[:10], start=1):
        logger.info("  %2d. %s (score=%.2f)", i, item["code"], item.get("score") or 0.0)
    
    logger.info("[WATCHLIST][CLI] Done!")


if __name__ == "__main__":
    main()
