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


def run_prep(env: str = "practice", offline: bool = False, force_now: str | None = None) -> dict:
    """Prep 단계 실행.

    Returns:
        {"status": "OK"|"ERROR", "intents": [...], ...}
    """
    logger.info("[US_PREP][START] env=%s offline=%s", env, offline)
    logger.info(
        "[US_PREP][FORCE_NOW] enabled=%s force_now=%s",
        1 if force_now else 0,
        force_now or "",
    )

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

    from zoneinfo import ZoneInfo
    _NY_TZ = ZoneInfo("America/New_York")
    if force_now:
        _now = datetime.fromisoformat(force_now).astimezone(_NY_TZ)
    else:
        _now = datetime.now(tz=_NY_TZ)
    run_id = f"prep-{_now.strftime('%Y%m%dT%H%M%S')}"
    trade_date = _now.strftime("%Y-%m-%d")

    all_intents = []
    watchlist_entries: list[dict] = []

    for StratCls in [USPb1PullbackStrategy, USMomentumStrategy, USEtfTrendStrategy]:
        strat = StratCls(run_id=run_id, trade_date=trade_date)
        try:
            intents = strat.run_on_universe(tickers, provider)
            all_intents.extend(intents)
            # watchlist 항목 구성
            for intent in intents:
                watchlist_entries.append({
                    "symbol": intent.get("symbol"),
                    "exchange": intent.get("exchange", "NASDAQ"),
                    "strategy": strat.name,
                    "score": float(intent.get("score", 0)),
                    "meta": {"run_id": run_id},
                })
            logger.info("[US_STRATEGY][SCORED] strategy=%s intents=%d",
                        strat.name, len(intents))
        except Exception as exc:
            logger.warning("[US_PREP][STRATEGY_ERROR] strategy=%s error=%s",
                           getattr(strat, "name", "?"), exc)

    # US PB1 engine scoring (US_STRATEGY_ENGINE=pb1)
    engine_name = os.getenv("US_STRATEGY_ENGINE", "").lower()
    if engine_name == "pb1":
        try:
            from trader.us.pb1.us_entry_engine import score_symbol
            watchlist_size = int(os.getenv("US_PREP_WATCHLIST_SIZE", "30"))
            scored: list[tuple[float, str]] = []
            for ticker in tickers:
                sym = ticker if isinstance(ticker, str) else ticker.get("symbol", "")
                exch = "NASDAQ" if isinstance(ticker, str) else ticker.get("exchange", "NASDAQ")
                try:
                    daily = provider.get_daily_prices(sym, exch)
                    current = provider.get_current_price(sym, exch)
                    sc = score_symbol(sym, daily, current)
                    if sc is not None:
                        scored.append((sc, sym, exch))
                except Exception:
                    pass
            scored.sort(key=lambda x: -x[0])
            for sc, sym, exch in scored[:watchlist_size]:
                watchlist_entries.append({
                    "symbol": sym,
                    "exchange": exch,
                    "strategy": "us_pb1",
                    "score": float(sc),
                    "meta": {"run_id": run_id, "source": "pb1_scoring"},
                })
        except Exception as exc:
            logger.warning("[US_PREP][PB1_SCORE_ERROR] %s", exc)

    # watchlist 저장
    if watchlist_entries:
        try:
            from trader.us.db.repos import save_us_watchlist
            saved = save_us_watchlist(watchlist_entries, trade_date=trade_date)
            logger.info("[US_PREP][WATCHLIST][SAVE] count=%d", saved)
        except Exception as exc:
            logger.warning("[US_PREP][WATCHLIST][WARN] %s", exc)

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
    parser.add_argument("--force-now", dest="force_now", default=None)
    args = parser.parse_args()

    result = run_prep(env=args.env, offline=args.offline, force_now=args.force_now)
    if result["status"] != "OK":
        sys.exit(1)


if __name__ == "__main__":
    main()
