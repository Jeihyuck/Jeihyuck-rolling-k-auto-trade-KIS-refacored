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

from trader.us.db.repos import (
    save_us_prep_run,
    finish_us_prep_run,
    clear_and_save_locked_us_watchlist,
)

logger = logging.getLogger(__name__)

# Critical ETFs for prep status determination
CRITICAL_ETFS = {"QQQ", "SPY", "SMH", "SOXX"}

# Critical ETFs for prep status determination
CRITICAL_ETFS = {"QQQ", "SPY", "SMH", "SOXX"}


def _determine_prep_status(
    total_symbols: int,
    success_count: int,
    critical_etf_failures: set[str],
    has_fatal_error: bool,
) -> str:
    """상태 판정: OK / OK_WITH_WARNINGS / DEGRADED / ERROR."""
    if has_fatal_error:
        return "ERROR"
    
    if critical_etf_failures:
        logger.warning(
            "[US_PREP][STATUS] DEGRADED due to critical ETF failures: %s",
            ", ".join(critical_etf_failures)
        )
        return "DEGRADED"
    
    if total_symbols == 0:
        return "ERROR"
    
    success_rate = success_count / total_symbols
    if success_rate >= 0.9:
        return "OK"
    elif success_rate >= 0.7:
        logger.warning(
            "[US_PREP][STATUS] OK_WITH_WARNINGS success=%d/%d rate=%.1f%%",
            success_count, total_symbols, success_rate * 100
        )
        return "OK_WITH_WARNINGS"
    else:
        logger.warning(
            "[US_PREP][STATUS] DEGRADED success=%d/%d rate=%.1f%%",
            success_count, total_symbols, success_rate * 100
        )
        return "DEGRADED"


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

    # 시간 및 run_id 초기화
    from zoneinfo import ZoneInfo
    _NY_TZ = ZoneInfo("America/New_York")
    if force_now:
        _now = datetime.fromisoformat(force_now).astimezone(_NY_TZ)
    else:
        _now = datetime.now(tz=_NY_TZ)
    trade_date = _now.strftime("%Y-%m-%d")
    
    # us_agent_runs에 prep run 시작 기록
    run_id = ""
    try:
        run_id = save_us_prep_run(
            trade_date=trade_date,
            agent_name="us_prep",
            mode="prep",
            env=env,
        )
        logger.info("[US_PREP][RUN_ID] %s", run_id)
    except Exception as exc:
        logger.error("[US_PREP][ERROR] save_us_prep_run failed: %s", exc)
        return {"status": "ERROR", "stage": "prep_run_init", "error": str(exc)}

    # 1. Universe 로드
    try:
        from trader.us.universe import load_universe, get_all_tickers
        universe = load_universe(force=True)
        tickers = get_all_tickers()
        logger.info("[US_PREP][UNIVERSE] loaded %d tickers", len(tickers))
    except Exception as exc:
        logger.error("[US_PREP][ERROR] universe load failed: %s", exc)
        finish_us_prep_run(run_id, status="ERROR", result=str(exc))
        return {"status": "ERROR", "stage": "universe", "error": str(exc)}

    # 2. Data Provider
    from trader.us.data_provider import USDataProvider
    provider = USDataProvider(offline=offline)

    # 3. Strategy scoring + skip/error 추적
    from trader.us.strategy.us_pb1_pullback import USPb1PullbackStrategy
    from trader.us.strategy.us_momentum import USMomentumStrategy
    from trader.us.strategy.us_etf_trend import USEtfTrendStrategy

    all_intents = []
    watchlist_entries: list[dict] = []
    success_count = 0
    skip_count = 0
    error_count = 0
    critical_etf_failures: set[str] = set()
    has_fatal_error = False

    for StratCls in [USPb1PullbackStrategy, USMomentumStrategy, USEtfTrendStrategy]:
        strat = StratCls(run_id=run_id, trade_date=trade_date)
        try:
            intents = strat.run_on_universe(tickers, provider)
            all_intents.extend(intents)
            # watchlist 항목 구성
            for intent in intents:
                symbol = intent.get("symbol")
                watchlist_entries.append({
                    "symbol": symbol,
                    "exchange": intent.get("exchange", "NASDAQ"),
                    "strategy": strat.name,
                    "score": float(intent.get("score", 0)),
                    "meta": {"run_id": run_id},
                })
                success_count += 1
                
                # Critical ETF 검사
                if symbol in CRITICAL_ETFS:
                    logger.info("[US_PREP][CRITICAL_ETF_OK] symbol=%s", symbol)
            
            logger.info("[US_STRATEGY][SCORED] strategy=%s intents=%d",
                        strat.name, len(intents))
        except Exception as exc:
            logger.warning("[US_PREP][STRATEGY_ERROR] strategy=%s error=%s",
                           getattr(strat, "name", "?"), exc)
            error_count += 1
            if "fatal" in str(exc).lower():
                has_fatal_error = True

    # US PB1 engine scoring (US_STRATEGY_ENGINE=pb1)
    engine_name = os.getenv("US_STRATEGY_ENGINE", "").lower()
    if engine_name == "pb1":
        try:
            from trader.us.pb1.us_entry_engine import score_symbol
            watchlist_size = int(os.getenv("US_PREP_WATCHLIST_SIZE", "30"))
            scored: list[tuple[float, str, str]] = []
            for ticker in tickers:
                sym = ticker if isinstance(ticker, str) else ticker.get("symbol", "")
                exch = "NASDAQ" if isinstance(ticker, str) else ticker.get("exchange", "NASDAQ")
                try:
                    daily = provider.get_daily_prices(sym, exch)
                    current = provider.get_current_price(sym, exch)
                    sc = score_symbol(sym, daily, current)
                    if sc is not None:
                        scored.append((sc, sym, exch))
                        success_count += 1
                    else:
                        skip_count += 1
                except Exception as symbol_exc:
                    logger.debug("[US_PREP][PB1_SKIP] symbol=%s error=%s", sym, symbol_exc)
                    skip_count += 1
                    if sym in CRITICAL_ETFS:
                        logger.error("[US_PREP][CRITICAL_ETF_FAIL] symbol=%s", sym)
                        critical_etf_failures.add(sym)
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
            error_count += 1

    # 상태 판정
    total_symbols = len(tickers)
    prep_status = _determine_prep_status(
        total_symbols=total_symbols,
        success_count=success_count,
        critical_etf_failures=critical_etf_failures,
        has_fatal_error=has_fatal_error,
    )
    
    logger.info(
        "[US_PREP][STATUS] %s total=%d success=%d skip=%d error=%d critical_fail=%s",
        prep_status, total_symbols, success_count, skip_count, error_count,
        ", ".join(critical_etf_failures) if critical_etf_failures else "none"
    )

    # locked watchlist 저장 (clear_and_save_locked_us_watchlist)
    saved_count = 0
    if watchlist_entries:
        try:
            saved_count = clear_and_save_locked_us_watchlist(
                entries=watchlist_entries,
                trade_date=trade_date,
                run_id=run_id,
                prep_status=prep_status,
            )
            logger.info("[US_PREP][WATCHLIST][LOCKED] count=%d status=%s", saved_count, prep_status)
        except Exception as exc:
            logger.error("[US_PREP][WATCHLIST][ERROR] %s", exc)
            prep_status = "ERROR"
            has_fatal_error = True
    else:
        logger.warning("[US_PREP][WATCHLIST][EMPTY] no entries to save")
        if prep_status == "OK":
            prep_status = "OK_WITH_WARNINGS"

    # us_agent_runs 완료 기록
    result_msg = f"status={prep_status} watchlist={saved_count} success={success_count}/{total_symbols}"
    finish_us_prep_run(run_id=run_id, status=prep_status, result=result_msg)
    
    logger.info("[US_PREP][FINISH] %s", result_msg)
    return {
        "status": prep_status,
        "run_id": run_id,
        "trade_date": trade_date,
        "tickers_loaded": total_symbols,
        "watchlist_saved": saved_count,
        "success_count": success_count,
        "skip_count": skip_count,
        "error_count": error_count,
        "critical_etf_failures": list(critical_etf_failures),
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
