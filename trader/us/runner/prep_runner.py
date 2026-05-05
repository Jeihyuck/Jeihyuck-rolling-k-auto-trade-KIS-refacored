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


def _determine_prep_status(
    total_symbols: int,
    locked_unique_count: int,
    data_failed_symbols: set[str],
    critical_etf_failures: set[str],
    has_fatal_error: bool,
    min_locked_unique: int = 10,
    target_locked_unique: int = 15,
    warn_on_data_failure: bool = True,
) -> str:
    """상태 판정: OK / OK_WITH_WARNINGS / DEGRADED / ERROR.
    
    Args:
        total_symbols: universe 전체 종목 수
        locked_unique_count: locked watchlist의 unique symbol 수
        data_failed_symbols: 데이터 조회 최종 실패 symbol set
        critical_etf_failures: critical ETF 중 실패한 symbol set
        has_fatal_error: fatal error 발생 여부
        min_locked_unique: DEGRADED 판정 minimum
        target_locked_unique: OK_WITH_WARNINGS 판정 target
        warn_on_data_failure: data_failed_symbols 있으면 OK_WITH_WARNINGS로 처리
    """
    if has_fatal_error:
        return "ERROR"
    
    if total_symbols <= 0:
        return "ERROR"
    
    if critical_etf_failures:
        logger.warning(
            "[US_PREP][STATUS] DEGRADED due to critical ETF failures: %s",
            ", ".join(critical_etf_failures)
        )
        return "DEGRADED"
    
    if locked_unique_count <= 0:
        return "ERROR"
    
    if locked_unique_count < min_locked_unique:
        logger.warning(
            "[US_PREP][STATUS] DEGRADED locked_unique=%d < min=%d",
            locked_unique_count, min_locked_unique
        )
        return "DEGRADED"
    
    if warn_on_data_failure and data_failed_symbols:
        logger.warning(
            "[US_PREP][STATUS] OK_WITH_WARNINGS due to data failures: %s",
            ", ".join(sorted(data_failed_symbols))
        )
        return "OK_WITH_WARNINGS"
    
    if locked_unique_count < target_locked_unique:
        logger.warning(
            "[US_PREP][STATUS] OK_WITH_WARNINGS locked_unique=%d < target=%d",
            locked_unique_count, target_locked_unique
        )
        return "OK_WITH_WARNINGS"
    
    return "OK"


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

    # 2. Data Provider (cache enabled for prep)
    from trader.us.data_provider import USDataProvider
    provider = USDataProvider(offline=offline, cache_enabled=True)

    # 3. Strategy scoring + skip/error 추적
    from trader.us.strategy.us_pb1_pullback import USPb1PullbackStrategy
    from trader.us.strategy.us_momentum import USMomentumStrategy
    from trader.us.strategy.us_etf_trend import USEtfTrendStrategy

    all_intents = []
    watchlist_entries: list[dict] = []
    scored_symbols: set[str] = set()  # unique symbols scored
    event_success_count = 0  # total scoring events
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
                if symbol:
                    norm_symbol = str(symbol).upper()
                    scored_symbols.add(norm_symbol)
                    event_success_count += 1
                    
                    # Critical ETF 검사
                    if norm_symbol in CRITICAL_ETFS:
                        logger.info("[US_PREP][CRITICAL_ETF_OK] symbol=%s", norm_symbol)
                
                watchlist_entries.append({
                    "symbol": symbol,
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
            error_count += 1
            if "fatal" in str(exc).lower():
                has_fatal_error = True

    # US PB1 engine scoring (US_STRATEGY_ENGINE=pb1)
    engine_name = os.getenv("US_STRATEGY_ENGINE", "").lower()
    if engine_name == "pb1":
        try:
            from trader.us.pb1.us_entry_engine import score_symbol
            from trader.us.symbols import normalize_symbol, resolve_exchange
            watchlist_size = int(os.getenv("US_PREP_WATCHLIST_SIZE", "30"))
            scored: list[tuple[float, str, str]] = []
            
            for ticker in tickers:
                if isinstance(ticker, str):
                    raw_sym = ticker
                    raw_exch = None
                else:
                    raw_sym = ticker.get("symbol", "")
                    raw_exch = ticker.get("exchange")
                
                critical_sym = str(raw_sym).upper()
                
                try:
                    sym = normalize_symbol(raw_sym)
                    critical_sym = sym
                    exch = resolve_exchange(sym)
                    
                    if raw_exch and raw_exch.upper() != exch:
                        logger.warning(
                            "[US_PREP][PB1_EXCHANGE_OVERRIDE] symbol=%s input_exchange=%s registry_exchange=%s",
                            sym, raw_exch, exch
                        )
                    
                    daily = provider.get_daily_prices(sym, exch)
                    current = provider.get_current_price(sym, exch)
                    sc = score_symbol(sym, daily, current)
                    
                    if sc is not None:
                        scored.append((sc, sym, exch))
                        scored_symbols.add(sym)
                        event_success_count += 1
                    else:
                        skip_count += 1
                        
                except Exception as symbol_exc:
                    logger.debug("[US_PREP][PB1_SKIP] symbol=%s error=%s", raw_sym, symbol_exc)
                    skip_count += 1
                    if critical_sym in CRITICAL_ETFS:
                        logger.error("[US_PREP][CRITICAL_ETF_FAIL] symbol=%s", critical_sym)
                        critical_etf_failures.add(critical_sym)
                        
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

    # ================================================================
    # Data coverage, signal coverage, missing symbols analysis
    # ================================================================
    total_symbols = len(tickers)
    
    # universe_symbols 정규화
    from trader.us.symbols import normalize_symbol as norm_sym
    universe_symbols: set[str] = set()
    for t in tickers:
        if isinstance(t, str):
            try:
                universe_symbols.add(norm_sym(t))
            except Exception:
                pass
        else:
            try:
                universe_symbols.add(norm_sym(t.get("symbol", "")))
            except Exception:
                pass
    
    # Data provider stats 추출
    data_ok_symbols = provider.stats["daily_ok_symbols"] | provider.stats["price_ok_symbols"]
    data_failed_symbols = provider.stats["daily_fail_symbols"] | provider.stats["price_fail_symbols"]
    
    # Cache stats 로그
    logger.info(
        "[US_PREP][DATA_CACHE] daily_hit=%d daily_miss=%d price_hit=%d price_miss=%d",
        provider.stats["daily_hit"], provider.stats["daily_miss"],
        provider.stats["price_hit"], provider.stats["price_miss"]
    )
    
    # KIS client stats 로그
    client_stats = provider.get_client_stats()
    if client_stats:
        logger.info(
            "[US_PREP][KIS_STATS] get_retry=%d post_retry=%d http_fail_final=%d",
            client_stats.get("get_retry_count", 0),
            client_stats.get("post_retry_count", 0),
            client_stats.get("http_fail_final_count", 0)
        )
    
    # Data coverage 로그
    logger.info(
        "[US_PREP][DATA_COVERAGE] total=%d data_ok=%d data_failed=%d",
        total_symbols, len(data_ok_symbols), len(data_failed_symbols)
    )
    
    if data_failed_symbols:
        logger.warning(
            "[US_PREP][DATA_FAILED_SYMBOLS] count=%d symbols=%s",
            len(data_failed_symbols), ", ".join(sorted(data_failed_symbols))
        )
        for sym in sorted(data_failed_symbols):
            reason = provider.stats["fail_reasons"].get(sym, "UNKNOWN")
            logger.warning("[US_PREP][DATA_FAIL_REASON] symbol=%s reason=%s", sym, reason)
    
    # Signal coverage: scored_symbols
    signal_symbols = scored_symbols
    missing_signal_symbols = sorted(universe_symbols - signal_symbols)
    
    logger.info(
        "[US_PREP][SIGNAL_COVERAGE] total=%d signal_unique=%d missing=%d event_success=%d",
        total_symbols, len(signal_symbols), len(missing_signal_symbols), event_success_count
    )
    
    if missing_signal_symbols:
        logger.info(
            "[US_PREP][MISSING_SIGNAL_SYMBOLS] count=%d symbols=%s reason=NO_SIGNAL_OR_FILTERED",
            len(missing_signal_symbols), ", ".join(missing_signal_symbols)
        )
    
    # ================================================================
    # Watchlist dedup summary
    # ================================================================
    locked_unique_symbols: set[str] = set()
    best_by_symbol: dict[str, dict] = {}
    
    for entry in watchlist_entries:
        try:
            sym = norm_sym(entry["symbol"])
            locked_unique_symbols.add(sym)
            score = float(entry.get("score") or 0)
            if sym not in best_by_symbol or score > best_by_symbol[sym]["score"]:
                best_by_symbol[sym] = entry
        except Exception:
            pass
    
    logger.info(
        "[US_WATCHLIST][DEDUP_SUMMARY] raw_rows=%d unique_symbols=%d duplicate_rows=%d",
        len(watchlist_entries), len(best_by_symbol), len(watchlist_entries) - len(best_by_symbol)
    )
    
    # ================================================================
    # Provisional prep status (saved 전 판정)
    # ================================================================
    locked_unique_count = len(locked_unique_symbols)
    min_locked_unique = int(os.getenv("US_PREP_MIN_LOCKED_UNIQUE", "10"))
    target_locked_unique = int(os.getenv("US_PREP_TARGET_LOCKED_UNIQUE", "15"))
    warn_on_data_failure = int(os.getenv("US_PREP_WARN_ON_DATA_FAILURE", "1")) != 0
    
    provisional_status = _determine_prep_status(
        total_symbols=total_symbols,
        locked_unique_count=locked_unique_count,
        data_failed_symbols=data_failed_symbols,
        critical_etf_failures=critical_etf_failures,
        has_fatal_error=has_fatal_error,
        min_locked_unique=min_locked_unique,
        target_locked_unique=target_locked_unique,
        warn_on_data_failure=warn_on_data_failure,
    )
    
    logger.info(
        "[US_PREP][STATUS] %s total=%d locked_unique=%d data_ok=%d data_failed=%d skip=%d error=%d critical_fail=%s",
        provisional_status, total_symbols, locked_unique_count, len(data_ok_symbols), 
        len(data_failed_symbols), skip_count, error_count,
        ", ".join(critical_etf_failures) if critical_etf_failures else "none"
    )

    # ================================================================
    # locked watchlist 저장
    # ================================================================
    saved_count = 0
    if watchlist_entries:
        try:
            saved_count = clear_and_save_locked_us_watchlist(
                entries=watchlist_entries,
                trade_date=trade_date,
                run_id=run_id,
                prep_status=provisional_status,
            )
            logger.info("[US_PREP][WATCHLIST][LOCKED] count=%d status=%s", saved_count, provisional_status)
        except Exception as exc:
            logger.error("[US_PREP][WATCHLIST][ERROR] %s", exc)
            provisional_status = "ERROR"
            has_fatal_error = True
    else:
        logger.warning("[US_PREP][WATCHLIST][EMPTY] no entries to save")
        if provisional_status == "OK":
            provisional_status = "OK_WITH_WARNINGS"
    
    # Final status (saved 후 최종 재판정)
    final_status = provisional_status
    
    logger.info(
        "[US_PREP][FINAL_STATUS] provisional=%s final=%s locked_unique=%d",
        provisional_status, final_status, locked_unique_count
    )

    # us_agent_runs 완료 기록
    result_msg = (
        f"status={final_status} watchlist={saved_count} "
        f"locked_unique={locked_unique_count}/{total_symbols} "
        f"event_success={event_success_count}"
    )
    finish_us_prep_run(run_id=run_id, status=final_status, result=result_msg)
    
    logger.info("[US_PREP][FINISH] %s", result_msg)
    return {
        "status": final_status,
        "run_id": run_id,
        "trade_date": trade_date,
        "tickers_loaded": total_symbols,
        "watchlist_saved": saved_count,
        "locked_unique_count": locked_unique_count,
        "event_success_count": event_success_count,
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
    if result["status"] not in ("OK", "OK_WITH_WARNINGS"):
        sys.exit(1)


if __name__ == "__main__":
    main()
