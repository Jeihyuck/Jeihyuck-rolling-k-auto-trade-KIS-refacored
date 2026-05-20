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


def _resolve_final_prep_status(
    provisional_status: str,
    saved_count: int,
    score_nonzero_count: int,
    score_contract_ok: bool,
    has_fatal_error: bool = False,
) -> tuple[str, bool]:
    """Locked watchlist score contract를 반영해 최종 상태를 확정한다."""
    final_status = provisional_status
    fatal = has_fatal_error

    if fatal:
        return "ERROR", True

    if saved_count > 0 and score_nonzero_count == 0:
        return "ERROR", True

    if saved_count > 0 and not score_contract_ok:
        return "ERROR", True

    return final_status, fatal


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

    from trader.us.score_columns import extract_us_score

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

                score_value, score_source = extract_us_score(intent, "final", return_source=True)
                if score_value is None:
                    logger.warning(
                        "[US_PREP][SCORE_MISSING] strategy=%s symbol=%s intent_keys=%s",
                        strat.name,
                        symbol,
                        sorted(intent.keys()),
                    )
                    score_value = 0.0
                    score_source = "missing"

                score_value = float(score_value or 0.0)
                reason_json = intent.get("reason_json") if isinstance(intent.get("reason_json"), dict) else {}
                
                watchlist_entries.append({
                    "symbol": symbol,
                    "exchange": intent.get("exchange", "NASDAQ"),
                    "strategy": strat.name,
                    "score": score_value,
                    "score_final": score_value,
                    "final_score": score_value,
                    "scores": {
                        "final": score_value,
                        "score": score_value,
                        "score_final": score_value,
                    },
                    "reason_json": reason_json,
                    "meta": {
                        "run_id": run_id,
                        "score": score_value,
                        "score_final": score_value,
                        "final_score": score_value,
                        "score_source": score_source,
                        "reason_json": reason_json,
                        "intent": intent,
                    },
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
    save_result = None
    saved_count = 0
    if watchlist_entries:
        try:
            save_result = clear_and_save_locked_us_watchlist(
                entries=watchlist_entries,
                trade_date=trade_date,
                run_id=run_id,
                prep_status=provisional_status,
            )
            # backward compatible: saved_count는 unique_count
            saved_count = save_result.get("unique_count", 0) if isinstance(save_result, dict) else save_result
            logger.info(
                "[US_PREP][WATCHLIST][LOCKED] raw=%d unique=%d duplicate=%d status=%s",
                save_result.get("raw_count", 0) if isinstance(save_result, dict) else 0,
                saved_count,
                save_result.get("duplicate_count", 0) if isinstance(save_result, dict) else 0,
                provisional_status
            )
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
    
    # Quality summary 추출
    score_nonzero_count = save_result.get("score_nonzero", 0) if isinstance(save_result, dict) else 0
    score_zero_count = save_result.get("score_zero", 0) if isinstance(save_result, dict) else 0
    score_missing_count = save_result.get("score_missing", 0) if isinstance(save_result, dict) else 0
    score_nonzero_ratio = save_result.get("score_nonzero_ratio", 0.0) if isinstance(save_result, dict) else 0.0
    
    # score contract ok 판단
    from trader.us.watchlist_quality import US_MIN_WATCHLIST_SCORE_NONZERO_RATIO
    score_contract_ok = bool(save_result.get("contract_ok")) if isinstance(save_result, dict) else False
    if not isinstance(save_result, dict):
        score_contract_ok = (
            score_nonzero_ratio >= US_MIN_WATCHLIST_SCORE_NONZERO_RATIO and
            score_missing_count == 0
        )

    final_status, has_fatal_error = _resolve_final_prep_status(
        provisional_status=provisional_status,
        saved_count=saved_count,
        score_nonzero_count=score_nonzero_count,
        score_contract_ok=score_contract_ok,
        has_fatal_error=has_fatal_error,
    )
    
    # trade_can_proceed 판단
    trade_can_proceed = (
        final_status in ("OK", "OK_WITH_WARNINGS") and
        score_contract_ok and
        saved_count >= int(os.getenv("US_MIN_LOCKED_WATCHLIST_COUNT", "10"))
    )
    
    logger.info(
        "[US_PREP][FINAL_STATUS] provisional=%s final=%s locked_unique=%d "
        "score_nonzero=%d score_zero=%d missing=%d ratio=%.4f contract_ok=%d trade_can_proceed=%d",
        provisional_status, final_status, saved_count,
        score_nonzero_count, score_zero_count, score_missing_count, score_nonzero_ratio,
        int(score_contract_ok), int(trade_can_proceed)
    )

    # us_agent_runs 완료 기록 (한국장 패턴: quality summary를 dict로 저장)
    result_dict = {
        "status": final_status,
        "watchlist_raw_count": save_result.get("raw_count", saved_count) if isinstance(save_result, dict) else saved_count,
        "watchlist_unique_count": saved_count,
        "watchlist_duplicate_count": save_result.get("duplicate_count", 0) if isinstance(save_result, dict) else 0,
        "watchlist_count": saved_count,  # backward compatible
        "locked_unique_count": locked_unique_count,
        "total_symbols": total_symbols,
        "event_success_count": event_success_count,
        "skip_count": skip_count,
        "error_count": error_count,
        "critical_etf_failures": list(critical_etf_failures),
        "score_nonzero_count": score_nonzero_count,
        "score_zero_count": score_zero_count,
        "score_missing_count": score_missing_count,
        "score_nonzero_ratio": round(score_nonzero_ratio, 4),
        "score_contract_ok": score_contract_ok,
        "contract_errors": save_result.get("contract_errors", []) if isinstance(save_result, dict) else [],
        "contract_warnings": save_result.get("contract_warnings", []) if isinstance(save_result, dict) else [],
        "trade_can_proceed": trade_can_proceed,
    }
    
    finish_us_prep_run(run_id=run_id, status=final_status, result=result_dict)
    
    logger.info(
        "[US_PREP][FINISH] status=%s unique=%d score_nonzero=%d trade_can_proceed=%d",
        final_status, saved_count, score_nonzero_count, int(trade_can_proceed)
    )
    
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
        "trade_can_proceed": trade_can_proceed,
    }


def main() -> None:
    from trader.us.utils.logging_utils import setup_us_logging
    setup_us_logging()
    parser = argparse.ArgumentParser(description="US Prep Runner")
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--force-now", dest="force_now", default=None)
    args = parser.parse_args()

    result = run_prep(env=args.env, offline=args.offline, force_now=args.force_now)
    
    # 오프라인 모드에서 빈 watchlist 허용 옵션
    allow_empty_offline = int(os.getenv("US_PREP_ALLOW_EMPTY_WATCHLIST_OFFLINE", "0")) != 0
    if args.offline and allow_empty_offline:
        # 오프라인 모드에서는 빈 watchlist도 정상 종료 허용
        if result["status"] in ("OK", "OK_WITH_WARNINGS", "ERROR"):
            logger.info("[US_PREP][OFFLINE_EXIT] status=%s allowed (empty watchlist permitted)", result["status"])
            sys.exit(0)
    
    if result["status"] not in ("OK", "OK_WITH_WARNINGS"):
        sys.exit(1)


if __name__ == "__main__":
    main()
