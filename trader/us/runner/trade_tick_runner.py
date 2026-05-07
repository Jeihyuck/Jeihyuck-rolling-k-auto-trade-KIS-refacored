# -*- coding: utf-8 -*-
"""US Trade Tick Runner.

미국장 1 tick: reconcile → exit 평가 → entry 평가 → order routing → 결과 저장.

CLI:
  python -m trader.us.runner.trade_tick_runner \\
    --session am \\
    --env practice \\
    [--offline] \\
    [--force-now 2026-01-02T09:35:00-05:00]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


def _entry_cutoff_passed(now: datetime) -> bool:
    """15:45 ET 이후이면 True."""
    from zoneinfo import ZoneInfo
    NY_TZ = ZoneInfo("America/New_York")
    cutoff_str = os.getenv("US_BLOCK_NEW_ENTRY_AFTER_ET", "15:45")
    try:
        h, m = cutoff_str.split(":")
        from datetime import time
        cutoff = time(int(h), int(m))
    except Exception:
        from datetime import time
        cutoff = time(15, 45)

    now_ny = now.astimezone(NY_TZ)
    return now_ny.time() >= cutoff


def _dedupe_watchlist_best_by_symbol(rows: list[dict]) -> list[dict]:
    """Locked watchlist rows를 symbol별 최고 score 1개로 축약한다.
    
    Args:
        rows: 중복 symbol이 포함된 watchlist rows
        
    Returns:
        symbol별 최고 score row만 남긴 list
    """
    from trader.us.symbols import normalize_symbol

    best: dict[str, dict] = {}
    raw_count = len(rows)

    for row in rows:
        try:
            sym = str(row.get("symbol", "")).upper()
            if not sym:
                continue
            # normalize_symbol로 정규화 (없으면 uppercase만)
            try:
                sym = normalize_symbol(sym)
            except Exception:
                pass
            
            score = float(row.get("score") or 0.0)

            clean = dict(row)
            clean["symbol"] = sym
            clean["_dedupe_score"] = score

            prev = best.get(sym)
            if prev is None or score > float(prev.get("_dedupe_score") or 0.0):
                best[sym] = clean
        except Exception as exc:
            logger.warning(
                "[US_ENTRY][WATCHLIST_DEDUPE_SKIP] row=%s error=%s",
                row, exc
            )

    deduped = sorted(
        best.values(),
        key=lambda r: float(r.get("_dedupe_score") or 0.0),
        reverse=True,
    )

    logger.info(
        "[US_ENTRY][WATCHLIST_DEDUPE] raw_rows=%d unique_symbols=%d duplicate_rows=%d",
        raw_count, len(deduped), raw_count - len(deduped)
    )
    return deduped


def run_trade_tick(
    session: str = "am",
    env: str = "practice",
    offline: bool = False,
    force_now: str | None = None,
    run_mode: str | None = None,
    signal_only: bool = False,
    kis_order_allowed: bool = True,
) -> dict:
    """미국장 단일 tick 실행.

    tick 순서:
    1. market phase 확인
    2. trading day 확인
    3. budget 계산
    4. KIS 잔고/체결 조회
    5. us_positions reconcile
    6. exit candidate 평가
    7. entry candidate 평가
    8. order intents 병합
    9. risk gate
    10. order router
    11. order result 저장
    12. tick summary 출력

    Returns:
        {"status": "OK"|"SKIP"|"ERROR"|"OK_WITH_WARNINGS", ...}
    """
    logger.info(
        "[US_TICK][START] session=%s env=%s offline=%s run_mode=%s signal_only=%s",
        session, env, offline, run_mode, signal_only,
    )

    # ── 시각 결정 ──────────────────────────────────────────────────────────────
    from trader.us.market_calendar import now_ny, is_us_trading_day, market_phase
    from zoneinfo import ZoneInfo

    NY_TZ = ZoneInfo("America/New_York")

    if force_now:
        now = datetime.fromisoformat(force_now).astimezone(NY_TZ)
    else:
        now = now_ny()

    logger.info(
        "[US_TICK][TIME] session=%s force_now=%s resolved_now_et=%s",
        session,
        force_now or "",
        now.isoformat(),
    )

    # ── 거래일 확인 ───────────────────────────────────────────────────────────
    # signal_only 모드에서는 거래일이 아니어도 계속 진행 (신호만 생성)
    if not is_us_trading_day(now.date()):
        if not signal_only:
            logger.info("[US_TICK][SKIP] not_trading_day date=%s", now.date())
            return {"status": "SKIP", "reason": "not_trading_day"}
        else:
            logger.info(
                "[US_TICK][SIGNAL_ONLY] non_trading_day date=%s run_mode=%s",
                now.date(), run_mode or "NON_TRADING_SIGNAL_ONLY",
            )

    # ── 장 phase 확인 ─────────────────────────────────────────────────────────
    phase = market_phase(now)
    if phase not in ("REGULAR_OPEN", "REGULAR_MID", "REGULAR_CLOSE"):
        logger.info("[US_TICK][SKIP] market not open phase=%s", phase)
        return {"status": "SKIP", "reason": "market_not_open", "phase": phase}

    # ── 예산 계산 ─────────────────────────────────────────────────────────────
    from trader.us.budget import resolve_us_order_budget
    from trader.us.data_provider import USDataProvider

    provider = USDataProvider(offline=offline)

    if offline:
        available_cash_usd = 10000.0
    else:
        # DRY_RUN 모드: 환경변수 fallback 먼저 시도
        dry_run_mode = os.getenv("DRY_RUN", "0") == "1"
        try:
            available_cash_usd = provider.get_orderable_cash(
                symbol="AAPL", exchange="NASDAQ", price=100.0
            )
        except Exception as exc:
            if dry_run_mode:
                fallback = float(os.getenv("US_DRY_RUN_CASH_FALLBACK_USD", "10000.0"))
                logger.warning(
                    "[US_TICK][WARN] orderable_cash failed in DRY_RUN, using fallback=%.2f: %s",
                    fallback, exc,
                )
                available_cash_usd = fallback
            else:
                # fallback: balance에서 현금성 필드 탐색
                try:
                    balance = provider.get_balance()
                    for _k in ("ord_psbl_cash", "ovrs_ord_psbl_amt", "orderable_cash",
                               "cash", "psbl_amt", "frcr_pchs_amt1"):
                        _v = balance.get(_k)
                        if _v is not None:
                            try:
                                available_cash_usd = float(_v)
                                break
                            except (ValueError, TypeError):
                                pass
                    else:
                        available_cash_usd = 0.0
                except Exception as exc2:
                    logger.warning("[US_TICK][WARN] cash fetch failed: %s", exc2)
                    available_cash_usd = 0.0

    budget = resolve_us_order_budget(available_cash_usd)
    effective_budget = budget["effective_order_budget_usd"]

    # ── reconcile ─────────────────────────────────────────────────────────────
    logger.info("[US_RECONCILE][START] session=%s", session)
    try:
        from trader.us.execution.reconcile import reconcile_positions
        recon = reconcile_positions(provider=provider)
        logger.info(
            "[US_RECONCILE][DONE] status=%s positions=%s",
            recon.get("status"),
            recon.get("position_count", 0),
        )
    except Exception as exc:
        logger.warning("[US_RECONCILE][WARN] %s", exc)
        recon = {"status": "WARN", "error": str(exc)}

    # ── 체결 조회 및 DB 저장 ──────────────────────────────────────────────────
    fills_today: list[dict] = []
    fills_error_count = 0
    fills_warnings_count = 0
    fills_contract_error = False
    fills_temp_error = False
    
    if not offline:
        try:
            from trader.us.execution.fills import get_fills_today
            fills_result = get_fills_today(provider=provider, signal_only=signal_only)
            if fills_result["status"] == "CONTRACT_ERROR":
                logger.error(
                    "[US_TICK][ERROR] fills fetch CONTRACT_ERROR: %s",
                    fills_result.get("error", "unknown")
                )
                fills_error_count += 1
                fills_contract_error = True
            elif fills_result["status"] == "TEMP_ERROR":
                logger.warning(
                    "[US_TICK][WARN] fills fetch TEMP_ERROR: %s",
                    fills_result.get("error", "unknown")
                )
                fills_warnings_count += 1
                fills_temp_error = True
            elif fills_result["status"] != "OK":
                logger.warning(
                    "[US_TICK][WARN] fills fetch failed: %s",
                    fills_result.get("error", "unknown")
                )
                fills_warnings_count += 1
            
            fills_today = fills_result["fills"]
            logger.info("[US_FILLS][FETCHED] count=%d status=%s", len(fills_today), fills_result["status"])
        except Exception as exc:
            logger.error("[US_TICK][ERROR] fills exception: %s", exc)
            fills_error_count += 1

    # KIS fills + DB sold_today 합산
    from trader.us.db.repos import load_today_symbols_sold, save_fills, save_position_snapshot, save_reconcile_log
    kis_sold = {f["symbol"] for f in fills_today if f.get("side") == "SELL"}
    try:
        db_sold = load_today_symbols_sold()
    except Exception:
        db_sold = set()
    sold_today = kis_sold | db_sold

    # fills DB 저장
    if fills_today:
        try:
            save_fills(fills_today)
        except Exception as exc:
            logger.warning("[US_TICK][WARN] save_fills failed: %s", exc)

    # reconcile 결과 positions DB 저장
    recon_positions = recon.get("positions", [])
    if recon_positions:
        try:
            save_position_snapshot(recon_positions)
        except Exception as exc:
            logger.warning("[US_TICK][WARN] save_position_snapshot failed: %s", exc)

    # reconcile log DB 저장
    try:
        save_reconcile_log({
            "status": recon.get("status", "OK"),
            "message": recon.get("error", ""),
            "position_count": len(recon_positions),
            "total_pvs": recon.get("total_pvs_usd", 0),
            "detail": {"session": session},
        })
    except Exception as exc:
        logger.warning("[US_TICK][WARN] save_reconcile_log failed: %s", exc)

    # ── 현재 포지션 ───────────────────────────────────────────────────────────
    # reconcile 결과 우선, 비어 있으면 DB fallback
    from trader.us.db.repos import load_positions as db_load_positions
    if recon_positions:
        current_positions = recon_positions
    else:
        try:
            current_positions = db_load_positions()
        except Exception:
            current_positions = []
    position_count = len(current_positions)

    # ── EXIT 평가 ─────────────────────────────────────────────────────────────
    logger.info("[US_EXIT][EVAL][START] session=%s positions=%d", session, position_count)
    exit_intents: list[dict] = []
    try:
        engine = _get_strategy_engine(env=env, offline=offline)
        exit_intents = engine.evaluate_exits(
            positions=current_positions,
            provider=provider,
            now=now,
        )
    except Exception as exc:
        logger.warning("[US_EXIT][EVAL][WARN] %s", exc)
    logger.info("[US_EXIT][EVAL][DONE] exit_intents=%d", len(exit_intents))

    # ── ENTRY 평가 ────────────────────────────────────────────────────────────
    logger.info("[US_ENTRY][EVAL][START] session=%s budget=%.2f", session, effective_budget)
    entry_intents: list[dict] = []
    entry_eval_error_count = 0
    after_cutoff = _entry_cutoff_passed(now)

    # fills contract error 발생 시 신규 BUY 차단 및 즉시 ERROR 반환
    if fills_contract_error and os.getenv("US_REQUIRE_FILL_CONFIRM", "1") == "1":
        logger.error(
            "[US_ENTRY][BLOCK] reason=fills_contract_error require_fill_confirm=1"
        )
        logger.error(
            "[US_ORDER][ROUTE][SKIP] reason=fills_contract_error"
        )
        logger.error(
            "[US_TICK][DONE] session=%s status=ERROR reason=fills_contract_error", session
        )
        return {
            "status": "ERROR",
            "reason": "fills_contract_error",
            "session": session,
            "orders": [],
            "ack": 0,
            "dry_run": 0,
            "blocked": 0,
            "signal_only": 0,
            "errors": 1,
            "budget": budget,
            "run_mode": run_mode,
            "signal_only_mode": signal_only,
            "kis_order_allowed": kis_order_allowed,
        }
    elif after_cutoff:
        logger.info(
            "[US_ENTRY][BLOCK] reason=after_entry_cutoff time=%s",
            now.strftime("%H:%M:%S"),
        )
    else:
        # prep status 확인 (locked watchlist contract)
        from trader.us.db.repos import load_latest_us_prep_status, load_locked_us_watchlist
        
        trade_date = now.strftime("%Y-%m-%d")
        prep_status_info = load_latest_us_prep_status(trade_date)
        prep_status = prep_status_info.get("status", "UNKNOWN") if prep_status_info else "UNKNOWN"
        
        logger.info(
            "[US_ENTRY][PREP_STATUS] date=%s status=%s",
            trade_date, prep_status
        )
        
        # DEGRADED/ERROR 상태이면 new entry 차단
        if prep_status in ("DEGRADED", "ERROR"):
            logger.warning(
                "[US_ENTRY][BLOCK] reason=prep_degraded_or_error status=%s",
                prep_status
            )
        else:
            # locked watchlist 로드
            try:
                min_watchlist_count = int(os.getenv("US_MIN_LOCKED_WATCHLIST_COUNT", "10"))
                allow_degraded = os.getenv("US_ALLOW_DEGRADED_IN_TRADE", "0") == "1"
                
                watchlist_rows = load_locked_us_watchlist(
                    trade_date=trade_date,
                    min_count=min_watchlist_count,
                    allow_degraded=allow_degraded,
                )
                
                if watchlist_rows:
                    raw_watchlist_count = len(watchlist_rows)
                    # symbol별 best row로 dedupe
                    watchlist_rows = _dedupe_watchlist_best_by_symbol(watchlist_rows)
                    
                    # ── Quality Contract 검증 (hard gate) ──────────────────────
                    from trader.us.watchlist_quality import validate_us_locked_watchlist_quality, format_us_watchlist_error_message
                    
                    quality_contract = validate_us_locked_watchlist_quality(
                        rows=watchlist_rows,
                        stage="trade_load",
                    )
                    
                    if not quality_contract["ok"]:
                        error_msg = format_us_watchlist_error_message(quality_contract)
                        logger.error(error_msg)
                        logger.error(
                            "[US_TICK][DONE] session=%s status=ERROR reason=locked_watchlist_score_contract_fail",
                            session,
                        )
                        return {
                            "status": "ERROR",
                            "reason": "locked_watchlist_score_contract_fail",
                            "session": session,
                            "orders": [],
                            "ack": 0,
                            "dry_run": 0,
                            "blocked": 0,
                            "signal_only": 0,
                            "errors": 1,
                            "score_contract": quality_contract,
                            "run_mode": run_mode,
                            "signal_only_mode": signal_only,
                            "kis_order_allowed": kis_order_allowed,
                        }
                    
                    # Contract 통과 로그
                    logger.info(
                        "[US_ENTRY][SCORE_CONTRACT] stage=trade_load rows=%d unique=%d duplicate=%d "
                        "nonzero=%d zero=%d missing=%d ratio=%.4f ok=1",
                        quality_contract["rows"], quality_contract["unique_symbols"], 
                        quality_contract["duplicate_count"],
                        quality_contract["score_nonzero"], quality_contract["score_zero"], 
                        quality_contract["score_missing"],
                        quality_contract["score_nonzero_ratio"]
                    )
                    
                    # Pipeline contract 로그
                    logger.info(
                        "[US_PIPELINE][CONTRACT] session=%s trade_date=%s prep_status=%s locked_raw=%d locked_deduped=%d fills_status=%s",
                        session, trade_date, prep_status, raw_watchlist_count, len(watchlist_rows),
                        "OK" if fills_error_count == 0 else ("CONTRACT_ERROR" if fills_contract_error else "TEMP_ERROR")
                    )
                    
                    logger.info(
                        "[US_ENTRY][LOCKED_WATCHLIST] raw_count=%d deduped_count=%d prep_status=%s source=db_us_locked_watchlist",
                        raw_watchlist_count, len(watchlist_rows), prep_status
                    )
                    
                    # INPUT CONTRACT 검증
                    logger.info(
                        "[US_ENTRY][INPUT_CONTRACT] rows=%d schema_ok=1",
                        len(watchlist_rows)
                    )
                else:
                    # locked watchlist empty: pipeline input missing, ERROR 즉시 반환
                    logger.error(
                        "[US_ENTRY][BLOCK] reason=locked_watchlist_missing trade_date=%s",
                        trade_date,
                    )
                    logger.error(
                        "[US_TICK][DONE] session=%s status=ERROR reason=locked_watchlist_missing",
                        session,
                    )
                    return {
                        "status": "ERROR",
                        "reason": "locked_watchlist_missing",
                        "session": session,
                        "orders": [],
                        "ack": 0,
                        "dry_run": 0,
                        "blocked": 0,
                        "signal_only": 0,
                        "errors": 1,
                        "run_mode": run_mode,
                        "signal_only_mode": signal_only,
                        "kis_order_allowed": kis_order_allowed,
                    }
                
                if watchlist_rows:
                    engine = _get_strategy_engine(env=env, offline=offline)
                    try:
                        entry_intents = engine.evaluate_entries(
                            tickers=None,  # watchlist_entries를 우선 사용
                            provider=provider,
                            sold_today=sold_today,
                            available_cash_usd=effective_budget,
                            position_count=position_count,
                            now=now,
                            watchlist_entries=watchlist_rows,  # authoritative input
                        )
                    except Exception as exc:
                        logger.error(
                            "[US_ENTRY][EVAL][ERROR] type=%s message=%s",
                            type(exc).__name__, str(exc)
                        )
                        entry_eval_error_count += 1
                        entry_intents = []
            except Exception as exc:
                logger.error("[US_ENTRY][EVAL][ERROR] %s", exc)
                entry_eval_error_count += 1
    
    logger.info("[US_ENTRY][EVAL][DONE] entry_intents=%d", len(entry_intents))

    # ── Order routing ─────────────────────────────────────────────────────────
    all_intents = exit_intents + entry_intents
    logger.info("[US_ORDER][ROUTE][START] total_intents=%d", len(all_intents))

    orders: list[dict] = []
    daily_notional = 0.0

    from trader.us.execution.order_router import route_order

    for intent in all_intents:
        try:
            result = route_order(
                intent,
                current_daily_notional_usd=daily_notional,
                current_position_count=position_count,
                total_portfolio_usd=max(effective_budget, 1000.0),
                available_cash_usd=max(effective_budget - daily_notional, 0.0),
                signal_only=signal_only,
            )
            orders.append(result)
            if result["status"] in ("DRY_RUN", "ACK"):
                daily_notional += float(intent.get("notional_usd", 0))
                if intent.get("side") == "BUY":
                    position_count += 1
        except Exception as exc:
            logger.warning("[US_ORDER][ROUTE][WARN] intent=%s error=%s", intent.get("symbol"), exc)
            orders.append({"status": "ERROR", "error": str(exc), "intent": intent})

    ack_cnt = sum(1 for o in orders if o["status"] == "ACK")
    dry_cnt = sum(1 for o in orders if o["status"] == "DRY_RUN")
    blocked_cnt = sum(1 for o in orders if o["status"] == "BLOCKED")
    signal_only_cnt = sum(1 for o in orders if o["status"] == "SIGNAL_ONLY")
    err_cnt = sum(1 for o in orders if o["status"] == "ERROR")

    logger.info(
        "[US_ORDER][ROUTE][DONE] total=%d ack=%d dry_run=%d blocked=%d signal_only=%d error=%d",
        len(orders), ack_cnt, dry_cnt, blocked_cnt, signal_only_cnt, err_cnt,
    )

    # ── 최종 status 판정 ───────────────────────────────
    total_errors = fills_error_count + entry_eval_error_count + err_cnt
    total_warnings = fills_warnings_count
    
    if total_errors > 0:
        status = "ERROR" if total_errors > 2 else "OK_WITH_ERRORS"
        status_reasons = []
        if fills_error_count > 0:
            status_reasons.append(f"fills_error={fills_error_count}")
        if entry_eval_error_count > 0:
            status_reasons.append(f"entry_eval_error={entry_eval_error_count}")
        if err_cnt > 0:
            status_reasons.append(f"order_error={err_cnt}")
        logger.warning(
            "[US_TICK][STATUS_DECISION] status=%s errors=%d reasons=[%s]",
            status, total_errors, ", ".join(status_reasons)
        )
    elif total_warnings > 0:
        status = "OK_WITH_WARNINGS"
        logger.info(
            "[US_TICK][STATUS_DECISION] status=%s warnings=%d",
            status, total_warnings
        )
    elif signal_only:
        status = "OK_SIGNAL_ONLY"
    elif ack_cnt == 0 and dry_cnt == 0 and len(all_intents) == 0:
        status = "OK_NO_TRADE"
    else:
        status = "OK"
    
    logger.info("[US_TICK][DONE] session=%s status=%s", session, status)

    return {
        "status": status,
        "session": session,
        "orders": orders,
        "ack": ack_cnt,
        "dry_run": dry_cnt,
        "blocked": blocked_cnt,
        "signal_only": signal_only_cnt,
        "errors": err_cnt,
        "budget": budget,
        "run_mode": run_mode,
        "signal_only_mode": signal_only,
        "kis_order_allowed": kis_order_allowed,
    }


# ---------------------------------------------------------------------------
# 전략 엔진 팩토리
# ---------------------------------------------------------------------------

_engine_cache: dict[str, Any] = {}


def _get_strategy_engine(env: str = "practice", offline: bool = False) -> Any:
    """US_STRATEGY_ENGINE env에 따라 엔진을 반환한다."""
    engine_name = os.getenv("US_STRATEGY_ENGINE", "pb1").lower()

    if engine_name not in _engine_cache:
        if engine_name == "pb1":
            from trader.us.pb1.us_pb1_engine import USPb1Engine
            _engine_cache[engine_name] = USPb1Engine(env=env, offline=offline)
        else:
            from trader.us.pb1.us_pb1_engine import USPb1Engine
            logger.warning(
                "[US_ENGINE][WARN] unknown engine=%s falling back to pb1", engine_name
            )
            _engine_cache[engine_name] = USPb1Engine(env=env, offline=offline)

    return _engine_cache[engine_name]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="US Trade Tick Runner")
    parser.add_argument("--session", default="am", choices=["am", "afternoon", "manual"])
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--force-now", dest="force_now", default=None)
    parser.add_argument("--run-mode", dest="run_mode", default=None)
    parser.add_argument("--signal-only", dest="signal_only", action="store_true")
    args = parser.parse_args()

    result = run_trade_tick(
        session=args.session,
        env=args.env,
        offline=args.offline,
        force_now=args.force_now,
        run_mode=args.run_mode,
        signal_only=args.signal_only,
    )
    if result["status"] == "ERROR":
        sys.exit(1)


if __name__ == "__main__":
    main()
