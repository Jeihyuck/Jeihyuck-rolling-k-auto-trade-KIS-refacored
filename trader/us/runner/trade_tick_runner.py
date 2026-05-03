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


def run_trade_tick(
    session: str = "am",
    env: str = "practice",
    offline: bool = False,
    force_now: str | None = None,
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
    logger.info("[US_TICK][START] session=%s env=%s offline=%s", session, env, offline)

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
    if not is_us_trading_day(now.date()):
        logger.info("[US_TICK][SKIP] not_trading_day date=%s", now.date())
        return {"status": "SKIP", "reason": "not_trading_day"}

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
    if not offline:
        try:
            from trader.us.execution.fills import get_fills_today
            fills_today = get_fills_today(provider=provider)
            logger.info("[US_FILLS][OK] count=%d", len(fills_today))
        except Exception as exc:
            logger.warning("[US_TICK][WARN] fills fetch failed: %s", exc)

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
    after_cutoff = _entry_cutoff_passed(now)

    if after_cutoff:
        logger.info(
            "[US_ENTRY][BLOCK] reason=after_entry_cutoff time=%s",
            now.strftime("%H:%M:%S"),
        )
    else:
        try:
            from trader.us.universe import get_all_tickers
            tickers = get_all_tickers()
            engine = _get_strategy_engine(env=env, offline=offline)
            entry_intents = engine.evaluate_entries(
                tickers=tickers,
                provider=provider,
                sold_today=sold_today,
                available_cash_usd=effective_budget,
                position_count=position_count,
                now=now,
            )
        except Exception as exc:
            logger.warning("[US_ENTRY][EVAL][WARN] %s", exc)
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
    err_cnt = sum(1 for o in orders if o["status"] == "ERROR")

    logger.info(
        "[US_ORDER][ROUTE][DONE] total=%d ack=%d dry_run=%d blocked=%d error=%d",
        len(orders), ack_cnt, dry_cnt, blocked_cnt, err_cnt,
    )

    status = "OK_WITH_WARNINGS" if err_cnt > 0 else "OK"
    logger.info("[US_TICK][DONE] session=%s status=%s", session, status)

    return {
        "status": status,
        "session": session,
        "orders": orders,
        "ack": ack_cnt,
        "dry_run": dry_cnt,
        "blocked": blocked_cnt,
        "errors": err_cnt,
        "budget": budget,
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
    args = parser.parse_args()

    result = run_trade_tick(
        session=args.session,
        env=args.env,
        offline=args.offline,
        force_now=args.force_now,
    )
    if result["status"] == "ERROR":
        sys.exit(1)


if __name__ == "__main__":
    main()
