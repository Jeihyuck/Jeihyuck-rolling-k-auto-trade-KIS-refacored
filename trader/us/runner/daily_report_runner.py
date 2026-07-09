# -*- coding: utf-8 -*-
"""US Daily Report Runner.

미국장 trading session 종료 후一日 summary report 생성.

Features:
- session 지원 (am, afternoon, close)
- trade_date: NY 기준 trade_date 자동 계산 또는 force_now 사용
- stale report 방지: force_now 사용 시 해당 날짜의 report 생성
- DB 기반 metrics: US order/fill/position/watchlist score contract
- fresh report guarantee: workflow 종료 후 새 report 생성 보장
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


def _git_value(args: list[str]) -> str:
    try:
        return subprocess.check_output(["git", *args], text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return ""


def _report_provenance(session: str | None) -> dict:
    branch = os.getenv("GITHUB_REF_NAME") or _git_value(["rev-parse", "--abbrev-ref", "HEAD"])
    sha = os.getenv("GITHUB_SHA") or _git_value(["rev-parse", "HEAD"])
    workflow = os.getenv("GITHUB_WORKFLOW") or "local"
    run_id = os.getenv("GITHUB_RUN_ID", "local")
    now_utc = datetime.utcnow().isoformat() + "Z"
    now_et = datetime.now(tz=NY_TZ).isoformat()
    now_kst = datetime.now(tz=ZoneInfo("Asia/Seoul")).isoformat()
    return {
        "branch": branch or "unknown",
        "commit_sha": sha or "unknown",
        "sha": sha or "unknown",
        "workflow": workflow,
        "github_run_id": run_id,
        "github_run_attempt": os.getenv("GITHUB_RUN_ATTEMPT", "0"),
        "event_name": os.getenv("GITHUB_EVENT_NAME", "local"),
        "actor": os.getenv("GITHUB_ACTOR", os.getenv("USER", "local")),
        "session": session,
        "run_id": run_id,
        "session_id": f"{session or 'daily'}-{run_id}",
        "source_log_file": os.getenv("US_SOURCE_LOG_FILE", "runtime/wsl-us-trader.log"),
        "started_at_utc": os.getenv("US_STARTED_AT_UTC", now_utc),
        "started_at_et": os.getenv("US_STARTED_AT_ET", now_et),
        "started_at_kst": os.getenv("US_STARTED_AT_KST", now_kst),
        "ended_at_utc": now_utc,
        "ended_at_et": os.getenv("US_ENDED_AT_ET", now_et),
        "ended_at_kst": os.getenv("US_ENDED_AT_KST", now_kst),
        "wall_elapsed_sec": float(os.getenv("US_WALL_ELAPSED_SEC", "0") or 0),
        "code_version_source": "github_actions" if os.getenv("GITHUB_RUN_ID") else "git_fallback",
    }

NY_TZ = ZoneInfo("America/New_York")


def get_ny_trade_date(force_now: str | None = None) -> str:
    """Get NY-based trade date.
    
    Args:
        force_now: Manual datetime string, e.g. "2026-05-05T09:35:00-04:00"
        
    Returns:
        Trade date string YYYY-MM-DD
    """
    if force_now:
        try:
            dt = datetime.fromisoformat(force_now).astimezone(NY_TZ)
            return dt.strftime("%Y-%m-%d")
        except Exception as exc:
            logger.warning("[US_DAILY_REPORT][WARN] force_now parse failed: %s, using today", exc)
    
    return datetime.now(tz=NY_TZ).strftime("%Y-%m-%d")


def reconcile_order_sources(*, db_orders: int, fills: int, balance_confirmed: int, router_summary: int) -> dict:
    sources = {
        "db_orders": int(db_orders or 0),
        "fills": int(fills or 0),
        "balance_confirmed": int(balance_confirmed or 0),
        "router_summary": int(router_summary or 0),
    }
    orders_ack = max(sources.values())
    warnings = []
    nonzero = [v for v in sources.values() if v > 0]
    if nonzero and len(set(sources.values())) > 1:
        warnings.append("SOURCE_MISMATCH")
        if sources["db_orders"] > 0 and sources["router_summary"] == 0:
            warnings.append("SOURCE_MISMATCH_DB_ORDER_EXISTS_ROUTER_SUMMARY_MISSING")
        if sources["fills"] > 0 and sources["balance_confirmed"] == 0:
            warnings.append("SOURCE_MISMATCH_FILLS_EXIST_BALANCE_CONFIRMATION_MISSING")
        if sources["db_orders"] > sources["fills"]:
            warnings.append("SOURCE_MISMATCH_ACK_EXISTS_FILL_MISSING")
        if sources["router_summary"] == 0 and sources["db_orders"] > 0:
            warnings.append("SOURCE_MISMATCH_SESSION_SUMMARY_OVERWRITTEN_OR_MISSING")
    if sources["fills"] < orders_ack:
        warnings.append("FILL_API_LESS_THAN_ACK")
    return {
        **sources,
        "orders_ack": orders_ack,
        "fill_api_count": sources["fills"],
        "balance_confirmed_count": sources["balance_confirmed"],
        "warnings": warnings,
    }




def _session_summary_paths(report_base: str, trade_date: str, session: str | None) -> tuple[str | None, str | None]:
    if not session:
        return None, None
    base = os.path.join(report_base, trade_date)
    os.makedirs(base, exist_ok=True)
    return (
        os.path.join(base, f"{session}_summary.json"),
        os.path.join(base, f"{session}_summary.md"),
    )


def _canonical_source_summary(*, db_orders: int, fills: int, final_positions: int, open_position_symbols: list, router_summary: int) -> dict:
    sources = {
        "kis_fills_inquire_ccnl": int(fills or 0),
        "kis_final_balance_positions": int(final_positions or 0),
        "db_orders": int(db_orders or 0),
        "router_session_summary": int(router_summary or 0),
    }
    inconsistencies: list[str] = []
    if sources["db_orders"] and sources["kis_fills_inquire_ccnl"] and sources["db_orders"] != sources["kis_fills_inquire_ccnl"]:
        inconsistencies.append("db_orders_fills_mismatch")
    if sources["db_orders"] and sources["router_session_summary"] and sources["db_orders"] != sources["router_session_summary"]:
        inconsistencies.append("db_orders_router_summary_mismatch")
    if sources["kis_final_balance_positions"] == 0 and open_position_symbols:
        inconsistencies.append("positions_zero_but_open_position_symbols_present")
    return {
        "canonical_priority": ["kis_fills_inquire_ccnl", "kis_final_balance", "db_orders", "router_session_summary"],
        "canonical_order_source": "kis_fills_inquire_ccnl" if sources["kis_fills_inquire_ccnl"] else ("db_orders" if sources["db_orders"] else "router_session_summary"),
        "canonical_position_source": "kis_final_balance",
        "source_counts": sources,
        "inconsistencies": inconsistencies,
        "report_consistency": "REPORT_INCONSISTENT" if inconsistencies else "OK",
    }

def run_daily_report(
    env: str = "practice",
    session: str | None = None,
    trade_date: str | None = None,
    offline: bool = False,
) -> dict:
    """Generate US daily report.
    
    Args:
        env: "practice" | "real"
        session: "am" | "afternoon" | "close" (optional)
        trade_date: YYYY-MM-DD or None (auto-detect from force_now or NY now)
        offline: If True, skip DB queries
        
    Returns:
        {"status": "OK" | "ERROR", "report": {...}}
    """
    force_now = os.getenv("FORCE_NOW", "").strip()
    
    # Auto-detect trade_date from force_now or current NY time
    if trade_date is None:
        trade_date = get_ny_trade_date(force_now)
    
    logger.info(
        "[US_DAILY_REPORT][START] env=%s session=%s trade_date=%s force_now=%s",
        env, session, trade_date, force_now or "None"
    )
    
    report: dict = {
        **_report_provenance(session),
        "trade_date": trade_date,
        "session": session,
        "env": env,
        "kis_env": env,
        "broker_environment": env,
        "environment_notice": (
            "모의투자 주문이며 실계좌/MTS에는 표시되지 않음"
            if str(env).lower() in {"practice", "paper", "mock"}
            else "실계좌 주문 환경"
        ),
        "dry_run": None,
        "orders_ack": 0,
        "orders_submitted_total": 0,
        "orders_sent_total": 0,
        "orders_ack_total": 0,
        "orders_balance_confirmed_total": 0,
        "buy_order_count": 0,
        "sell_order_count": 0,
        "orders_dry_run": 0,
        "orders_blocked": 0,
        "orders_rejected": 0,
        "orders_rejected_total": 0,
        "orders_unresolved_total": 0,
        "orders_submitted": 0,
        "broker_fill_confirmed": 0,
        "balance_delta_confirmed": 0,
        "ack_only_unresolved": 0,
        "broker_orderable_cash_blocks": 0,
        "broker_orderable_cash_unknown_blocks": 0,
        "broker_orderable_qty_blocks": 0,
        "broker_position_mismatch": 0,
        "cash_exhausted": False,
        "buy_notional_total": 0.0,
        "sell_notional_total": 0.0,
        "actual_new_positions": 0,
        "open_position_count": 0,
        "account_equity_krw": 0.0,
        "account_equity_usd": 0.0,
        "invested_market_value_usd": 0.0,
        "cash_usd": 0.0,
        "gross_exposure_pct": 0.0,
        "target_exposure_pct": 0.0,
        "max_exposure_pct": 0.0,
        "min_cash_buffer_pct": 0.0,
        "deployment_gap_usd": 0.0,
        "deployable_cash_usd": 0.0,
        "allowed_new_buy_usd": 0.0,
        "capital_deployment_action": "NORMAL",
        "position_count": 0,
        "max_positions": int(os.getenv("US_MAX_POSITIONS", "35") or 35),
        "available_new_slots": 0,
        "avg_position_value_usd": 0.0,
        "positions_below_target_weight": 0,
        "add_to_existing_candidates": 0,
        "new_symbol_slots_available": 0,
        "underdeployed": False,
        "full_position": False,
        "orders_disabled": 0,
        "orders_signal_only": 0,
        "fills": 0,
        "fills_count": 0,
        "real_broker_buys": 0,
        "real_broker_sells": 0,
        "synthetic_reconcile_buys": 0,
        "synthetic_reconcile_sells": 0,
        "fill_api_count": 0,
        "balance_confirmed_count": 0,
        "positions": 0,
        "watchlist_raw_count": 0,
        "watchlist_unique_count": 0,
        "watchlist_duplicate_count": 0,
        "score_nonzero_count": 0,
        "score_zero_count": 0,
        "score_missing_count": 0,
        "score_nonzero_ratio": 0.0,
        "score_contract_ok": None,
        "prep_status": None,
        "market_state": "UNKNOWN",
        "defense_regime": "NONE",
        "risk_on_regime": "NONE",
        "market_state_reasons": [],
        "exposure_multiplier": 1.0,
        "effective_budget_before_overlay": 0.0,
        "effective_budget_after_overlay": 0.0,
        "defense_entry_blocked_count": 0,
        "defense_trim_count": 0,
        "defense_trim_notional": 0.0,
        "partial_take_profit_count": 0,
        "partial_take_profit_notional": 0.0,
        "runner_positions_count": 0,
        "trailing_stop_mode": "normal",
        "profit_locked_notional": 0.0,
        "account_loss_kill_switch_triggered": False,
        "forbidden_hedge_block_count": 0,
        "prep_trade_can_proceed": None,
        "kis_retry_count": 0,
        "warnings": [],
        "errors": [],
        "order_final_classification": [],
        "order_final_classification_counts": {},
        "entry_degraded": 0,
        "entry_degraded_reason": "",
        "entry_watchlist_source": "",
        "watchlist_fallback_used": 0,
        "exit_routed_before_entry": 0,
        "buy_notional_routed": 0.0,
        "sell_notional_routed": 0.0,
        "total_order_notional_routed": 0.0,
        "buy_daily_notional_after_routing": 0.0,
        "sell_notional_does_not_consume_buy_budget": 0,
        "ack_reconcile_before_route_status": "",
        "ack_reconcile_after_route_status": "",
        "ack_reconcile_after_route_unresolved_count": 0,
        "ack_pending_reconcile_count": 0,
        "pending_order_count": 0,
        "open_position_symbols": [],
        "canonical_sources": {},
        "report_consistency": "OK",
        "rotation_regime": "UNKNOWN",
        "portfolio_cluster_weights": {},
        "cluster_exposure": {},
        "cap_violations": [],
    }
    
    # DRY_RUN
    try:
        from trader.utils.env import env_bool
        report["dry_run"] = env_bool("DRY_RUN", default=True)
    except Exception:
        pass
    
    if offline:
        logger.info("[US_DAILY_REPORT][OFFLINE] skipping DB queries")
    else:
        # DB queries
        try:
            try:
                from trader.us.db.repos import load_locked_us_watchlist, load_positions, load_us_prep_status, load_us_daily_orders_for_report
            except Exception as exc:
                logger.warning("[US_DAILY_REPORT][WARN] optional repo imports failed: %s", exc)
                load_locked_us_watchlist = lambda _td: []
                load_positions = lambda as_of=None: []
                load_us_prep_status = lambda _td: None
                load_us_daily_orders_for_report = lambda _td: []
            try:
                from trader.us.score_columns import collect_us_score_nonzero_stats
            except Exception:
                collect_us_score_nonzero_stats = lambda rows: {"score_nonzero": 0, "score_zero": 0, "score_missing": 0, "score_nonzero_ratio": 0.0}

            # Watchlist
            try:
                watchlist = load_locked_us_watchlist(trade_date)
                report["watchlist_raw_count"] = len(watchlist)
                
                if watchlist:
                    # Dedupe to count unique symbols
                    unique_symbols = set(row.get("symbol") for row in watchlist if row.get("symbol"))
                    report["watchlist_unique_count"] = len(unique_symbols)
                    report["watchlist_duplicate_count"] = len(watchlist) - len(unique_symbols)
                    
                    # Score stats
                    stats = collect_us_score_nonzero_stats(watchlist)
                    report["score_nonzero_count"] = stats["score_nonzero"]
                    report["score_zero_count"] = stats["score_zero"]
                    report["score_missing_count"] = stats["score_missing"]
                    report["score_nonzero_ratio"] = stats["score_nonzero_ratio"]
            except Exception as exc:
                report["warnings"].append(f"watchlist_load_failed: {exc}")
                logger.warning("[US_DAILY_REPORT][WARN] watchlist load failed: %s", exc)
            
            # Prep status
            try:
                prep_status_result = load_us_prep_status(trade_date)
                if prep_status_result:
                    report["prep_status"] = prep_status_result.get("status")
                    # Check_if prep allows trade to proceed
                    result_data = prep_status_result.get("result") or {}
                    if isinstance(result_data, str):
                        try:
                            result_data = json.loads(result_data)
                        except Exception:
                            result_data = {}
                    report["prep_trade_can_proceed"] = result_data.get("trade_can_proceed")
                    report["score_contract_ok"] = result_data.get("score_contract_ok")
                    report["rotation_regime"] = result_data.get("rotation_regime") or result_data.get("rotation_context", {}).get("rotation_regime") or report.get("rotation_regime")
                    report["portfolio_cluster_weights"] = result_data.get("portfolio_cluster_weights") or report.get("portfolio_cluster_weights")
                    report["cap_violations"] = result_data.get("cap_violations") or report.get("cap_violations")
                    for key in ("market_state", "defense_regime", "risk_on_regime", "market_state_reasons", "exposure_multiplier", "trailing_stop_mode", "account_loss_kill_switch_triggered"):
                        if key in result_data:
                            report[key] = result_data.get(key)
            except Exception as exc:
                report["warnings"].append(f"prep_status_load_failed: {exc}")
                logger.warning("[US_DAILY_REPORT][WARN] prep status load failed: %s", exc)
            
            # Orders - count by status
            try:
                all_orders_today = load_us_daily_orders_for_report(trade_date)
                if all_orders_today:
                    for order in all_orders_today:
                        status = order.get("status", "").upper()
                        side = str(order.get("side", "")).upper()
                        if side == "BUY":
                            report["buy_order_count"] += 1
                        elif side == "SELL":
                            report["sell_order_count"] += 1
                        meta = order.get("meta") or {}
                        reason = str((meta.get("reason") if isinstance(meta, dict) else "") or order.get("reason") or "")
                        if "FORBIDDEN_HEDGE_OR_INVERSE_ETF" in reason:
                            report["forbidden_hedge_block_count"] += 1
                        if "DEFENSE_" in reason and "ENTRY" in reason:
                            report["defense_entry_blocked_count"] += 1
                        if "DEFENSE_" in reason and "TRIM" in reason:
                            report["defense_trim_count"] += 1
                            report["defense_trim_notional"] += float(order.get("notional_usd") or order.get("notional") or 0)
                        if "TAKE_PROFIT_TP" in reason:
                            report["partial_take_profit_count"] += 1
                            report["partial_take_profit_notional"] += float(order.get("notional_usd") or order.get("notional") or 0)
                            report["profit_locked_notional"] += float(order.get("notional_usd") or order.get("notional") or 0)
                        if status in {"SUBMITTED", "SENT"}:
                            report["orders_submitted_total"] += 1
                            report["orders_submitted"] += 1
                        elif status in {"ACK", "ACKED", "ACCEPTED"}:
                            report["orders_ack_total"] += 1
                            report["ack_only_unresolved"] += 1
                        elif status in {"FILLED", "PARTIALLY_FILLED"}:
                            report["orders_ack_total"] += 1
                            report["broker_fill_confirmed"] += 1
                        elif status in {"BALANCE_CONFIRMED", "BALANCE_DELTA_CONFIRMED"}:
                            report["orders_balance_confirmed_total"] += 1
                            report["balance_delta_confirmed"] += 1
                        elif status == "DRY_RUN":
                            report["orders_dry_run"] += 1
                        elif status == "BLOCKED" and "broker_orderable_cash_unavailable" in reason:
                            report["orders_blocked"] += 1
                            report["broker_orderable_cash_blocks"] += 1
                            report["broker_orderable_cash_unknown_blocks"] += 1
                        elif status == "BLOCKED" and "broker_orderable_cash_insufficient" in reason:
                            report["orders_blocked"] += 1
                            report["broker_orderable_cash_blocks"] += 1
                            report["cash_exhausted"] = True
                        elif status == "BLOCKED" and "broker_orderable_qty_zero" in reason:
                            report["orders_blocked"] += 1
                            report["broker_orderable_qty_blocks"] += 1
                        elif status == "BLOCKED":
                            report["orders_blocked"] += 1
                        elif status == "REJECTED":
                            report["orders_rejected"] += 1
                            report["orders_rejected_total"] += 1
                            if "broker_orderable_cash_unavailable" in reason:
                                report["broker_orderable_cash_blocks"] += 1
                                report["broker_orderable_cash_unknown_blocks"] += 1
                            if "broker_orderable_cash_insufficient" in reason or "주문가능금액" in reason:
                                report["broker_orderable_cash_blocks"] += 1
                                report["cash_exhausted"] = True
                            if "잔고내역" in reason or "broker_position_mismatch" in reason:
                                report["broker_position_mismatch"] += 1
                        elif status in {"ACK_UNRESOLVED", "ACK_STALE_UNRESOLVED", "ACK_PENDING_RECONCILE"}:
                            report["orders_unresolved_total"] += 1
                            report["ack_only_unresolved"] += 1
                        elif status == "ORDER_DISABLED":
                            report["orders_disabled"] += 1
                        elif status == "SIGNAL_ONLY":
                            report["orders_signal_only"] += 1
                    report["orders_sent_total"] = report["orders_submitted_total"] + report["orders_ack_total"]
                    report["orders_ack"] = report["orders_ack_total"]
                    report["orders_submitted"] = report["orders_submitted_total"]
            except Exception as exc:
                report["warnings"].append(f"orders_load_failed: {exc}")
                logger.warning("[US_DAILY_REPORT][WARN] orders load failed: %s", exc)
            
            # Fills
            try:
                fill_breakdown = load_us_fills_breakdown(trade_date)
                report.update(fill_breakdown)
                report["fills"] = fill_breakdown["fills_count"]
                logger.info(
                    "[US_DAILY_REPORT][ORDER_COUNTS] submitted=%d ack=%d balance_confirmed=%d sent_total=%d fills=%d",
                    report["orders_submitted_total"], report["orders_ack_total"],
                    report["orders_balance_confirmed_total"], report["orders_sent_total"],
                    report["fills"],
                )
            except Exception as exc:
                report["warnings"].append(f"fills_load_failed: {exc}")
                logger.warning("[US_DAILY_REPORT][WARN] fills load failed: %s", exc)
            
            # Positions
            try:
                positions = load_positions(as_of=trade_date)
                report["positions"] = len(positions)
                report["position_count"] = len(positions)
                report["open_position_count"] = len(positions)
                report["open_position_symbols"] = sorted({str(p.get("symbol") or "").upper() for p in positions if p.get("symbol")})
                report["available_new_slots"] = max(0, int(report.get("max_positions", 35) or 35) - len(positions))
                report["new_symbol_slots_available"] = report["available_new_slots"]
                report["full_position"] = report["available_new_slots"] <= 0
                invested = 0.0
                for pos in positions:
                    try:
                        invested += float(pos.get("market_value_usd") or pos.get("market_value") or pos.get("eval_amount_usd") or pos.get("total_pvs_usd") or pos.get("eval_amount") or 0)
                    except (TypeError, ValueError):
                        pass
                try:
                    from trader.us.rotation import apply_cap_flags, compute_cluster_exposure
                    regime = str(report.get("rotation_regime") or "NEUTRAL")
                    report["cluster_exposure"] = apply_cap_flags(compute_cluster_exposure(positions), regime)
                    if not report.get("portfolio_cluster_weights"):
                        report["portfolio_cluster_weights"] = report["cluster_exposure"]
                    if not report.get("cap_violations"):
                        report["cap_violations"] = [c for c, v in report["cluster_exposure"].items() if v.get("over_cap")]
                except Exception as exc:
                    logger.warning("[US_DAILY_REPORT][CLUSTER][WARN] %s", exc)
                try:
                    from trader.us.capital_deployment import compute_deployment_metrics, decide_deployment_action
                    metrics = compute_deployment_metrics(account_equity_usd=float(os.getenv("US_ACCOUNT_EQUITY_USD", "0") or 0), invested_market_value_usd=invested, cash_usd=None)
                    report.update(metrics)
                    report["capital_deployment_action"] = decide_deployment_action(metrics, position_count=len(positions), max_positions=int(report.get("max_positions", 35) or 35))
                    report["avg_position_value_usd"] = invested / len(positions) if positions else 0.0
                    if report["full_position"] and metrics.get("underdeployed"):
                        report["warnings"].append("US_CAPITAL_UNDERDEPLOYED_FULL_POSITION")
                except Exception as exc:
                    logger.warning("[US_DAILY_REPORT][CAPITAL][WARN] %s", exc)
            except Exception as exc:
                report["warnings"].append(f"positions_load_failed: {exc}")
                logger.warning("[US_DAILY_REPORT][WARN] positions load failed: %s", exc)

            db_ack = int(report.get("orders_ack", 0) or 0)
            fill_count = int(report.get("fills", 0) or 0)
            balance_confirmed = load_balance_confirmed_count(trade_date)
            router_summary = load_router_summary_ack_count(trade_date, session=session)
            schedule_fallback = load_schedule_health_fallback(trade_date, session=session)
            if router_summary == 0 and int(schedule_fallback.get("orders_ack") or 0) > 0:
                report["warnings"].append("SOURCE_MISMATCH_ROUTER_SUMMARY_ZERO_USING_SCHEDULE_HEALTH")
                router_summary = int(schedule_fallback.get("orders_ack") or 0)
                report["orders_ack"] = max(db_ack, router_summary)
                report["orders_ack_total"] = max(db_ack, router_summary)
                report["buy_notional_routed"] = max(float(report.get("buy_notional_routed") or 0.0), float(schedule_fallback.get("buy_notional_routed") or 0.0))
                report["sell_notional_routed"] = max(float(report.get("sell_notional_routed") or 0.0), float(schedule_fallback.get("sell_notional_routed") or 0.0))
                report["total_order_notional_routed"] = max(float(report.get("total_order_notional_routed") or 0.0), float(schedule_fallback.get("total_order_notional_routed") or 0.0))
                report["buy_notional_total"] = max(float(report.get("buy_notional_total") or 0.0), float(report.get("buy_notional_routed") or 0.0))
                report["sell_notional_total"] = max(float(report.get("sell_notional_total") or 0.0), float(report.get("sell_notional_routed") or 0.0))
                report["notional_total_source_note"] = "buy_notional_total/sell_notional_total synchronized from routed fallback; prefer *_routed fields"
                report["deprecated_notional_total_fields"] = ["buy_notional_total", "sell_notional_total"]
                db_ack = int(report.get("orders_ack") or 0)
            report["source_numbers"] = {"db_orders": db_ack, "kis_fills": fill_count, "router_session_summary": router_summary, "schedule_health_fallback": schedule_fallback, "final_balance_positions": int(report.get("positions", 0) or 0)}
            reconciled = reconcile_order_sources(db_orders=db_ack, fills=fill_count, balance_confirmed=balance_confirmed, router_summary=router_summary)
            # Canonical daily counts come from US order rows for submitted/ACK and unique fills for executions.
            report["orders_ack"] = db_ack
            report["orders_ack_total"] = db_ack
            report["fill_api_count"] = reconciled["fill_api_count"]
            report["balance_confirmed_count"] = reconciled["balance_confirmed_count"]
            logger.info("[US_DAILY_REPORT][ORDER_SOURCES] db_orders=%s fills=%s balance_confirmed=%s router_summary=%s", db_ack, fill_count, balance_confirmed, router_summary)
            if reconciled["orders_ack"] == 0 and fill_count == 0 and balance_confirmed == 0 and router_summary == 0:
                reconciled["warnings"].append("ORDER_SOURCE_EMPTY")
            for warn in reconciled["warnings"]:
                report["warnings"].append(warn)
                logger.warning("[US_DAILY_REPORT][RECONCILE_WARN] reason=%s db_orders=%s fills=%s balance_confirmed=%s router_summary=%s", warn, db_ack, fill_count, balance_confirmed, router_summary)

            report["canonical_sources"] = _canonical_source_summary(
                db_orders=db_ack,
                fills=fill_count,
                final_positions=int(report.get("positions", 0) or 0),
                open_position_symbols=report.get("open_position_symbols") or [],
                router_summary=router_summary,
            )
            if report["canonical_sources"].get("report_consistency") == "REPORT_INCONSISTENT":
                report["warnings"].append("REPORT_INCONSISTENT:" + ",".join(report["canonical_sources"].get("inconsistencies") or []))

            # Close-session final balance delta classification
            if session == "close":
                try:
                    from trader.us.data_provider import USDataProvider
                    from trader.us.execution.reconcile import classify_ack_orders_with_final_balance
                    close_provider = USDataProvider(offline=offline)
                    close_class = classify_ack_orders_with_final_balance(provider=close_provider, trade_date=trade_date, env=env)
                    report["order_final_classification"] = close_class.get("orders", [])
                    report["order_final_classification_counts"] = close_class.get("counts", {})
                    if close_class.get("pending_order_count") is not None:
                        report["pending_order_count"] = int(close_class.get("pending_order_count") or 0)
                except Exception as exc:
                    report["warnings"].append(f"close_balance_delta_classification_failed: {exc}")
                    logger.warning("[US_DAILY_REPORT][WARN] close balance delta classification failed: %s", exc)
        
        except Exception as exc:
            report["errors"].append(f"DB_query_failed: {exc}")
            logger.error("[US_DAILY_REPORT][ERROR] DB query failed: %s", exc)
    
    # Budget cap
    try:
        from trader.us.budget import get_us_capital_usd_cap
        budget_cap_usd = round(get_us_capital_usd_cap(), 2)
        report["budget_cap_usd"] = budget_cap_usd
    except Exception:
        pass
    
    # Save reports
    report_base = os.getenv("US_DAILY_REPORT_BASE", "reports/us_daily")
    os.makedirs(report_base, exist_ok=True)

    # Latest report (always overwrite)
    latest_md_path = f"{report_base}/latest_us_daily_report.md"
    latest_json_path = f"{report_base}/latest_us_daily_report.json"
    session_summary_json_path, session_summary_md_path = _session_summary_paths(report_base, trade_date, session)
    
    # Dated report (for history)
    dated_dir = f"{report_base}/{trade_date}"
    if session:
        dated_dir = f"{dated_dir}/{session}"
    os.makedirs(dated_dir, exist_ok=True)
    dated_md_path = f"{dated_dir}/us_daily_report.md"
    dated_json_path = f"{dated_dir}/us_daily_report.json"
    
    # Generate markdown
    md_lines = [
        f"# US Daily Report — {trade_date}",
        "",
    ]
    
    md_lines.extend([
        f"> **KIS_ENV: {str(env).upper()}**",
        f"> {report.get('environment_notice', '')}",
        "",
    ])

    if session:
        md_lines.append(f"**Session**: {session.upper()}")
        md_lines.append("")

    if int(report.get("positions", 0) or 0) == 0 and report.get("open_position_symbols"):
        report["errors"].append("REPORT_VALIDATION_FAILED: positions_zero_but_open_position_symbols_present")
    if str(report.get("started_at_utc") or "") == str(report.get("ended_at_utc") or "") and float(report.get("wall_elapsed_sec") or 0) > 1:
        report["errors"].append("REPORT_VALIDATION_FAILED: identical_start_end_with_elapsed")
    if any(str(w).startswith("SOURCE_MISMATCH") or str(w).startswith("REPORT_INCONSISTENT") for w in report.get("warnings", [])):
        report["report_consistency"] = "SOURCE_MISMATCH"
    else:
        report["report_consistency"] = (report.get("canonical_sources") or {}).get("report_consistency", "OK")

    if report.get("report_consistency") == "SOURCE_MISMATCH":
        report["status"] = "WARNING_RECONCILE_MISMATCH"
    elif report["errors"]:
        report["status"] = "FAILED_RECONCILE"
    elif int(report.get("orders_unresolved_total", 0) or 0) > 0:
        report["status"] = "WARNING_RECONCILE_MISMATCH"
    else:
        report["status"] = "OK"

    if report.get("report_consistency") == "SOURCE_MISMATCH":
        md_lines.extend(["# ⚠️ SOURCE_MISMATCH", "", f"source_counts={(report.get('canonical_sources') or {}).get('source_counts', {})}", ""])

    md_lines.extend([
        "## Runtime Metadata",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| trade_date | {trade_date} |",
        f"| branch | {report.get('branch')} |",
        f"| commit_sha | {report.get('commit_sha')} |",
        f"| workflow | {report.get('workflow')} |",
        f"| run_id | {report.get('run_id')} |",
        f"| session | {session or 'N/A'} |",
        f"| env | {env} |",
        f"| KIS_ENV | {report.get('kis_env')} |",
        f"| environment_notice | {report.get('environment_notice')} |",
        f"| dry_run | {report['dry_run']} |",
        f"| force_now | {force_now or 'N/A'} |",
        "",
        "## Trading Summary",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| report_status | {report.get('status')} |",
        f"| market_state | {report.get('market_state')} |",
        f"| defense_regime | {report.get('defense_regime')} |",
        f"| risk_on_regime | {report.get('risk_on_regime')} |",
        f"| market_state_reasons | {report.get('market_state_reasons')} |",
        f"| exposure_multiplier | {report.get('exposure_multiplier')} |",
        f"| effective_budget_before_overlay | {report.get('effective_budget_before_overlay')} |",
        f"| effective_budget_after_overlay | {report.get('effective_budget_after_overlay')} |",
        f"| 신규매수 허용/차단 | {report.get('market_state')} / blocked={report.get('defense_entry_blocked_count')} |",
        f"| 방어 trim | count={report.get('defense_trim_count')} notional={report.get('defense_trim_notional')} |",
        f"| 부분익절 | count={report.get('partial_take_profit_count')} notional={report.get('partial_take_profit_notional')} |",
        f"| trailing_stop_mode | {report.get('trailing_stop_mode')} |",
        f"| account_loss_kill_switch_triggered | {report.get('account_loss_kill_switch_triggered')} |",
        f"| forbidden_hedge_block_count | {report.get('forbidden_hedge_block_count')} |",
        f"| report_consistency | {report.get('report_consistency')} |",
        f"| canonical_order_source | {(report.get('canonical_sources') or {}).get('canonical_order_source', '')} |",
        f"| canonical_position_source | {(report.get('canonical_sources') or {}).get('canonical_position_source', '')} |",
        f"| orders_submitted_total | {report.get('orders_submitted_total', 0)} |",
        f"| orders_ack_total | {report.get('orders_ack_total', 0)} |",
        f"| orders_rejected_total | {report.get('orders_rejected_total', 0)} |",
        f"| orders_unresolved_total | {report.get('orders_unresolved_total', 0)} |",
        f"| buy_notional_total | {report.get('buy_notional_total', 0)} |",
        f"| sell_notional_total | {report.get('sell_notional_total', 0)} |",
        f"| actual_new_positions | {report.get('actual_new_positions', 0)} |",
        f"| open_position_count | {report.get('open_position_count', 0)} |",
        f"| account_equity_krw | {report.get('account_equity_krw', 0)} |",
        f"| account_equity_usd | {report.get('account_equity_usd', 0)} |",
        f"| invested_market_value_usd | {report.get('invested_market_value_usd', 0)} |",
        f"| cash_usd | {report.get('cash_usd', 0)} |",
        f"| gross_exposure_pct | {report.get('gross_exposure_pct', 0)} |",
        f"| target_exposure_pct | {report.get('target_exposure_pct', 0)} |",
        f"| max_exposure_pct | {report.get('max_exposure_pct', 0)} |",
        f"| min_cash_buffer_pct | {report.get('min_cash_buffer_pct', 0)} |",
        f"| deployment_gap_usd | {report.get('deployment_gap_usd', 0)} |",
        f"| deployable_cash_usd | {report.get('deployable_cash_usd', 0)} |",
        f"| allowed_new_buy_usd | {report.get('allowed_new_buy_usd', 0)} |",
        f"| capital_deployment_action | {report.get('capital_deployment_action', '')} |",
        f"| max_positions | {report.get('max_positions', 0)} |",
        f"| available_new_slots | {report.get('available_new_slots', 0)} |",
        f"| avg_position_value_usd | {report.get('avg_position_value_usd', 0)} |",
        f"| underdeployed | {report.get('underdeployed', False)} |",
        f"| full_position | {report.get('full_position', False)} |",
        f"| orders_submitted | {report.get('orders_submitted', 0)} |",
        f"| orders_ack | {report['orders_ack']} |",
        f"| broker_fill_confirmed | {report.get('broker_fill_confirmed', 0)} |",
        f"| balance_delta_confirmed | {report.get('balance_delta_confirmed', 0)} |",
        f"| ack_only_unresolved | {report.get('ack_only_unresolved', 0)} |",
        f"| broker_orderable_cash_blocks | {report.get('broker_orderable_cash_blocks', 0)} |",
        f"| broker_orderable_cash_unknown_blocks | {report.get('broker_orderable_cash_unknown_blocks', 0)} |",
        f"| broker_orderable_qty_blocks | {report.get('broker_orderable_qty_blocks', 0)} |",
        f"| broker_position_mismatch | {report.get('broker_position_mismatch', 0)} |",
        f"| cash_exhausted | {report.get('cash_exhausted', False)} |",
        f"| orders_dry_run | {report['orders_dry_run']} |",
        f"| orders_blocked | {report['orders_blocked']} |",
        f"| orders_rejected | {report['orders_rejected']} |",
        f"| orders_disabled | {report['orders_disabled']} |",
        f"| orders_signal_only | {report['orders_signal_only']} |",
        f"| fills | {report['fills']} |",
        f"| fill_api_count | {report['fill_api_count']} |",
        f"| balance_confirmed_count | {report['balance_confirmed_count']} |",
        f"| pending_order_count | {report.get('pending_order_count', 0)} |",
        f"| buy_notional_routed | {report.get('buy_notional_routed', 0)} |",
        f"| sell_notional_routed | {report.get('sell_notional_routed', 0)} |",
        f"| total_order_notional_routed | {report.get('total_order_notional_routed', 0)} |",
        f"| ack_reconcile_after_route_status | {report.get('ack_reconcile_after_route_status', '')} |",
        f"| ack_pending_reconcile_count | {report.get('ack_pending_reconcile_count', 0)} |",
        f"| positions | {report['positions']} |",
        f"| rotation_regime | {report.get('rotation_regime', 'UNKNOWN')} |",
        f"| cap_violations | {report.get('cap_violations', [])} |",
        "",
        "## Watchlist & Score Contract",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| watchlist_raw_count | {report['watchlist_raw_count']} |",
        f"| watchlist_unique_count | {report['watchlist_unique_count']} |",
        f"| watchlist_duplicate_count | {report['watchlist_duplicate_count']} |",
        f"| score_nonzero | {report['score_nonzero_count']} |",
        f"| score_zero | {report['score_zero_count']} |",
        f"| score_missing | {report['score_missing_count']} |",
        f"| score_nonzero_ratio | {report['score_nonzero_ratio']:.4f} |",
        f"| score_contract_ok | {report['score_contract_ok']} |",
        "",
        "## Prep Status",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| prep_status | {report['prep_status'] or 'N/A'} |",
        f"| prep_trade_can_proceed | {report['prep_trade_can_proceed']} |",
        "",
        "## Cluster Exposure & Rotation",
        "",
        f"**rotation_regime**: {report.get('rotation_regime', 'UNKNOWN')}",
        "",
        "| Cluster | Market Value | Weight | Unrealized PNL | 1D PNL | Cap | Over Cap |",
        "|---|---:|---:|---:|---:|---:|---|",
    ])
    for cluster, row in sorted((report.get('portfolio_cluster_weights') or report.get('cluster_exposure') or {}).items()):
        md_lines.append(
            f"| {cluster} | {float(row.get('cluster_market_value', 0.0)):.2f} | {float(row.get('cluster_weight', 0.0)):.4f} | "
            f"{float(row.get('cluster_unrealized_pnl', 0.0)):.2f} | {float(row.get('cluster_1d_pnl', 0.0)):.2f} | "
            f"{float(row.get('cluster_cap', 0.0)):.4f} | {row.get('over_cap', False)} |"
        )
    md_lines.append("")
    

    if report.get("order_final_classification"):
        md_lines.extend([
            "## Order Final Classification",
            "",
            "| time | side | symbol | qty | order_no | ack_status | fill_api_status | balance_delta_status | final_status | price_source | pnl_if_sell |",
            "|---|---|---|---:|---|---|---|---|---|---|---:|",
        ])
        for row in report.get("order_final_classification", []):
            md_lines.append(
                f"| {row.get('time', '')} | {row.get('side', '')} | {row.get('symbol', '')} | {row.get('qty', 0)} | "
                f"{row.get('order_no', '')} | {row.get('ack_status', '')} | {row.get('fill_api_status', '')} | "
                f"{row.get('balance_delta_status', '')} | {row.get('final_status', '')} | {row.get('price_source', '')} | {row.get('pnl_if_sell', '')} |"
            )
        md_lines.append("")

    if report["warnings"]:
        md_lines.append("## Warnings")
        md_lines.append("")
        for warn in report["warnings"]:
            md_lines.append(f"-{warn}")
        md_lines.append("")
    
    if report["errors"]:
        md_lines.append("## Errors")
        md_lines.append("")
        for err in report["errors"]:
            md_lines.append(f"- {err}")
        md_lines.append("")
    
    md_content = "\n".join(md_lines)
    
    # Write reports
    try:
        if session_summary_md_path and session_summary_json_path:
            with open(session_summary_md_path, "w") as f:
                f.write(md_content)
            with open(session_summary_json_path, "w") as f:
                json.dump(report, f, indent=2, default=str)
        if session in (None, "close"):
            with open(latest_md_path, "w") as f:
                f.write(md_content)
            with open(latest_json_path, "w") as f:
                json.dump(report, f, indent=2, default=str)
        with open(dated_md_path, "w") as f:
            f.write(md_content)
        with open(dated_json_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(
            "[US_DAILY_REPORT][SAVED] latest=%s dated=%s",
            latest_md_path, dated_md_path
        )
    except Exception as exc:
        logger.error("[US_DAILY_REPORT][SAVE_FAILED] %s", exc)
        report["errors"].append(f"report_save_failed: {exc}")
    
    logger.info(
        "[US_DAILY_REPORT][OK] date=%s session=%s orders_ack=%d",
        trade_date, session or "N/A", report["orders_ack"]
    )
    
    return {"status": report.get("status", "OK" if not report["errors"] else "ERROR"), "report": report}


def _dict_rows(result) -> list[dict]:
    return [dict(getattr(r, "_mapping", r)) for r in result]


def _read_autocommit(engine, sql: str, params: dict) -> list[dict]:
    from sqlalchemy import text
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        return _dict_rows(conn.execute(text(sql), params))


def _ny_date_bounds_utc(trade_date: str) -> tuple[str, str]:
    from datetime import date, datetime, time, timedelta
    from zoneinfo import ZoneInfo
    ny = ZoneInfo("America/New_York")
    utc = ZoneInfo("UTC")
    d = date.fromisoformat(trade_date)
    start = datetime.combine(d, time.min, tzinfo=ny).astimezone(utc).isoformat()
    end = datetime.combine(d + timedelta(days=1), time.min, tzinfo=ny).astimezone(utc).isoformat()
    return start, end


def load_us_fills_count(trade_date: str) -> int:
    """Count fill API-confirmed fills from fills/us_fills tables, not sold-symbol proxy."""
    from trader.us.db.repos import _get_engine_or_none
    engine = _get_engine_or_none()
    if engine is None:
        return 0
    queries = [
        "SELECT COUNT(*) AS n FROM us_fills WHERE trade_date = :td",
    ]
    for sql in queries:
        try:
            rows = _read_autocommit(engine, sql, {"td": trade_date})
            if rows:
                return int(rows[0].get("n") or 0)
        except Exception as exc:
            logger.debug("[US_FILLS][LOAD][FALLBACK] sql=%s err=%s", sql, exc)
    return int(os.getenv("US_DAILY_FILL_API_COUNT", "0") or 0) if os.getenv("PYTEST_CURRENT_TEST") else 0


def load_us_fills_breakdown(trade_date: str) -> dict:
    """Count unique fills by broker/synthetic source and side."""
    from trader.us.db.repos import _get_engine_or_none
    result = {
        "fills_count": 0,
        "real_broker_buys": 0,
        "real_broker_sells": 0,
        "synthetic_reconcile_buys": 0,
        "synthetic_reconcile_sells": 0,
    }
    engine = _get_engine_or_none()
    if engine is None:
        result["fills_count"] = load_us_fills_count(trade_date)
        return result
    try:
        rows = _read_autocommit(
            engine,
            """
            SELECT side, COALESCE(meta->>'fill_source', meta->>'source', '') AS fill_source, COUNT(*) AS n
            FROM us_fills
            WHERE trade_date = :td
            GROUP BY side, COALESCE(meta->>'fill_source', meta->>'source', '')
            """,
            {"td": trade_date},
        )
        for row in rows:
            side = str(row.get("side") or "").upper()
            source = str(row.get("fill_source") or "").lower()
            n = int(row.get("n") or 0)
            result["fills_count"] += n
            is_synthetic = "synthetic" in source or "reconcile" in source
            if side == "BUY":
                result["synthetic_reconcile_buys" if is_synthetic else "real_broker_buys"] += n
            elif side == "SELL":
                result["synthetic_reconcile_sells" if is_synthetic else "real_broker_sells"] += n
    except Exception as exc:
        logger.debug("[US_FILLS][BREAKDOWN][FALLBACK] err=%s", exc)
        result["fills_count"] = load_us_fills_count(trade_date)
    return result


def load_balance_confirmed_count(trade_date: str) -> int:
    """Count balance-confirmed orders/positions from persisted reconciliation state."""
    from trader.us.db.repos import _get_engine_or_none
    engine = _get_engine_or_none()
    if engine is None:
        return 0
    queries = [
        "SELECT COUNT(*) AS n FROM us_orders WHERE trade_date = :td AND UPPER(COALESCE(state, status, '')) = 'BALANCE_CONFIRMED'",
        "SELECT COUNT(*) AS n FROM us_positions WHERE as_of = :td AND COALESCE(qty, quantity, 0) > 0",
    ]
    for sql in queries:
        try:
            rows = _read_autocommit(engine, sql, {"td": trade_date})
            if rows and int(rows[0].get("n") or 0) > 0:
                return int(rows[0].get("n") or 0)
        except Exception as exc:
            logger.debug("[US_BALANCE_CONFIRMED][LOAD][FALLBACK] sql=%s err=%s", sql, exc)
    return int(os.getenv("US_DAILY_BALANCE_CONFIRMED_COUNT", "0") or 0) if os.getenv("PYTEST_CURRENT_TEST") else 0


def load_schedule_health_fallback(trade_date: str, session: str | None = None) -> dict:
    path = os.path.abspath(f"reports/us_schedule_health/{trade_date}.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
        candidates = []
        if session:
            candidates.extend([payload.get(session), (payload.get("sessions") or {}).get(session), (payload.get("session_aggregate") or {}).get(session)])
        candidates.extend([payload.get("aggregate"), payload.get("totals"), payload])
        for row in candidates:
            if isinstance(row, dict) and any(k in row for k in ("orders_ack", "fills_count", "buy_notional_routed", "sell_notional_routed")):
                return {
                    "orders_ack": int(row.get("orders_ack") or row.get("ack") or 0),
                    "fills_count": int(row.get("fills_count") or row.get("fills") or 0),
                    "buy_notional_routed": float(row.get("buy_notional_routed") or 0.0),
                    "sell_notional_routed": float(row.get("sell_notional_routed") or 0.0),
                    "total_order_notional_routed": float(row.get("total_order_notional_routed") or (float(row.get("buy_notional_routed") or 0.0) + float(row.get("sell_notional_routed") or 0.0))),
                }
    except Exception as exc:
        logger.warning("[US_DAILY_REPORT][SCHEDULE_HEALTH_FALLBACK][WARN] path=%s err=%s", path, exc)
    return {}


def load_router_summary_ack_count(trade_date: str, session: str | None = None) -> int:
    """Read order-router summary artifacts instead of production env vars."""
    candidates = [
        f"runtime/us/order_router_summary/{trade_date}/{session or 'all'}.json",
        f"runtime/us/order_router_summary/{trade_date}/latest.json",
        "runtime/us/order_router_summary/latest.json",
        "runtime/us_order_router_summary.json",
    ]
    for raw in candidates:
        path = os.path.abspath(raw)
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                payload = json.load(f)
            return int(payload.get("orders_ack") or payload.get("ack") or payload.get("acked") or 0)
        except Exception as exc:
            logger.warning("[US_DAILY_REPORT][WARN] router summary load failed path=%s err=%s", path, exc)
    return int(os.getenv("US_DAILY_ROUTER_ACK_COUNT", "0") or 0) if os.getenv("PYTEST_CURRENT_TEST") else 0


def main() -> None:
    from trader.us.utils.logging_utils import setup_us_logging
    setup_us_logging()
    parser = argparse.ArgumentParser(description="US Daily Report Runner")
    parser.add_argument("--env", default="practice", help="Environment (practice|real)")
    parser.add_argument("--session", default=None, help="Session (am|afternoon|close)")
    parser.add_argument("--trade-date", default=None, help="Trade date YYYY-MM-DD (auto if not specified)")
    parser.add_argument("--offline", action="store_true", help="Offline mode (skip DB)")
    args = parser.parse_args()
    
    result = run_daily_report(
        env=args.env,
        session=args.session,
        trade_date=args.trade_date,
        offline=args.offline,
    )
    
    if result["status"] == "ERROR":
        sys.exit(1)


if __name__ == "__main__":
    main()
