# -*- coding: utf-8 -*-
"""US Daily Report Runner.

미국장 trading session 종료 후一日 summary report 생성.

Features:
- session 지원 (am, afternoon, close)
- trade_date: NY 기준 trade_date 자동 계산 또는 force_now 사용
- stale report 방지: force_now 사용 시 해당 날짜의 report 생성
- DB 기반 metrics: orders, fills, positions, watchlist score contract
- fresh report guarantee: workflow 종료 후 새 report 생성 보장
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

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
        "trade_date": trade_date,
        "session": session,
        "env": env,
        "dry_run": None,
        "orders_ack": 0,
        "orders_dry_run": 0,
        "orders_blocked": 0,
        "orders_rejected": 0,
        "orders_disabled": 0,
        "orders_signal_only": 0,
        "fills": 0,
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
        "prep_trade_can_proceed": None,
        "kis_retry_count": 0,
        "warnings": [],
        "errors": [],
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
            from trader.us.db.repos import (
                load_locked_us_watchlist,
                load_positions,
                load_today_symbols_sold,
                load_us_prep_status,
            )
            from trader.us.score_columns import collect_us_score_nonzero_stats
            
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
            except Exception as exc:
                report["warnings"].append(f"prep_status_load_failed: {exc}")
                logger.warning("[US_DAILY_REPORT][WARN] prep status load failed: %s", exc)
            
            # Orders - count by status
            try:
                # Load DB orders for today
                # Need to add load_us_orders to repos.py
                all_orders_today = []
                try:
                    all_orders_today = load_us_orders(trade_date)
                except NameError:
                    # load_us_orders not yet implemented, load from us_orders table directly
                    pass
                
                if all_orders_today:
                    for order in all_orders_today:
                        status = order.get("status", "").upper()
                        if status == "ACK" or status == "SENT":
                            report["orders_ack"] += 1
                        elif status == "DRY_RUN":
                            report["orders_dry_run"] += 1
                        elif status == "BLOCKED":
                            report["orders_blocked"] += 1
                        elif status == "REJECTED":
                            report["orders_rejected"] += 1
                        elif status == "ORDER_DISABLED":
                            report["orders_disabled"] += 1
                        elif status == "SIGNAL_ONLY":
                            report["orders_signal_only"] += 1
            except Exception as exc:
                report["warnings"].append(f"orders_load_failed: {exc}")
                logger.warning("[US_DAILY_REPORT][WARN] orders load failed: %s", exc)
            
            # Fills
            try:
                sold = load_today_symbols_sold(trade_date)
                report["fills"] = len(sold)
            except Exception as exc:
                report["warnings"].append(f"fills_load_failed: {exc}")
                logger.warning("[US_DAILY_REPORT][WARN] fills load failed: %s", exc)
            
            # Positions
            try:
                positions = load_positions(as_of=trade_date)
                report["positions"] = len(positions)
            except Exception as exc:
                report["warnings"].append(f"positions_load_failed: {exc}")
                logger.warning("[US_DAILY_REPORT][WARN] positions load failed: %s", exc)
        
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
    try:
        os.makedirs("repo/reports/us_daily", exist_ok=True)
    except Exception:
        os.makedirs("reports/us_daily", exist_ok=True)
    
    # Latest report (always overwrite)
    report_base = "repo/reports/us_daily" if os.path.exists("repo") else "reports/us_daily"
    latest_md_path = f"{report_base}/latest_us_daily_report.md"
    latest_json_path = f"{report_base}/latest_us_daily_report.json"
    
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
    
    if session:
        md_lines.append(f"**Session**: {session.upper()}")
        md_lines.append("")
    
    md_lines.extend([
        "## Runtime Metadata",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| trade_date | {trade_date} |",
        f"| session | {session or 'N/A'} |",
        f"| env | {env} |",
        f"| dry_run | {report['dry_run']} |",
        f"| force_now | {force_now or 'N/A'} |",
        "",
        "## Trading Summary",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| orders_ack | {report['orders_ack']} |",
        f"| orders_dry_run | {report['orders_dry_run']} |",
        f"| orders_blocked | {report['orders_blocked']} |",
        f"| orders_rejected | {report['orders_rejected']} |",
        f"| orders_disabled | {report['orders_disabled']} |",
        f"| orders_signal_only | {report['orders_signal_only']} |",
        f"| fills | {report['fills']} |",
        f"| positions | {report['positions']} |",
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
    ])
    
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
    
    return {"status": "OK" if not report["errors"] else "ERROR", "report": report}


def load_us_orders(trade_date: str) -> list[dict]:
    """Temporary helper to load US orders for report.
    
    TODO: Move this to trader/us/db/repos.py as permanent function.
    """
    from trader.us.db.repos import _get_engine_or_none, _today
    from sqlalchemy import text
    
    engine = _get_engine_or_none()
    if engine is None:
        return []
    
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text("SELECT * FROM us_orders WHERE trade_date = :td"),
                {"td": trade_date},
            )
            return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.error("[US_ORDERS][LOAD][ERROR] %s", exc)
        return []


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
