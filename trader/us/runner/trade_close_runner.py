# -*- coding: utf-8 -*-
"""US Trade Close Runner.

- fills 조회
- positions reconcile
- 미체결 주문 점검
- pnl snapshot
- report 준비
"""
from __future__ import annotations

import argparse
import atexit
import logging
import sys

logger = logging.getLogger(__name__)


def run_trade_close(env: str = "practice", offline: bool = False, force_now: str | None = None) -> dict:
    logger.info("[US_TRADE_CLOSE][START] env=%s offline=%s", env, offline)
    logger.info(
        "[US_TRADE_CLOSE][FORCE_NOW] enabled=%s force_now=%s",
        1 if force_now else 0,
        force_now or "",
    )

    from trader.us.data_provider import USDataProvider
    from trader.us.db.repos import (
        save_fills_with_result, save_position_snapshot, save_reconcile_log,
    )
    from trader.us.market_calendar import now_ny
    from datetime import datetime
    from zoneinfo import ZoneInfo
    import os

    provider = USDataProvider(offline=offline)

    NY_TZ = ZoneInfo("America/New_York")
    if force_now:
        now = datetime.fromisoformat(force_now).astimezone(NY_TZ)
    else:
        now = now_ny()
    trade_date = now.strftime("%Y-%m-%d")

    from trader.us.utils.session_guard import acquire_us_session_running_lock, release_us_session_running_lock
    run_id = os.getenv("GITHUB_RUN_ID", "local-close")
    running_lock = acquire_us_session_running_lock(trade_date, "close", run_id)
    if not running_lock.get("acquired"):
        logger.warning(
            "[US_TRADE_CLOSE][SKIP_DUPLICATE_RUNNING] trade_date=%s reason=%s",
            trade_date, running_lock.get("reason"),
        )
        return {
            "status": "SKIP",
            "reason": "duplicate_session_running",
            "detail_status": "SKIP_DUPLICATE_RUNNING",
            "trade_date": trade_date,
        }
    atexit.register(release_us_session_running_lock, trade_date, "close", run_id)

    try:
        # ── close entry 정책 ─────────────────────────────────────────────────
        close_entry_enabled = os.getenv("US_CLOSE_ENTRY_ENABLED", "0") == "1"
        if not close_entry_enabled:
            logger.info("[US_CLOSE][ENTRY_DISABLED] reason=US_CLOSE_ENTRY_ENABLED=0")

        # 1. Fills 조회
        fills: list[dict] = []
        fills_status = "SKIP" if offline else "UNKNOWN"
        fills_error = ""
        if not offline:
            try:
                from trader.us.execution.fills import get_fills_today
                fills_result = get_fills_today(provider=provider, trade_date=trade_date)
                fills_status = fills_result.get("status", "UNKNOWN")
                fills = fills_result["fills"]
                fills_error = fills_result.get("error", "")
                if fills_status != "OK":
                    logger.error(
                        "[US_TRADE_CLOSE][ERROR] fills failed: status=%s error=%s",
                        fills_status,
                        fills_error,
                    )
                logger.info("[US_TRADE_CLOSE][FILLS] count=%d status=%s", len(fills), fills_status)
            except Exception as exc:
                logger.error("[US_TRADE_CLOSE][ERROR] fills exception: %s", exc)
                fills_status = "ERROR"
                fills_error = str(exc)
        else:
            logger.info("[US_TRADE_CLOSE][FILLS] offline — skipping KIS fills")

        # 2. Fills DB 저장
        try:
            if fills:
                fill_save_result = save_fills_with_result(fills, trade_date=trade_date)
            else:
                fill_save_result = {"status": "OK", "inserted_count": 0, "updated_count": 0, "unchanged_count": 0, "regression_count": 0}
            if fill_save_result.get("status") != "OK":
                fills_status = fill_save_result.get("status", "ERROR")
                fills_error = fill_save_result.get("error") or fills_status
                logger.error("[US_FILLS][SAVE][FAILED] result=%s", fill_save_result)
            logger.info("[US_FILLS][SAVE] count=%d result=%s", len(fills), fill_save_result)
        except Exception as exc:
            logger.warning("[US_TRADE_CLOSE][WARN] save_fills failed: %s", exc)
            fills_status = "DB_ERROR"
            fills_error = str(exc)

        # 3. Reconcile
        reconcile_result: dict = {
            "status": "SKIP",
            "balance_fetch_status": "SKIP",
            "authoritative_positions": False,
            "preserve_previous_positions": True,
            "positions": [],
            "position_count": 0,
        }
        if not offline:
            try:
                from trader.us.execution.reconcile import reconcile_positions
                reconcile_result = reconcile_positions(provider=provider, trade_date=trade_date)
                logger.info("[US_TRADE_CLOSE][RECONCILE] status=%s", reconcile_result.get("status"))
            except TypeError as exc:
                logger.error("[US_TRADE_CLOSE][CONTRACT_ERROR] reconcile TypeError: %s", exc)
                reconcile_result = {
                    "status": "CONTRACT_ERROR",
                    "reason": "reconcile_internal_type_error",
                    "balance_fetch_status": "FAILED",
                    "authoritative_positions": False,
                    "preserve_previous_positions": True,
                    "error": str(exc),
                    "positions": [],
                    "position_count": 0,
                }
            except Exception as exc:
                logger.error("[US_TRADE_CLOSE][ERROR] reconcile failed: %s", exc)
                reconcile_result = {
                    "status": "TEMP_ERROR",
                    "balance_fetch_status": "FAILED",
                    "authoritative_positions": False,
                    "preserve_previous_positions": True,
                    "error": str(exc),
                    "positions": [],
                    "position_count": 0,
                }

        # 4. Positions DB 저장 — only authoritative OK zero/positions may overwrite snapshot.
        positions = reconcile_result.get("positions", [])
        position_snapshot_error = ""
        try:
            can_save_positions = (
                reconcile_result.get("status") == "OK"
                and reconcile_result.get("balance_fetch_status") == "OK"
                and reconcile_result.get("authoritative_positions") is True
                and reconcile_result.get("preserve_previous_positions") is False
            )
            if not can_save_positions:
                logger.warning(
                    "[US_RECONCILE][SKIP_ZERO_SNAPSHOT] reason=balance_fetch_failed preserve_previous=1 "
                    "status=%s balance_fetch_status=%s authoritative_positions=%s",
                    reconcile_result.get("status"),
                    reconcile_result.get("balance_fetch_status"),
                    reconcile_result.get("authoritative_positions"),
                )
            else:
                save_position_snapshot(positions, trade_date=trade_date, balance_fetch_status="OK",
                                       balance_parse_status="OK", authoritative_positions=True,
                                       preserve_previous_positions=False, close_source="kis_final_balance")
                logger.info("[US_POSITIONS][SNAPSHOT][SAVE] count=%d", len(positions))
        except Exception as exc:
            position_snapshot_error = str(exc)
            logger.error("[US_TRADE_CLOSE][ERROR] save_position_snapshot failed: %s", exc)

        # 5. Reconcile log DB 저장
        try:
            save_reconcile_log({
                "status": reconcile_result.get("status", "OK"),
                "message": reconcile_result.get("error", ""),
                "position_count": len(positions),
                "total_pvs": reconcile_result.get("total_pvs_usd", 0),
                "detail": {"env": env, "runner": "trade_close"},
            }, trade_date=trade_date)
            logger.info("[US_RECONCILE_LOG][SAVE]")
        except Exception as exc:
            logger.warning("[US_TRADE_CLOSE][WARN] save_reconcile_log failed: %s", exc)

        # 6. Balance snapshot
        balance = {}
        balance_fetch_ok = False
        try:
            balance = provider.get_balance()
            balance_fetch_ok = isinstance(balance, dict) and str(balance.get("balance_parse_status", "OK")) == "OK"
        except Exception as exc:
            logger.warning("[US_TRADE_CLOSE][WARN] balance fetch failed: %s", exc)

        # 7. Final balance delta classification for same-day ACK orders
        close_order_classification = {"status": "SKIP", "orders": [], "counts": {}, "pending_order_count": 0}
        if not offline:
            try:
                from trader.us.execution.reconcile import classify_ack_orders_with_final_balance
                close_order_classification = classify_ack_orders_with_final_balance(provider=provider, trade_date=trade_date, env=env)
                logger.info(
                    "[US_TRADE_CLOSE][ORDER_FINAL_CLASSIFICATION] status=%s pending=%s counts=%s",
                    close_order_classification.get("status"),
                    close_order_classification.get("pending_order_count"),
                    close_order_classification.get("counts"),
                )
            except Exception as exc:
                close_order_classification = {"status": "ERROR", "error": str(exc), "orders": [], "counts": {}, "pending_order_count": 0}
                logger.warning("[US_TRADE_CLOSE][WARN] order final classification failed: %s", exc)

        # 8. Daily report
        try:
            from trader.us.runner.daily_report_runner import run_daily_report
            final_positions_authoritative = bool(
                reconcile_result.get("status") == "OK"
                and reconcile_result.get("balance_fetch_status") == "OK"
                and reconcile_result.get("balance_parse_status", "OK") == "OK"
                and reconcile_result.get("preserve_previous_positions") is False
            )
            direct_positions = positions if final_positions_authoritative else None
            direct_balance = reconcile_result if final_positions_authoritative else None
            if not final_positions_authoritative and balance_fetch_ok:
                candidate_positions = balance.get("positions")
                if isinstance(candidate_positions, list):
                    direct_positions = candidate_positions
                    direct_balance = balance
            daily_report_result = run_daily_report(
                env=env, session="close", trade_date=trade_date, offline=offline,
                final_balance=direct_balance, final_positions=direct_positions, kis_fills=fills,
                close_order_classification=close_order_classification, close_run_id=run_id,
            )
        except Exception as exc:
            logger.warning("[US_TRADE_CLOSE][WARN] daily report failed: %s", exc)
            daily_report_result = {"status": "ERROR", "report": {"report_consistency": "REPORT_INCONSISTENT"}}

        # 9. Status 계산
        status = "OK"
        daily_report_result = daily_report_result or {"status": "OK", "report": {"report_consistency": "OK"}}
        pending_count = int(close_order_classification.get("pending_order_count") or 0)
        report_consistency = (daily_report_result.get("report") or {}).get("report_consistency", "OK")
        reconcile_status = str(reconcile_result.get("status") or "UNKNOWN").upper()
        reconcile_error_statuses = {
            "CONTRACT_ERROR",
            "FATAL_ERROR",
            "POSITION_PERSIST_ERROR",
            "FILL_ACCOUNTING_INVARIANT_FAILED",
            "RECONCILE_UPDATE_FAILED",
            "EVIDENCE_QUANTITY_REGRESSION",
            "EVIDENCE_QUANTITY_CONFLICT",
            "EVIDENCE_QUANTITY_OVERFLOW",
        }
        if (
            fills_status in {"DB_ERROR", "EVIDENCE_QUANTITY_REGRESSION"}
            or position_snapshot_error
            or reconcile_status in reconcile_error_statuses
        ):
            report_consistency = "FAILED"
        report_failed = (
            daily_report_result.get("status") not in {"OK", "OK_WITH_WARNINGS"}
            or bool((daily_report_result.get("report") or {}).get("errors"))
            or report_consistency != "OK"
        )
        if fills_status in {"CONTRACT_ERROR", "DB_ERROR", "EVIDENCE_QUANTITY_REGRESSION"}:
            status = "ERROR"
        elif reconcile_status in reconcile_error_statuses:
            status = "ERROR"
        elif position_snapshot_error:
            status = "ERROR"
        elif report_failed:
            status = "ERROR"
        elif close_order_classification.get("status") == "ERROR":
            status = "ERROR"
        elif pending_count > 0:
            status = "DEGRADED_ACK_UNRESOLVED"
        elif fills_status not in ("OK", "SKIP") or reconcile_status not in ("OK", "SKIP"):
            status = "OK_WITH_WARNINGS"

        # 10. Final 로그
        if status == "OK":
            logger.info("[US_TRADE_CLOSE][OK]")
        elif status == "OK_WITH_WARNINGS":
            logger.warning(
                "[US_TRADE_CLOSE][WARNINGS] fills_status=%s reconcile_status=%s",
                fills_status,
                reconcile_status,
            )
        else:
            logger.error(
                "[US_TRADE_CLOSE][ERROR] final_status=ERROR fills_status=%s reconcile_status=%s fills_error=%s position_snapshot_error=%s",
                fills_status,
                reconcile_status,
                fills_error,
                position_snapshot_error,
            )

        return {
            "status": status,
            "fills_status": fills_status,
            "fills_error": fills_error,
            "fills_count": len(fills),
            "positions_count": len(positions),
            "reconcile_status": reconcile_status,
            "balance": balance,
            "close_entry_enabled": close_entry_enabled,
            "order_final_classification": close_order_classification.get("orders", []),
            "order_final_classification_counts": close_order_classification.get("counts", {}),
            "pending_order_count": close_order_classification.get("pending_order_count", 0),
            "daily_report_status": daily_report_result.get("status"),
            "report_consistency": report_consistency,
            "position_snapshot_error": position_snapshot_error,
        }
    finally:
        release_us_session_running_lock(trade_date, "close", run_id=run_id)


def main() -> None:
    from trader.us.utils.logging_utils import setup_us_logging
    setup_us_logging()
    parser = argparse.ArgumentParser(description="US Trade Close Runner")
    parser.add_argument("--env", default="practice")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--force-now", dest="force_now", default=None)
    args = parser.parse_args()
    result = run_trade_close(env=args.env, offline=args.offline, force_now=args.force_now)
    if result["status"] == "ERROR":
        sys.exit(1)


if __name__ == "__main__":
    main()
