#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""scripts/validate_us_daily_report.py

US daily report contract validator.

구분:
  A. FATAL (exit 1):
     - report 파일 없음
     - trade_date / run_id / dry_run 불일치 (stale/wrong report)
     - fatal_error=true
     - Traceback / engine crash
  B. WARNING (exit 0, warning log):
     - optional field 누락 (orders_blocked, block_reasons, etc.)
     - final_status == NO_TRADE / OK_NO_TRADE / OK_WITH_WARNINGS
     - entry_intents=0 (no-trade day)
     - PARTIAL PNL
     - report_status=PARTIAL

사용:
  python3 scripts/validate_us_daily_report.py \\
    --report reports/us_daily/latest_us_daily_report.json \\
    --expected-trade-date 2026-05-24 \\
    --expected-run-id 12345678 \\
    --expected-dry-run 0 \\
    --session am
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# ── status sets ──────────────────────────────────────────────────────────────

# These final_statuses indicate the session ran but made no trades — NOT a failure
NO_TRADE_STATUSES = {
    "NO_TRADE",
    "OK_NO_TRADE",
    "NO_ENTRY_INTENTS",
    "SKIP_PHASE_WINDOW",
    "SKIPPED_DUPLICATE_SESSION",
    "OK_ALREADY_RAN",
}

# These final_statuses are "ran with warnings but OK"
WARNING_STATUSES = {
    "OK_WITH_WARNINGS",
    "PARTIAL_ORDERS_BLOCKED",
    "NO_ORDERS_RISK_BLOCKED",
}

# These final_statuses are considered normal success
SUCCESS_STATUSES = {
    "OK_ORDERS_SENT",
    "SKIP_PHASE_WINDOW",
    "SKIP_ALREADY_HEALTHY_SESSION",
}

# These final_statuses are explicit failures — trigger fatal path
FATAL_STATUSES = {
    "FAILED_PREP_GUARD",
    "FAILED_PREP_CONTRACT",
    "FAILED_DRY_RUN_CONTRACT",
    "FAILED_TRADE_NOT_STARTED",
    "FAILED",
}

# Required fields that MUST be present (fatal if missing)
REQUIRED_FIELDS_FATAL = [
    "trade_date",
    "run_id",
    "session",
    "env",
    "dry_run",
    "final_status",
    "last_stage",
]

# Optional fields — warn if missing but don't fail
OPTIONAL_FIELDS_WARN = [
    "sha",
    "workflow",
    "event_name",
    "expected_to_trade",
    "kis_order_allowed",
    "prep_status",
    "locked_watchlist_count",
    "entry_eval_status",
    "entry_error_type",
    "entry_error_message",
    "entry_intents",
    "orders_sent",
    "orders_blocked",
    "block_reasons",
    "fills",
    "positions",
    "reason",
]


def validate_report(
    report_path: str,
    expected_trade_date: str,
    expected_run_id: str,
    expected_dry_run: bool,
    session: str,
) -> tuple[int, list[str], list[str]]:
    """
    Returns (exit_code, fatal_errors, warnings).
    exit_code: 0 = OK/WARN, 1 = FATAL
    """
    fatals: list[str] = []
    warnings: list[str] = []

    # ── 1. File existence ──────────────────────────────────────────────────
    path = Path(report_path)
    if not path.exists():
        fatals.append(f"report_file_missing path={report_path}")
        return 1, fatals, warnings

    md_path = path.with_suffix(".md")
    if not md_path.exists():
        warnings.append(f"report_md_missing path={md_path}")

    # ── 2. Parse JSON ──────────────────────────────────────────────────────
    try:
        payload = json.loads(path.read_text())
    except Exception as exc:
        fatals.append(f"report_json_parse_error err={exc}")
        return 1, fatals, warnings

    # ── 3. Identity validation (stale report = FATAL) ──────────────────────
    actual_trade_date = str(payload.get("trade_date", ""))
    actual_run_id = str(payload.get("run_id", ""))
    actual_dry_run = bool(payload.get("dry_run", False))

    if actual_trade_date != expected_trade_date:
        fatals.append(
            f"trade_date_mismatch actual={actual_trade_date} expected={expected_trade_date}"
        )

    if actual_run_id != expected_run_id:
        fatals.append(
            f"run_id_mismatch actual={actual_run_id} expected={expected_run_id}"
        )

    if actual_dry_run != expected_dry_run:
        fatals.append(
            f"dry_run_mismatch actual={actual_dry_run} expected={expected_dry_run}"
        )

    if fatals:
        return 1, fatals, warnings

    # ── 4. fatal_error flag ────────────────────────────────────────────────
    if payload.get("fatal_error", False):
        fatals.append("fatal_error=true in report payload")
        return 1, fatals, warnings

    # ── 5. Required field presence ─────────────────────────────────────────
    for f in REQUIRED_FIELDS_FATAL:
        if f not in payload:
            fatals.append(f"required_field_missing field={f}")

    if fatals:
        return 1, fatals, warnings

    # ── 6. Optional fields (warn only) ────────────────────────────────────
    for f in OPTIONAL_FIELDS_WARN:
        if f not in payload:
            warnings.append(f"optional_field_missing field={f}")

    # ── 7. final_status classification ────────────────────────────────────
    final_status = payload.get("final_status", "UNKNOWN")

    if final_status in FATAL_STATUSES:
        fatals.append(f"final_status={final_status} is a fatal failure")
        return 1, fatals, warnings

    expected_to_trade = int(payload.get("expected_to_trade", 0) or 0)
    trade_runner_started = int(payload.get("trade_runner_started", 0) or 0)
    if expected_to_trade == 1 and trade_runner_started != 1:
        fatals.append("expected_to_trade_without_runner_started")
        return 1, fatals, warnings

    pnl_dir = Path("reports/us_pnl")
    pnl_json = pnl_dir / "latest_us_pnl_report.json"
    pnl_md = pnl_dir / "latest_us_pnl_report.md"
    pnl_csv = pnl_dir / "latest_us_pnl_report.csv"
    for required_pnl in (pnl_json, pnl_md, pnl_csv):
        if not required_pnl.exists():
            fatals.append(f"pnl_file_missing path={required_pnl}")
    if pnl_md.exists() and not pnl_md.read_text(encoding="utf-8").strip():
        fatals.append(f"pnl_markdown_empty path={pnl_md}")

    if payload.get("prep_status") == "OK" and int(payload.get("locked_watchlist_count", 0) or 0) == 0:
        fatals.append("locked_watchlist_count_zero_under_prep_ok")

    log_name = f"us-trade-{session}.log"
    log_path = Path("artifacts") / log_name
    if log_path.exists():
        log_text = log_path.read_text(encoding="utf-8", errors="ignore")
        for marker in ("[US_PNL_REPORT_MD][BEGIN]", "[US_PNL_REPORT_MD][END]"):
            if marker not in log_text:
                fatals.append(f"pnl_console_marker_missing marker={marker}")
        if "avg_fill_price" in log_text:
            fatals.append("runtime_sql_contains_avg_fill_price")
        if "[US_RECONCILE][ACK_RECONCILE][INVALID_QTY]" in log_text:
            fatals.append("balance_reconcile_invalid_qty_detected")

    if pnl_json.exists():
        try:
            pnl_payload = json.loads(pnl_json.read_text())
            pnl_status = str(pnl_payload.get("status", "UNKNOWN"))
            if pnl_status not in {"OK", "PARTIAL", "FAILED_PNL_REPORT"}:
                fatals.append(f"invalid_pnl_status={pnl_status}")
            if pnl_payload.get("realized_pnl_source") not in {"db_fills_daily", "unavailable"}:
                fatals.append("invalid_realized_pnl_source")
            if pnl_payload.get("realized_pnl_source") != "unavailable" and pnl_payload.get("kis_account_realized_pnl_raw") == pnl_payload.get("realized_pnl_usd"):
                fatals.append("realized_pnl_direct_from_kis_raw")
        except Exception as exc:
            fatals.append(f"pnl_json_parse_error err={exc}")

    if final_status in NO_TRADE_STATUSES:
        warnings.append(f"no_trade_day final_status={final_status}")
    elif final_status in WARNING_STATUSES:
        warnings.append(f"session_warnings final_status={final_status}")
    elif final_status in SUCCESS_STATUSES:
        pass  # normal
    else:
        # Unknown status — treat as warning not fatal
        warnings.append(f"unknown_final_status={final_status}")

    # ── 8. no-trade day check (entry_intents=0) ───────────────────────────
    entry_intents = payload.get("entry_intents", None)
    orders_sent = payload.get("orders_sent", None)
    if entry_intents == 0 and orders_sent == 0:
        warnings.append("no_trade_day entry_intents=0 orders_sent=0")

    # ── 9. report_status PARTIAL ──────────────────────────────────────────
    report_status = payload.get("report_status", None)
    if report_status == "PARTIAL":
        warnings.append("report_status=PARTIAL")

    return 0, fatals, warnings


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate US daily report contract")
    parser.add_argument("--report", required=True, help="Path to latest_us_daily_report.json")
    parser.add_argument("--expected-trade-date", required=True, help="Expected trade date YYYY-MM-DD")
    parser.add_argument("--expected-run-id", required=True, help="Expected GitHub run ID")
    parser.add_argument("--expected-dry-run", required=True, choices=["0", "1"], help="Expected dry_run flag")
    parser.add_argument("--session", required=True, choices=["am", "afternoon", "prep"], help="Session name")
    args = parser.parse_args()

    expected_dry_run = args.expected_dry_run == "1"

    exit_code, fatals, warnings = validate_report(
        report_path=args.report,
        expected_trade_date=args.expected_trade_date,
        expected_run_id=args.expected_run_id,
        expected_dry_run=expected_dry_run,
        session=args.session,
    )

    fatal_count = len(fatals)
    warning_count = len(warnings)

    for msg in fatals:
        print(f"[US_REPORT_VALIDATE][FAIL] session={args.session} {msg}")

    for msg in warnings:
        print(f"[US_REPORT_VALIDATE][WARN] session={args.session} {msg}")

    report_status_label = "UNKNOWN"
    try:
        payload = json.loads(Path(args.report).read_text())
        report_status_label = str(payload.get("final_status", "UNKNOWN"))
    except Exception:
        pass

    print(
        f"[US_REPORT_VALIDATE][SUMMARY] session={args.session}"
        f" fatal_error={fatal_count}"
        f" report_status={report_status_label}"
        f" warnings={warning_count}"
        f" exit_code={exit_code}"
    )

    if exit_code == 0:
        if warning_count > 0:
            print(f"[US_REPORT_VALIDATE][OK] session={args.session} status=OK_WITH_WARNINGS warnings={warning_count}")
        else:
            print(f"[US_REPORT_VALIDATE][OK] session={args.session} status=OK")
    else:
        print(f"[US_REPORT_VALIDATE][FAIL] session={args.session} fatal_count={fatal_count} WORKFLOW_FATAL=true")
        # Print GitHub Actions error annotation for each fatal
        for msg in fatals:
            print(f"::error::{msg}")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
