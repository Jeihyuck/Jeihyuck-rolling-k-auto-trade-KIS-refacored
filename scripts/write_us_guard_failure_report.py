#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Write US guard failure report when prep guard fails.

Reads sidecar JSON from guard_us_prep_contract.py and generates:
- reports/us_daily/latest_us_daily_report.json
- reports/us_daily/latest_us_daily_report.md
- reports/us_daily/{trade_date}/{session}/us_daily_report.json
- reports/us_daily/{trade_date}/{session}/us_daily_report.md
- artifacts/us_last_stage.txt

This ensures downstream validation/verification steps have proper artifacts
even when trading session is blocked by prep guard.
"""
import argparse
import json
import os
import sys
from pathlib import Path

# Parse args
parser = argparse.ArgumentParser(description="Write US guard failure report")
parser.add_argument("--session", required=True, choices=["am", "pm"], help="Trading session")
parser.add_argument("--guard-result", required=True, help="Path to guard result sidecar JSON")
args = parser.parse_args()

# Load guard result
try:
    with open(args.guard_result, "r") as f:
        guard_result = json.load(f)
    print(f"[US_GUARD_FAILURE_REPORT][LOAD] {args.guard_result}", flush=True)
except Exception as e:
    print(f"[US_GUARD_FAILURE_REPORT][ERROR] Failed to load guard result: {e}", flush=True)
    sys.exit(1)

# Extract data
trade_date = guard_result.get("trade_date", "unknown")
prep_status = guard_result.get("prep_status", "UNKNOWN")
locked_count = guard_result.get("locked_count", 0)
reason = guard_result.get("reason", "unknown")
force_now = guard_result.get("force_now")

# Get environment variables
run_id = os.getenv("GITHUB_RUN_ID", "local")
sha = os.getenv("GITHUB_SHA", "unknown")
workflow = os.getenv("GITHUB_WORKFLOW", "unknown")
event_name = os.getenv("GITHUB_EVENT_NAME", "unknown")
dry_run = os.getenv("DRY_RUN", "0") == "1"
kis_order_allowed = int(os.getenv("US_KIS_ORDER_ALLOWED", "0"))
offline = os.getenv("OFFLINE", "false").lower() in ("true", "1")
max_ticks = int(os.getenv("MAX_TICKS", "0"))

print(
    f"[US_GUARD_FAILURE_REPORT][META] trade_date={trade_date} run_id={run_id} "
    f"session={args.session} prep_status={prep_status} locked_count={locked_count} reason={reason}",
    flush=True,
)

# Build report payload
report = {
    "trade_date": trade_date,
    "run_id": run_id,
    "sha": sha,
    "workflow": workflow,
    "session": args.session,
    "event_name": event_name,
    "env": "practice",
    "dry_run": dry_run,
    "kis_order_allowed": kis_order_allowed,
    "prep_status": prep_status,
    "locked_watchlist_count": locked_count,
    "entry_eval_status": "SKIPPED",
    "entry_error_type": "FAILED_PREP_GUARD",
    "entry_error_message": reason,
    "entry_intents": 0,
    "orders_sent": 0,
    "orders_blocked": 0,
    "block_reasons": {},
    "fills": 0,
    "positions": 0,
    "last_stage": "prep_guard",
    "final_status": "FAILED_PREP_GUARD",
    "reason": reason,
    "temp_error_count": 0,
    "temp_recovered_count": 0,
    "missed_trade_window": False,
    "force_now": force_now,
    "offline": offline,
    "tick_count": 0,
    "max_ticks": max_ticks,
    "wall_elapsed_sec": 0,
}

# Generate markdown
md_lines = [
    f"# US Daily Report - {trade_date}",
    "",
    f"**Session:** {args.session}",
    f"**Run ID:** {run_id}",
    f"**Status:** {report['final_status']}",
    f"**Reason:** {reason}",
    "",
    "## Metadata",
    "",
    f"- **Workflow:** {workflow}",
    f"- **Event:** {event_name}",
    f"- **SHA:** {sha}",
    f"- **Env:** practice",
    f"- **Dry Run:** {dry_run}",
    "",
    "## Prep Guard Failure",
    "",
    f"- **Prep Status:** {prep_status}",
    f"- **Locked Watchlist Count:** {locked_count}",
    f"- **Force Now:** {force_now or 'N/A'}",
    f"- **Offline:** {offline}",
    "",
    "## Trading Result",
    "",
    "- **Entry Intents:** 0 (skipped)",
    "- **Orders Sent:** 0",
    "- **Fills:** 0",
    "- **Positions:** 0",
    "",
    "## Summary",
    "",
    f"Prep guard blocked trading session due to `{reason}`. No orders were evaluated or sent.",
    "",
]
md_content = "\n".join(md_lines)

# Write reports
base_dir = Path("reports/us_daily")
base_dir.mkdir(parents=True, exist_ok=True)

# Latest report
latest_json = base_dir / "latest_us_daily_report.json"
latest_md = base_dir / "latest_us_daily_report.md"
latest_json.write_text(json.dumps(report, indent=2))
latest_md.write_text(md_content)
print(f"[US_GUARD_FAILURE_REPORT][WRITE] {latest_json}", flush=True)
print(f"[US_GUARD_FAILURE_REPORT][WRITE] {latest_md}", flush=True)

# Session-specific report
session_dir = base_dir / trade_date / args.session
session_dir.mkdir(parents=True, exist_ok=True)
session_json = session_dir / "us_daily_report.json"
session_md = session_dir / "us_daily_report.md"
session_json.write_text(json.dumps(report, indent=2))
session_md.write_text(md_content)
print(f"[US_GUARD_FAILURE_REPORT][WRITE] {session_json}", flush=True)
print(f"[US_GUARD_FAILURE_REPORT][WRITE] {session_md}", flush=True)

# Write last_stage artifact
artifacts_dir = Path("artifacts")
artifacts_dir.mkdir(parents=True, exist_ok=True)
last_stage_file = artifacts_dir / "us_last_stage.txt"
last_stage_file.write_text(f"{report['last_stage']}\n")
print(f"[US_GUARD_FAILURE_REPORT][WRITE] {last_stage_file}", flush=True)

print(
    f"[US_GUARD_FAILURE_REPORT][DONE] final_status={report['final_status']} "
    f"run_id={run_id} trade_date={trade_date}",
    flush=True,
)
