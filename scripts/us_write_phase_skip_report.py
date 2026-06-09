#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", required=True, choices=["am", "afternoon"])
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--missed-trade-window", default="false")
    parser.add_argument("--kis-order-allowed", default="0")
    args = parser.parse_args()

    report = {
        "trade_date": args.trade_date,
        "run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "sha": os.environ.get("GITHUB_SHA", ""),
        "workflow": os.environ.get("GITHUB_WORKFLOW", ""),
        "session": args.session,
        "event_name": os.environ.get("GITHUB_EVENT_NAME", ""),
        "env": os.environ.get("KIS_ENV", "practice"),
        "dry_run": os.environ.get("DRY_RUN", "0") == "1",
        "kis_order_allowed": int(args.kis_order_allowed or 0),
        "prep_status": "SKIP_PHASE_WINDOW",
        "locked_watchlist_count": 0,
        "entry_eval_status": "SKIPPED",
        "entry_error_type": "",
        "entry_error_message": "",
        "entry_intents": 0,
        "orders_sent": 0,
        "fills": 0,
        "positions": 0,
        "last_stage": "phase_guard",
        "final_status": "SKIP_PHASE_WINDOW",
        "reason": args.reason,
        "temp_error_count": 0,
        "temp_recovered_count": 0,
        "missed_trade_window": args.missed_trade_window.lower() == "true",
    }
    base = Path("reports/us_daily")
    base.mkdir(parents=True, exist_ok=True)
    (base / "latest_us_daily_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    md = "\n".join([
        f"# US Daily Report - {report['trade_date']}",
        "",
        "## Required Fields",
        "",
    ] + [f"- {key}: {value}" for key, value in report.items()]) + "\n"
    (base / "latest_us_daily_report.md").write_text(md, encoding="utf-8")
    day_dir = base / report["trade_date"] / args.session
    day_dir.mkdir(parents=True, exist_ok=True)
    (day_dir / "us_daily_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (day_dir / "us_daily_report.md").write_text(md, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
