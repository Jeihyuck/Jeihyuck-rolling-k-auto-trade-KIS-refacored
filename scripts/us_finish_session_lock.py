#!/usr/bin/env python3
import argparse
import os
from datetime import date
import sys

sys.path.insert(0, ".")

from trader.us.db.session_locks import finish_us_session_lock


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", required=True, choices=["am", "afternoon"])
    parser.add_argument("--trade-date", default="")
    args = parser.parse_args()

    trade_date = args.trade_date or os.environ.get("TRADE_DATE", "")
    trade_status = os.environ.get("US_TRADE_STATUS", "FAILED")
    reason = os.environ.get("US_TRADE_RUNNER_BLOCK_REASON", "")
    runner_started = os.environ.get("US_TRADE_RUNNER_STARTED", "0")

    if not trade_date:
        print("[US_TRADE_SESSION_LOCK][RELEASE_SKIP] reason=missing_trade_date")
        return 0

    if trade_status == "OK":
        lock_status = "DONE"
    elif trade_status in {"SKIPPED_DUPLICATE_SESSION", "OK_ALREADY_RAN"}:
        lock_status = "DONE_WITH_WARNINGS"
    else:
        lock_status = "FAILED_RETRYABLE"

    finish_us_session_lock(
        env=os.environ.get("KIS_ENV", "practice"),
        trade_date=date.fromisoformat(trade_date),
        session=args.session,
        github_run_id=os.environ.get("GITHUB_RUN_ID"),
        status=lock_status,
        reason=reason or trade_status,
        metadata={
            "trade_status": trade_status,
            "trade_runner_started": runner_started,
            "reason": reason,
        },
    )
    print(
        f"[US_TRADE_SESSION_LOCK][RELEASE] session={args.session} trade_date={trade_date} "
        f"status={lock_status} reason={reason or trade_status}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
