#!/usr/bin/env python3
import argparse
import sys

sys.path.insert(0, ".")

from trader.us.db.repos import load_latest_us_prep_status, load_locked_us_watchlist


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-date", required=True)
    args = parser.parse_args()

    prep = load_latest_us_prep_status(args.trade_date, timeout_sec=10)
    prep_status = prep.get("status", "UNKNOWN") if prep else "UNKNOWN"
    prep_run_id = prep.get("run_id", "") if prep else ""

    try:
        locked = load_locked_us_watchlist(args.trade_date, min_count=10, allow_degraded=False, timeout_sec=10)
        locked_count = len(locked) if locked else 0
    except Exception as exc:
        print(f"[US_TRADE_AM][WATCHLIST_LOAD_ERROR] {exc}", file=sys.stderr)
        locked_count = 0

    can_trade = prep_status in ("OK", "OK_WITH_WARNINGS") and locked_count >= 10
    print(f"prep_status={prep_status}")
    print(f"prep_run_id={prep_run_id}")
    print(f"locked_count={locked_count}")
    print(f"can_trade={'1' if can_trade else '0'}")

    outcome = "OK" if can_trade else "FAIL"
    print(
        f"[US_TRADE_AM][PREP_VERIFY][{outcome}] trade_date={args.trade_date} "
        f"prep_status={prep_status} locked_count={locked_count} run_id={prep_run_id}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
