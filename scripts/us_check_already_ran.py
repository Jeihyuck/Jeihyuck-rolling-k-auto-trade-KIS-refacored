#!/usr/bin/env python3
import argparse
import sys

sys.path.insert(0, ".")

from trader.us.db.repos import check_us_afternoon_already_ran, check_us_am_already_ran


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", required=True, choices=["am", "afternoon"])
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--mode", choices=["check", "final-duplicate"], default="check")
    args = parser.parse_args()

    checker = check_us_am_already_ran if args.session == "am" else check_us_afternoon_already_ran
    result = checker(args.trade_date, timeout_sec=5)
    already_ran = bool(result.get("already_ran", True))
    guard_status = result.get("guard_status", "UNKNOWN")
    reason = result.get("reason", "")
    prefix = "US_TRADE_AM" if args.session == "am" else "US_TRADE_AFTERNOON"

    print(f"already_ran={'1' if already_ran else '0'}")
    if args.mode == "final-duplicate":
        print(
            f"[{prefix}][FINAL_DUPLICATE_GUARD][RESULT] trade_date={args.trade_date} "
            f"already_ran={already_ran} guard_status={guard_status} reason={reason}",
            file=sys.stderr,
        )
    elif already_ran:
        print(f"[{prefix}][ALREADY_RAN] trade_date={args.trade_date} guard_status={guard_status} reason={reason}", file=sys.stderr)
    else:
        print(f"[{prefix}][NOT_RAN_YET] trade_date={args.trade_date}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
