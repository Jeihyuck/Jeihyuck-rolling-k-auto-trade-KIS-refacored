#!/usr/bin/env python3
import argparse
from datetime import date
from pathlib import Path
import sys

sys.path.insert(0, ".")

from trader.us.db.session_locks import claim_us_session_lock


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", required=True, choices=["am", "afternoon"])
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--env", default="practice")
    parser.add_argument("--github-run-id", default="")
    parser.add_argument("--github-workflow", default="")
    parser.add_argument("--github-run-attempt", default="")
    parser.add_argument("--event-name", default="")
    parser.add_argument("--manual-confirm-ok", default="0")
    parser.add_argument("--run-window", default="")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    try:
        claimed, existing = claim_us_session_lock(
            env=args.env,
            trade_date=date.fromisoformat(args.trade_date),
            session=args.session,
            github_run_id=args.github_run_id,
            github_workflow=args.github_workflow,
            github_run_attempt=args.github_run_attempt,
            metadata={
                "event_name": args.event_name,
                "manual_confirm_ok": args.manual_confirm_ok == "1",
                "run_window": args.run_window,
            },
        )
    except Exception as exc:
        exception_text = f"{type(exc).__name__}: {exc}"
        lines = [
            "claimed=0",
            "existing_status=session_lock_exception",
            f"session_lock_exception={exception_text}",
        ]
        Path(args.output).write_text("\n".join(lines) + "\n", encoding="utf-8")
        print("\n".join(lines))
        print(f"::error::session_lock_exception {exception_text}", file=sys.stderr)
        return 0

    lines = [
        f"claimed={'1' if claimed else '0'}",
        f"existing_status={(existing or {}).get('status', '')}",
    ]

    Path(args.output).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
