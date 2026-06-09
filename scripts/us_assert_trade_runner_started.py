#!/usr/bin/env python3
import argparse
import re
from pathlib import Path


RUNNER_START_RE = re.compile(r"\[US_TRADE_RUNNER\]\[START\]|\[US_SESSION\]\[START\]")
PHASE_SKIP_RE = re.compile(r"\[RUN_SUMMARY\]\[RESULT\] status=SKIP_PHASE_WINDOW|\[US_TRADE_(?:AM|AFTERNOON)\]\[EXIT\] reason=phase_guard_skip")
DUPLICATE_RE = re.compile(r"\[US_TRADE_SESSION_LOCK\]\[SKIP\] reason=session_already_running_or_completed")
ALREADY_RAN_RE = re.compile(r"\[RUN_SUMMARY\]\[RESULT\] status=OK reason=(already_ran|already_ran_after_wait)")


def accepted_skip_status(log_text: str) -> str:
    if PHASE_SKIP_RE.search(log_text):
        return "SKIPPED"
    if DUPLICATE_RE.search(log_text):
        return "SKIPPED_DUPLICATE_SESSION"
    if ALREADY_RAN_RE.search(log_text):
        return "OK_ALREADY_RAN"
    return ""


def should_fail_trade_not_started(
    *,
    log_text: str,
    event_name: str,
    kis_env: str,
    phase_should_run: str,
    run_mode: str,
    order_allowed: str,
    kis_order_allowed: str,
    runner_started: str,
) -> bool:
    critical = (
        event_name == "schedule"
        and kis_env == "practice"
        and phase_should_run == "1"
        and run_mode == "TRADE"
        and order_allowed == "1"
        and kis_order_allowed == "1"
    )
    if not critical:
        return False
    if accepted_skip_status(log_text):
        return False
    if runner_started == "1":
        return False
    return RUNNER_START_RE.search(log_text) is None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", required=True, choices=["am", "afternoon"])
    parser.add_argument("--log", required=True)
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--kis-env", required=True)
    parser.add_argument("--phase-should-run", required=True)
    parser.add_argument("--run-mode", required=True)
    parser.add_argument("--order-allowed", required=True)
    parser.add_argument("--kis-order-allowed", required=True)
    parser.add_argument("--runner-started", default="0")
    parser.add_argument("--emit-final-status", action="store_true")
    args = parser.parse_args()

    path = Path(args.log)
    if not path.exists():
        print(f"::error::missing US {args.session} log file")
        return 1
    log_text = path.read_text(encoding="utf-8", errors="replace")
    status = accepted_skip_status(log_text)
    if should_fail_trade_not_started(
        log_text=log_text,
        event_name=args.event_name,
        kis_env=args.kis_env,
        phase_should_run=args.phase_should_run,
        run_mode=args.run_mode,
        order_allowed=args.order_allowed,
        kis_order_allowed=args.kis_order_allowed,
        runner_started=args.runner_started,
    ):
        label = "AM" if args.session == "am" else "Afternoon"
        prefix = "US_TRADE_AM" if args.session == "am" else "US_TRADE_AFTERNOON"
        print(f"::error::US {label} scheduled practice trade passed guards but trade runner did not start.")
        with path.open("a", encoding="utf-8") as fh:
            fh.write(f"[{prefix}][FATAL] reason=trade_runner_not_started_after_guards\n")
            if args.emit_final_status:
                fh.write("[US_WORKFLOW][FINAL_STATUS] status=FAILED_TRADE_NOT_STARTED\n")
        if args.emit_final_status:
            print("[US_WORKFLOW][FINAL_STATUS] status=FAILED_TRADE_NOT_STARTED")
        return 1

    if args.emit_final_status and status:
        print(f"[US_WORKFLOW][FINAL_STATUS] status={status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
