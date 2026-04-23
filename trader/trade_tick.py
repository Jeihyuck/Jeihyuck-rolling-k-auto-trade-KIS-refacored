"""
Backward-compatibility alias module for trade_tick.

This module exists to prevent "No module named trader.trade_tick" errors.
It simply delegates to the actual trade tick implementation in pb1_runner.

Usage:
    python -m trader.trade_tick
"""

from __future__ import annotations

import argparse
import sys
import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def _verify_log_cli(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="python -m trader.trade_tick verify-log")
    parser.add_argument("--session", required=True, choices=["am", "pm", "close"])
    parser.add_argument("--log", required=True)
    args = parser.parse_args(argv)

    from trader.pb1_runner import evaluate_workflow_log_success

    text = Path(args.log).read_text(encoding="utf-8")
    result = evaluate_workflow_log_success(session=args.session, log_text=text)
    prefix = {
        "am": "TRADE_AM",
        "pm": "TRADE_PM",
        "close": "TRADE_CLOSE",
    }[args.session]
    print(
        f"[{prefix}][VERIFY][COUNTS] fatal_count={0 if result['ok'] else 1} "
        f"timeout_count={result['timeout_count']} db_autocommit_count={result['db_autocommit_count']} "
        f"degraded_count={result['degraded_count']}"
    )
    if result["timeout_count"] > 0:
        print(f"[{prefix}][VERIFY][WARN] timeout_count={result['timeout_count']}")
    if result["db_autocommit_count"] > 0:
        print(f"[{prefix}][VERIFY][WARN] db_autocommit_count={result['db_autocommit_count']}")
    if result["degraded_count"] > 0:
        print(f"[{prefix}][VERIFY][WARN] degraded_count={result['degraded_count']}")
    if result.get("exit_pass_timeout_observed"):
        print(f"[{prefix}][VERIFY][WARN] exit_pass_timeout_observed=1")
    print(f"[{prefix}][VERIFY][RESULT] status={result['status']} reason={result['reason']}")
    return 0 if result["ok"] else 1


def main() -> int:
    """Run the PB1 trade tick pipeline."""
    if len(sys.argv) > 1 and sys.argv[1] == "verify-log":
        return _verify_log_cli(sys.argv[2:])
    os.environ.setdefault("PB1_TICK_HARD_TIMEOUT_SEC", "90")
    os.environ.setdefault("PB1_LAST_STAGE", "trade_tick.bootstrap")
    logger.info("[TRADE][BOOT][START] module=trade_tick_alias")
    logger.info(
        "[TRADE][BOOT][ENV] MODE=%s STRATEGY_ENV=%s KIS_ENV=%s FAIL_IF_POOL_MISSING=%s CANDIDATE_POOL_STRATEGY=%s PB1_PHASE_DEFAULT=%s PB1_TICK_HARD_TIMEOUT_SEC=%s PB1_LAST_STAGE=%s",
        os.getenv("MODE", "trade"),
        os.getenv("STRATEGY_ENV", ""),
        os.getenv("KIS_ENV", ""),
        os.getenv("FAIL_IF_POOL_MISSING", ""),
        os.getenv("CANDIDATE_POOL_STRATEGY_KEY", ""),
        os.getenv("PB1_PHASE_DEFAULT", ""),
        os.getenv("PB1_TICK_HARD_TIMEOUT_SEC", "90"),
        os.getenv("PB1_LAST_STAGE", "trade_tick.bootstrap"),
    )
    logger.info(
        "[TRADE][BOOT][SESSION_POLICY] force_trade_session=%s forced_trade_session=%s force_entry_window_override=%s session_recovery_continue=%s phase_guard_classification=%s",
        os.getenv("PB1_FORCE_TRADE_SESSION", "auto"),
        os.getenv("PB1_FORCED_TRADE_SESSION", ""),
        os.getenv("PB1_FORCE_ENTRY_WINDOW_OVERRIDE", "0"),
        os.getenv("PB1_SESSION_RECOVERY_CONTINUE", "0"),
        os.getenv("PB1_PHASE_GUARD_CLASSIFICATION", ""),
    )
    from trader.pb1_runner import main as pb1_main
    return pb1_main()


if __name__ == "__main__":
    sys.exit(main())
