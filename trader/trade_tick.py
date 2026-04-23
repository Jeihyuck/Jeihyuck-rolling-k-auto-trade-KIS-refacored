"""
Backward-compatibility alias module for trade_tick.

This module exists to prevent "No module named trader.trade_tick" errors.
It simply delegates to the actual trade tick implementation in pb1_runner.

Usage:
    python -m trader.trade_tick
"""

from __future__ import annotations

import sys
import os
import logging

logger = logging.getLogger(__name__)


def main() -> int:
    """Run the PB1 trade tick pipeline."""
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
