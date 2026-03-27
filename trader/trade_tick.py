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
    logger.info("[TRADE][BOOT][START] module=trade_tick_alias")
    logger.info(
        "[TRADE][BOOT][ENV] MODE=%s STRATEGY_ENV=%s KIS_ENV=%s FAIL_IF_POOL_MISSING=%s CANDIDATE_POOL_STRATEGY=%s PB1_PHASE_DEFAULT=%s",
        os.getenv("MODE", "trade"),
        os.getenv("STRATEGY_ENV", ""),
        os.getenv("KIS_ENV", ""),
        os.getenv("FAIL_IF_POOL_MISSING", ""),
        os.getenv("CANDIDATE_POOL_STRATEGY_KEY", ""),
        os.getenv("PB1_PHASE_DEFAULT", ""),
    )
    from trader.pb1_runner import main as pb1_main
    return pb1_main()


if __name__ == "__main__":
    sys.exit(main())
