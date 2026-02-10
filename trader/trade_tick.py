"""
Backward-compatibility alias module for trade_tick.

This module exists to prevent "No module named trader.trade_tick" errors.
It simply delegates to the actual trade tick implementation in pb1_runner.

Usage:
    python -m trader.trade_tick
"""

from __future__ import annotations

import sys


def main() -> int:
    """Run the PB1 trade tick pipeline."""
    from trader.pb1_runner import main as pb1_main
    return pb1_main()


if __name__ == "__main__":
    sys.exit(main())
