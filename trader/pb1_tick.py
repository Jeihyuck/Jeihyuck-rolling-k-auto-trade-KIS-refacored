from __future__ import annotations

import logging

from .pb1_runner import PB1Runner


def main() -> int:
    """
    Run a single PB1 tick (one-cycle execution) and exit.
    Intended for GitHub Actions scheduled runs every 5 minutes.
    """
    runner = PB1Runner()
    runner.run_once()
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    raise SystemExit(main())
