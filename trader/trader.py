# -*- coding: utf-8 -*-
"""Thin entrypoint orchestrating KOSPI core + KOSDAQ alpha engines."""
from __future__ import annotations

import logging

from portfolio.portfolio_manager import PortfolioManager

logger = logging.getLogger(__name__)


def main() -> None:
    mgr = PortfolioManager()
    result = mgr.run_once()
    logger.info("[TRADER] cycle complete %s", result)


if __name__ == "__main__":
    main()
