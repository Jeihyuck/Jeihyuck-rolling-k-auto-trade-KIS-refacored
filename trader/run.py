from __future__ import annotations

import logging
import sys

import trader
from trader import pb1_runner

logger = logging.getLogger(__name__)


def main() -> int:
    logger.info("[IMPORT_ORIGIN] trader=%s", trader.__file__)
    logger.info("[SYSPATH_HEAD]=%s", sys.path[:10])
    pb1_runner.main()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
