from __future__ import annotations

import logging
import os
import sys

import trader
from trader import pb1_runner
from trader.botstate_paths import get_botstate_root
from trader.runtime_store import RuntimeStore
from trader.time_utils import now_kst

logger = logging.getLogger(__name__)


def main() -> int:
    logger.info("[IMPORT_ORIGIN] trader=%s", trader.__file__)
    logger.info("[SYSPATH_HEAD]=%s", sys.path[:10])
    runtime_store = RuntimeStore(base_dir=get_botstate_root())
    pb1_runner.ensure_universe_built_once(
        runtime_store=runtime_store,
        env=os.getenv("KIS_ENV"),
        strategy=os.getenv("PB1_UNIVERSE_STRATEGY"),
        as_of=now_kst().date().isoformat(),
    )
    pb1_runner.main()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
