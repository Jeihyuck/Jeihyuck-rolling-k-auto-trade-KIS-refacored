from __future__ import annotations

import os
from typing import Any

from trader import legacy_kosdaq_runner


def run_trade_loop(capital_override: float | None = None) -> Any:
    if capital_override is not None:
        os.environ["DAILY_CAPITAL"] = str(int(capital_override))
    return legacy_kosdaq_runner.main()
