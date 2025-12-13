from __future__ import annotations

from typing import Any


def run_trade_loop(capital_override: float | None = None) -> Any:
    # Delay import to avoid any potential circular initialization between
    # strategy and trader modules while still delegating to the legacy runner.
    from trader import legacy_kosdaq_runner

    return legacy_kosdaq_runner.main(capital_override=capital_override)
