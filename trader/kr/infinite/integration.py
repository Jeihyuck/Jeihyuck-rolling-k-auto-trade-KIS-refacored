from __future__ import annotations

import logging
from typing import Callable

logger = logging.getLogger(__name__)


def run_isolated(callback: Callable[[], object]) -> object | None:
    """Exception boundary intended for the minimal KR runner hook."""
    try:
        return callback()
    except Exception:
        logger.exception("[KR_INF][ERROR] sleeve_failed; existing KR session continues")
        return None


def exclude_owned_symbol(symbols: list[str], enabled: bool) -> list[str]:
    """PB1 adapter: preserve order/ranking while removing only sleeve-owned 122630."""
    return [s for s in symbols if not (enabled and str(s).zfill(6) == "122630")]
