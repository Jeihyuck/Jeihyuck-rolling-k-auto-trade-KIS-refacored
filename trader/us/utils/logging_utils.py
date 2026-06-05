"""US trading 전용 로깅 유틸리티."""
from __future__ import annotations

import logging
import sys


_LOGGING_CONFIGURED = False

def setup_us_logging_once(
    level: int = logging.INFO,
    fmt: str = "%(asctime)s %(levelname)s %(name)s %(message)s",
) -> None:
    """root logger를 한 번만 설정한다."""

    global _LOGGING_CONFIGURED
    root = logging.getLogger()
    if _LOGGING_CONFIGURED and getattr(root, "_us_logging_configured", False):
        return

    for h in list(root.handlers):
        root.removeHandler(h)

    logging.basicConfig(
        level=level,
        format=fmt,
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
    root.propagate = False
    setattr(root, "_us_logging_configured", True)
    _LOGGING_CONFIGURED = True


def setup_us_logging(
    level: int = logging.INFO,
    fmt: str = "%(asctime)s %(levelname)s %(name)s %(message)s",
) -> None:
    setup_us_logging_once(level=level, fmt=fmt)
