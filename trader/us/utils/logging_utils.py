"""US trading 전용 로깅 유틸리티.

중복 basicConfig 호출을 방지하는 setup_us_logging() 제공.
"""
from __future__ import annotations

import logging


def setup_us_logging(
    level: int = logging.INFO,
    fmt: str = "%(asctime)s %(levelname)s %(name)s %(message)s",
) -> None:
    """root logger를 한 번만 설정한다. 중복 호출은 무시된다."""
    root = logging.getLogger()
    if getattr(root, "_us_trading_logging_configured", False):
        return
    if not root.handlers:
        logging.basicConfig(level=level, format=fmt)
    else:
        root.setLevel(level)
    root._us_trading_logging_configured = True  # type: ignore[attr-defined]
