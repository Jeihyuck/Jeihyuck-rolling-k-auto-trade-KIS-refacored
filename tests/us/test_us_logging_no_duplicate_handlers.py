from __future__ import annotations

import logging


def test_setup_us_logging_once_does_not_duplicate_handlers() -> None:
    from trader.us.utils.logging_utils import setup_us_logging_once

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    if hasattr(root, "_us_logging_configured"):
        delattr(root, "_us_logging_configured")

    setup_us_logging_once()
    first_count = len(root.handlers)
    setup_us_logging_once()
    setup_us_logging_once()

    assert first_count == 1
    assert len(root.handlers) == 1
