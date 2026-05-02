# -*- coding: utf-8 -*-
"""tests/us/test_us_dispatcher_modes.py - dispatcher mode alias 테스트."""
import pytest


def test_normalize_mode_trade_am():
    from trader.us.runner.dispatcher import normalize_mode
    assert normalize_mode("trade-am") == "session-am"


def test_normalize_mode_trade_afternoon():
    from trader.us.runner.dispatcher import normalize_mode
    assert normalize_mode("trade-afternoon") == "session-afternoon"


def test_normalize_mode_trade_pm():
    from trader.us.runner.dispatcher import normalize_mode
    assert normalize_mode("trade-pm") == "session-afternoon"


def test_normalize_mode_trade_close():
    from trader.us.runner.dispatcher import normalize_mode
    assert normalize_mode("trade-close") == "close"


def test_normalize_mode_passthrough():
    from trader.us.runner.dispatcher import normalize_mode
    for mode in ("prep", "open", "mid", "close", "report", "all", "tick"):
        assert normalize_mode(mode) == mode


def test_modes_tuple_includes_aliases():
    from trader.us.runner.dispatcher import MODES
    for alias in ("trade-am", "trade-pm", "trade-afternoon", "trade-close"):
        assert alias in MODES, f"{alias!r} not in MODES"


def test_modes_tuple_includes_sessions():
    from trader.us.runner.dispatcher import MODES
    for m in ("session-am", "session-afternoon", "tick"):
        assert m in MODES, f"{m!r} not in MODES"
