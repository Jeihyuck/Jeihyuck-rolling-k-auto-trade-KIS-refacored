# -*- coding: utf-8 -*-
"""trade_tick_runner가 route_order 호출 시 kis_order_allowed를 전달하는지 확인."""
from __future__ import annotations

import pytest


def test_route_order_receives_kis_order_allowed_false(monkeypatch):
    """route_order 호출부에 kis_order_allowed가 전달되어야 한다 (소스 검증)."""
    import inspect
    import trader.us.runner.trade_tick_runner as mod

    src = inspect.getsource(mod)
    # route_order 호출 부분에서 kis_order_allowed=kis_order_allowed가 있는지 확인
    assert "kis_order_allowed=kis_order_allowed" in src, (
        "trade_tick_runner의 route_order 호출에 kis_order_allowed=kis_order_allowed가 없다"
    )


def test_route_order_receives_kis_order_allowed_true(monkeypatch):
    """kis_order_allowed=True(기본값)도 route_order에 전달되어야 한다."""
    import inspect
    import trader.us.runner.trade_tick_runner as mod

    src = inspect.getsource(mod)
    # route_order 호출부에 kis_order_allowed=kis_order_allowed가 있어야 한다
    assert "kis_order_allowed=kis_order_allowed" in src, (
        "trade_tick_runner의 route_order 호출에 kis_order_allowed=kis_order_allowed가 없다"
    )
