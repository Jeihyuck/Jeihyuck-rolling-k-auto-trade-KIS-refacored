from __future__ import annotations

import sys
import pytest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.append(str(Path(__file__).resolve().parents[1]))

import trader.kis_wrapper as kis_wrapper


def test_wait_before_order_submit_enforces_hashkey_gap(monkeypatch) -> None:
    monkeypatch.setenv("PB1_KIS_RATE_LIMIT_SAFE", "1")
    monkeypatch.setattr(kis_wrapper.random, "uniform", lambda _a, _b: 0.0)
    sleep_calls: list[float] = []
    monkeypatch.setattr(kis_wrapper.time, "sleep", lambda sec: sleep_calls.append(sec))
    monkeypatch.setattr(kis_wrapper.time, "time", lambda: 100.2)

    api = kis_wrapper.KisAPI.__new__(kis_wrapper.KisAPI)
    api._order_limiter = MagicMock()
    api._limiter = MagicMock()
    api._last_hashkey_at = 100.0
    api._order_hashkey_gap_sec = 0.5

    api._wait_before_order_submit()

    assert sleep_calls == [pytest.approx(0.3)]
    api._order_limiter.wait.assert_called_once_with("orders-safe")
    api._limiter.wait.assert_called_once_with("orders")


def test_wait_before_order_submit_skips_gap_when_flag_disabled(monkeypatch) -> None:
    monkeypatch.delenv("PB1_KIS_RATE_LIMIT_SAFE", raising=False)
    sleep_calls: list[float] = []
    monkeypatch.setattr(kis_wrapper.time, "sleep", lambda sec: sleep_calls.append(sec))

    api = kis_wrapper.KisAPI.__new__(kis_wrapper.KisAPI)
    api._order_limiter = MagicMock()
    api._limiter = MagicMock()
    api._last_hashkey_at = 100.0
    api._order_hashkey_gap_sec = 0.5

    api._wait_before_order_submit()

    assert sleep_calls == []
    api._order_limiter.wait.assert_not_called()
    api._limiter.wait.assert_called_once_with("orders")