from __future__ import annotations

import math

from trader.config import TP1_SELL_PCT
from trader.kr.pb1.exit_simulation import resolve_force_exit_simulation
from trader.pb1_engine import PB1Engine


def test_resolve_force_exit_simulation_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("FORCE_EXIT_SIMULATION", raising=False)
    assert resolve_force_exit_simulation(code="000001", orderable_qty=10, exit_policy_family="FULL_EXIT") is None


def test_resolve_force_exit_simulation_tp1_and_family_hints(monkeypatch) -> None:
    monkeypatch.setenv("FORCE_EXIT_SIMULATION", "1")
    monkeypatch.setenv("FORCE_EXIT_CODE", "000001")
    monkeypatch.setenv("FORCE_EXIT_REASON", "tp1")

    helper = resolve_force_exit_simulation(code="000001", orderable_qty=10, exit_policy_family="FULL_EXIT")
    assert helper == {
        "reason": "TP1",
        "qty": max(1, int(math.ceil(10 * float(TP1_SELL_PCT)))),
        "stage": "TP1",
        "exit_policy_family": "MOMENTUM_EXIT",
    }


def test_pb1engine_force_exit_simulation_wrapper_matches_helper(monkeypatch) -> None:
    monkeypatch.setenv("FORCE_EXIT_SIMULATION", "1")
    monkeypatch.setenv("FORCE_EXIT_CODE", "000001")
    monkeypatch.setenv("FORCE_EXIT_REASON", "FAILED_BREAKOUT")

    engine = PB1Engine.__new__(PB1Engine)
    helper = resolve_force_exit_simulation(code="000001", orderable_qty=7, exit_policy_family="FULL_EXIT")
    wrapper = engine._resolve_force_exit_simulation(code="000001", orderable_qty=7, exit_policy_family="FULL_EXIT")

    assert helper == wrapper == {
        "reason": "FAILED_BREAKOUT",
        "qty": 7,
        "stage": "FAILED_BREAKOUT",
        "exit_policy_family": "PULLBACK_EXIT",
    }
