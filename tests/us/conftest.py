"""US test compatibility fixtures for PR128 strict persistent-state gates.

PR128 moved BUY-side pending/same-day state checks from the legacy fail-soft
repository helpers to strict persistent-state helpers. A few older unit-test
modules intentionally exercise in-memory/mocked repository state and therefore
need their existing mocks bridged to the new strict helper names.

This fixture is deliberately limited to those legacy modules. The dedicated
PR128 integrity tests are excluded so DB-unavailable/query-failure scenarios
continue to verify fail-closed production behavior.
"""
from __future__ import annotations

import pytest


_LEGACY_STRICT_STATE_COMPAT_MODULES = {
    "test_us_order_preflight_backfill.py",
    "test_us_pb1_entry_exit_contract.py",
    "test_us_risk_gate.py",
}


@pytest.fixture(autouse=True)
def _bridge_legacy_us_state_mocks(request, monkeypatch):
    path = getattr(request.node, "path", None)
    module_name = path.name if path is not None else request.node.fspath.basename
    if module_name not in _LEGACY_STRICT_STATE_COMPAT_MODULES:
        return

    from trader.us.db import repos, strict_order_state

    def _pending_strict_compat(
        symbol: str,
        side: str,
        trade_date: str | None = None,
        include_statuses: set[str] | None = None,
    ) -> bool:
        return repos.has_pending_order_for_symbol_side(
            symbol=symbol,
            side=side,
            trade_date=trade_date,
            include_statuses=include_statuses,
        )

    def _sold_strict_compat(trade_date: str | None = None) -> set[str]:
        return repos.load_today_symbols_sold(trade_date)

    monkeypatch.setattr(
        strict_order_state,
        "has_pending_order_for_symbol_side_strict",
        _pending_strict_compat,
    )
    monkeypatch.setattr(
        strict_order_state,
        "load_today_symbols_sold_strict",
        _sold_strict_compat,
    )
