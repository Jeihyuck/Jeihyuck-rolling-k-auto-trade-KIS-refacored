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

_LEGACY_EXECUTION_CLAIM_COMPAT_MODULES = {
    "test_us_20260707_emergency.py",
    "test_us_20260804_second_round.py",
    "test_us_dual_agent_pr17_fixes.py",
    "test_us_execution_integrity_incidents.py",
    "test_us_order_ack_db_failure.py",
    "test_us_tqqq_complete_isolation.py",
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


@pytest.fixture(autouse=True)
def _bridge_legacy_execution_claim_mocks(request, monkeypatch):
    path = getattr(request.node, "path", None)
    module_name = path.name if path is not None else request.node.fspath.basename
    if module_name not in _LEGACY_EXECUTION_CLAIM_COMPAT_MODULES:
        return

    from trader.execution_claims import ExecutionClaim
    from trader.execution_state import SemanticActionIdentity
    from trader.us.execution import order_router
    from trader.us.db import repos

    def _identity(intent, *, account_env):
        meta = intent.get("meta") if isinstance(intent.get("meta"), dict) else {}
        return SemanticActionIdentity(
            env=account_env,
            account_id="test-account",
            market="US",
            trading_epoch_id="test-epoch",
            strategy_owner=str(intent.get("strategy_owner") or meta.get("strategy_owner") or "US_STANDARD"),
            lifecycle_id=str(
                intent.get("position_lifecycle_id")
                or meta.get("position_lifecycle_id")
                or intent.get("position_cycle_id")
                or meta.get("position_cycle_id")
                or f"ENTRY:{intent.get('client_order_key') or intent.get('order_key') or ''}"
            ),
            action=str(
                intent.get("semantic_action")
                or meta.get("semantic_action")
                or intent.get("profit_capture_stage")
                or meta.get("profit_capture_stage")
                or intent.get("stage")
                or intent.get("reason")
                or "ENTRY"
            ),
        )

    def _claim(identity, *, attempt_id, requested_qty, fresh_validation=False, client_order_key=None):
        return ExecutionClaim(True, identity.action_key, attempt_id)

    monkeypatch.setattr(order_router, "_semantic_action_identity", _identity)
    monkeypatch.setattr(repos, "claim_execution_action", _claim)
    monkeypatch.setattr(repos, "record_execution_action_observation", lambda *args, **kwargs: None)
