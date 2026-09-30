from __future__ import annotations

from trader.us.infinite.risk_adapter import (
    apply_tqqq_runtime_entry_override,
    effective_regime,
)
from trader.us.runner.tick_entry_permission import (
    resolve_shared_tick_entry_evaluation_permission,
)


def _overlay(**overrides):
    base = {
        "market_state": "DEFENSE_RISK_OFF",
        "market_regime": "DEFENSIVE",
        "entry_can_proceed": False,
        "exit_can_proceed": True,
        "trade_block_reason": "sector_cap_violation_block",
        "tqqq_context_quality": "ok",
        "tqqq_quote_stale": False,
    }
    base.update(overrides)
    return base


def _guard(**overrides):
    base = {
        "guard_state": "PREP_DEGRADED_ENTRY_BLOCKED",
        "entry_can_proceed": False,
        "exit_can_proceed": True,
        "trade_block_reason": "sector_cap_violation_block",
        "run_revision_mismatch": False,
    }
    base.update(overrides)
    return base


def test_pb1_sector_cap_does_not_reblock_healthy_tqqq_owner():
    overlay = _overlay()

    regime, multiplier, _reserve, entry_allowed, _reason = effective_regime(overlay)

    assert regime in {"RISK_OFF", "DEFENSIVE"}
    assert multiplier == 0.5
    assert entry_allowed is True
    assert overlay["entry_can_proceed"] is True
    assert overlay["tqqq_runtime_entry_override"] is True
    assert overlay["tqqq_runtime_entry_override_reason"] == "sector_cap_violation_block"


def test_unknown_operational_block_stays_fail_closed_for_tqqq():
    overlay = _overlay(trade_block_reason="prep_missing_after_preflight_recovery")

    changed, reason = apply_tqqq_runtime_entry_override(overlay)

    assert changed is False
    assert reason == "prep_missing_after_preflight_recovery"
    assert overlay["entry_can_proceed"] is False


def test_stale_tqqq_quote_cannot_bypass_pb1_gate():
    overlay = _overlay(tqqq_quote_stale=True)

    changed, reason = apply_tqqq_runtime_entry_override(overlay)

    assert changed is False
    assert reason == "tqqq_operational_safety_not_proven"
    assert overlay["entry_can_proceed"] is False


def test_session_policy_only_block_allows_owner_specific_tick_evaluation():
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(), timeout_entry_block=False, session_execution_mode="NORMAL"
    ) is True


def test_preflight_exit_only_never_promotes_tqqq_evaluation():
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(
            guard_state="PREFLIGHT_EXIT_ONLY",
            trade_block_reason="prep_missing_after_preflight_recovery",
        ),
        timeout_entry_block=False,
        session_execution_mode="NORMAL",
    ) is False


def test_revision_mismatch_never_promotes_tqqq_without_validated_recovery():
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(run_revision_mismatch=True),
        timeout_entry_block=False,
        session_execution_mode="NORMAL",
    ) is False
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(run_revision_mismatch=True, revision_mismatch_recovery_allowed=True),
        timeout_entry_block=False,
        session_execution_mode="NORMAL",
    ) is True


def test_safe_degraded_and_timeout_remain_global_buy_fences():
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(), timeout_entry_block=False, session_execution_mode="SAFE_DEGRADED"
    ) is False
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(), timeout_entry_block=True, session_execution_mode="NORMAL"
    ) is False


def test_unknown_or_noncanonical_degraded_reason_remains_fail_closed():
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(trade_block_reason="data_sync_quality_entry_block"),
        timeout_entry_block=False,
        session_execution_mode="NORMAL",
    ) is False
    assert resolve_shared_tick_entry_evaluation_permission(
        _guard(guard_state="PREP_MISSING_EXIT_ONLY"),
        timeout_entry_block=False,
        session_execution_mode="NORMAL",
    ) is False
