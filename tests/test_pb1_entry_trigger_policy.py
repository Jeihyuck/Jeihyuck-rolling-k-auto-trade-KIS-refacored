from __future__ import annotations

from trader.kr.pb1.entry_trigger_policy import resolve_entry_trigger_policy
from trader.pb1_engine import PB1Engine


def test_resolve_entry_trigger_policy_matches_baseline_cases() -> None:
    assert resolve_entry_trigger_policy(
        trigger_ok=True,
        entry_ok=False,
        setup_filters_ok=False,
        decision_family=None,
    ) == "BREAKOUT_CONFIRMED"
    assert resolve_entry_trigger_policy(
        trigger_ok=False,
        entry_ok=True,
        setup_filters_ok=True,
        decision_family="ENTRY_PULLBACK_OVERRIDE",
    ) == "PULLBACK_OVERRIDE"
    assert resolve_entry_trigger_policy(
        trigger_ok=False,
        entry_ok=True,
        setup_filters_ok=True,
        decision_family="ENTRY_MOMENTUM_CONTINUATION",
    ) == "MOMENTUM_CONTINUATION"
    assert resolve_entry_trigger_policy(
        trigger_ok=False,
        entry_ok=True,
        setup_filters_ok=True,
        decision_family="ENTRY_SCORE_OVERRIDE",
    ) == "SCORE_OVERRIDE"
    assert resolve_entry_trigger_policy(
        trigger_ok=False,
        entry_ok=True,
        setup_filters_ok=True,
        decision_family="ENTRY_SETUP_OVERRIDE",
    ) == "SETUP_OVERRIDE"
    assert resolve_entry_trigger_policy(
        trigger_ok=False,
        entry_ok=False,
        setup_filters_ok=True,
        decision_family="ENTRY_SETUP_OVERRIDE",
    ) == "NONE"


def test_pb1engine_entry_trigger_policy_wrapper_matches_helper() -> None:
    helper = resolve_entry_trigger_policy(
        trigger_ok=False,
        entry_ok=True,
        setup_filters_ok=True,
        decision_family="ENTRY_PULLBACK_OVERRIDE",
    )
    wrapper = PB1Engine._resolve_entry_trigger_policy(
        trigger_ok=False,
        entry_ok=True,
        setup_filters_ok=True,
        decision_family="ENTRY_PULLBACK_OVERRIDE",
    )
    assert helper == wrapper
