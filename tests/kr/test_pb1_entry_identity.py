from __future__ import annotations

from types import SimpleNamespace

from trader.kr.pb1.entry_identity import resolve_entry_identity_from_mapping
from trader.pb1_engine import PB1Engine


def test_resolve_entry_identity_from_mapping_preserves_aliases_and_fallback():
    assert resolve_entry_identity_from_mapping({"entry_reason": "BREAKOUT"}) == {
        "entry_reason": "ENTRY_BREAKOUT",
        "entry_style_selected": "ENTRY_BREAKOUT",
        "entry_decision_family": "ENTRY_BREAKOUT",
        "exit_policy_family": "BREAKOUT_EXIT",
    }
    assert resolve_entry_identity_from_mapping({"entry_style_selected": "ENTRY_PULLBACK_OVERRIDE"}) == {
        "entry_reason": "ENTRY_PULLBACK",
        "entry_style_selected": "ENTRY_PULLBACK",
        "entry_decision_family": "ENTRY_PULLBACK",
        "exit_policy_family": "PULLBACK_EXIT",
    }
    assert resolve_entry_identity_from_mapping({"entry_signal": "ENTRY_MOMENTUM_CONTINUATION"}) == {
        "entry_reason": "ENTRY_MOMENTUM",
        "entry_style_selected": "ENTRY_MOMENTUM",
        "entry_decision_family": "ENTRY_MOMENTUM",
        "exit_policy_family": "MOMENTUM_EXIT",
    }
    assert resolve_entry_identity_from_mapping({"entry_reason": "other"}) == {
        "entry_reason": "ENTRY_GENERIC",
        "entry_style_selected": "ENTRY_GENERIC",
        "entry_decision_family": "ENTRY_GENERIC",
        "exit_policy_family": "GENERIC_EXIT",
    }
    assert resolve_entry_identity_from_mapping({}) == {
        "entry_reason": "ENTRY_GENERIC",
        "entry_style_selected": "ENTRY_GENERIC",
        "entry_decision_family": "ENTRY_GENERIC",
        "exit_policy_family": "GENERIC_EXIT",
    }


def test_pb1engine_entry_identity_wrapper_matches_helper():
    engine = PB1Engine.__new__(PB1Engine)
    engine._normalize_entry_reason = PB1Engine._normalize_entry_reason
    engine._resolve_exit_family = PB1Engine._resolve_exit_family

    cf = SimpleNamespace(
        features={
            "entry_reason": "BREAKOUT",
            "entry_style_selected": "ENTRY_BREAKOUT_CONFIRMED",
            "entry_decision_family": "entry_breakout_confirmed",
        }
    )

    assert engine._resolve_entry_identity_from_mapping(cf.features) == {
        "entry_reason": "ENTRY_BREAKOUT",
        "entry_style_selected": "ENTRY_BREAKOUT",
        "entry_decision_family": "ENTRY_BREAKOUT_CONFIRMED",
        "exit_policy_family": "BREAKOUT_EXIT",
    }
    assert engine._resolve_entry_identity_for_candidate(cf) == engine._resolve_entry_identity_from_mapping(cf.features)
