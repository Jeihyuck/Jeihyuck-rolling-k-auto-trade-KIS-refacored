from __future__ import annotations

from trader.kr.pb1.entry_family import resolve_entry_decision_family, resolve_entry_setup_family
from trader.pb1_engine import CandidateFeature, PB1Engine


def _make_cf(**overrides):
    features = {
        "entry_style_selected": "ENTRY_PULLBACK",
        "entry_signal": "ENTRY_PULLBACK",
        "breakout_score": 0.0,
        "pullback_score": 0.0,
        "momentum_score": 0.0,
    }
    features.update(overrides)
    return CandidateFeature(
        code="000001",
        market="KOSDAQ",
        features=features,
        setup_ok=True,
        reasons=["ok"],
        mode=1,
        mode_reasons=[],
    )


def test_resolve_entry_setup_family_prefers_selected_positive_score() -> None:
    assert resolve_entry_setup_family(
        entry_style_selected="ENTRY_BREAKOUT",
        entry_signal=None,
        breakout_score=1.0,
        pullback_score=0.0,
        momentum_score=0.0,
    ) == "ENTRY_BREAKOUT"


def test_resolve_entry_setup_family_falls_back_to_strongest_positive_score() -> None:
    assert resolve_entry_setup_family(
        entry_style_selected="ENTRY_GENERIC",
        entry_signal=None,
        breakout_score=0.0,
        pullback_score=2.0,
        momentum_score=1.0,
    ) == "ENTRY_PULLBACK"


def test_resolve_entry_decision_family_matches_baseline_override_rules() -> None:
    assert resolve_entry_decision_family(
        entry_reason="ENTRY_PULLBACK",
        setup_filters_ok=True,
        breakout_trigger_ok=False,
    ) == "ENTRY_PULLBACK_OVERRIDE"
    assert resolve_entry_decision_family(
        entry_reason="ENTRY_MOMENTUM",
        setup_filters_ok=True,
        breakout_trigger_ok=False,
    ) == "ENTRY_MOMENTUM_CONTINUATION"
    assert resolve_entry_decision_family(
        entry_reason="ENTRY_GENERIC",
        setup_filters_ok=True,
        breakout_trigger_ok=False,
        trigger_reason="score_gate",
    ) == "ENTRY_SCORE_OVERRIDE"
    assert resolve_entry_decision_family(
        entry_reason="ENTRY_GENERIC",
        setup_filters_ok=False,
        breakout_trigger_ok=False,
        trigger_reason=None,
    ) == "ENTRY_SETUP_OVERRIDE"


def test_pb1engine_entry_family_wrappers_match_helpers() -> None:
    engine = PB1Engine.__new__(PB1Engine)
    cf = _make_cf(entry_style_selected="ENTRY_BREAKOUT", breakout_score=1.0)

    helper_setup = resolve_entry_setup_family(
        entry_style_selected=cf.features.get("entry_style_selected"),
        entry_signal=cf.features.get("entry_signal"),
        breakout_score=float(cf.features.get("breakout_score") or 0.0),
        pullback_score=float(cf.features.get("pullback_score") or 0.0),
        momentum_score=float(cf.features.get("momentum_score") or 0.0),
    )
    wrapper_setup = PB1Engine._resolve_entry_setup_family(engine, cf)
    assert helper_setup == wrapper_setup

    helper_decision = resolve_entry_decision_family(
        entry_reason="ENTRY_PULLBACK",
        setup_filters_ok=True,
        breakout_trigger_ok=False,
    )
    wrapper_decision = PB1Engine._resolve_entry_decision_family(
        entry_reason="ENTRY_PULLBACK",
        setup_filters_ok=True,
        breakout_trigger_ok=False,
    )
    assert helper_decision == wrapper_decision
