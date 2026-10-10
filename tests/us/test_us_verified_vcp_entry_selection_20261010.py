"""US fourth entry family: verified VCP must be independently selectable."""
from trader.us.watchlist_builder import _select_entry_style
from trader.us.pb1.us_explain import (
    normalize_us_entry_style,
    validate_tradable_us_entry_style,
    build_us_entry_explanation,
)


def _scores(row: dict) -> str:
    return _select_entry_style(
        row, pb1_score=0.70, momentum_score=0.73,
        pullback_score=0.64, breakout_score=0.65,
        vcp_score=0.92,
    )


def test_verified_minervini_vcp_can_win_without_pullback_gate():
    row = {
        "vcp_pass": True,
        "trend_template_pass": True,
        "pivot_price": 100.0,
        "close": 101.0,
    }
    assert _scores(row) == "vcp"
    assert validate_tradable_us_entry_style("vcp") == (True, "ENTRY_VCP")


def test_unverified_vcp_score_is_never_entry_signal():
    # Low ATR/high trend can create a high proxy score without a genuine VCP.
    assert _scores({"vcp_pass": False, "trend_template_pass": True,
                    "pivot_price": 100.0, "close": 105.0}) != "vcp"
    assert _scores({"vcp_pass": True, "trend_template_pass": False,
                    "pivot_price": 100.0, "close": 105.0}) != "vcp"
    assert _scores({"vcp_pass": True, "trend_template_pass": True,
                    "pivot_price": 100.0, "close": 99.0}) != "vcp"
    assert _scores({"vcp_pass": True, "trend_template_pass": True,
                    "close": 105.0}) != "vcp"


def test_preexisting_style_selection_stays_unchanged_without_vcp_evidence():
    row = {}
    assert _scores(row) == "momentum"
    assert _select_entry_style(
        row, pb1_score=0.70, momentum_score=0.73,
        pullback_score=0.64, breakout_score=0.65,
    ) == "momentum"


def test_vcp_explanation_remains_proven_family_not_generic():
    entry = {
        "entry_style_selected": "vcp",
        "vcp_score": 0.92,
        "pivot_price": 100.0,
        "close": 101.0,
        "vcp_pass": True,
        "trend_template_pass": True,
    }
    explanation = build_us_entry_explanation(
        symbol="ABCD", entry_data=entry, decision="BUY",
    )
    assert explanation["entry_style_selected"] == "ENTRY_VCP"
    assert explanation["entry_component"] == "vcp_contraction"
    assert normalize_us_entry_style(explanation["entry_style_selected"]) == "ENTRY_VCP"
