"""KR VCP/Minervini provenance must not fall back to Pullback."""
from types import SimpleNamespace

from trader.pb1_engine import PB1Engine
from trader.watchlist_builder import (
    ALLOWED_ENTRY_STYLES,
    WatchlistBuilder,
    sanitize_final30_entry_styles,
)


def test_explicit_vcp_survives_final30_sanitization():
    rows = sanitize_final30_entry_styles([{
        "code": "036930",
        "entry_style_selected": "ENTRY_VCP",
        "vcp_score": 80.0,
        "vcp_pass": True,
        "breakout_score": 35.0,
        "pullback_score": 40.0,
        "momentum_score": 42.0,
    }], stage="test")
    assert "VCP" in ALLOWED_ENTRY_STYLES
    assert rows[0]["entry_style_selected"] == "VCP"
    assert rows[0]["meta"]["entry_style_selected"] == "VCP"


def test_explicit_vcp_without_positive_evidence_does_not_override_score():
    builder = object.__new__(WatchlistBuilder)
    row = {
        "entry_style_selected": "VCP",
        "vcp_pass": False,
        "vcp_score": 80.0,
        "breakout_score": 10.0,
        "pullback_score": 90.0,
        "momentum_score": 30.0,
        "rs_percentile": 0.90,
        "rs_score": 0.90,
        "trend_score": 80.0,
        "volume": 10000.0,
        "volume_avg20": 9000.0,
    }
    builder._compute_tech_score(row)
    assert row["entry_style_selected"] == "PULLBACK"


def test_explicit_proven_vcp_remains_vcp_during_scoring():
    builder = object.__new__(WatchlistBuilder)
    row = {
        "entry_style_selected": "VCP",
        "vcp_pass": True,
        "vcp_score": 80.0,
        "breakout_score": 10.0,
        "pullback_score": 90.0,
        "momentum_score": 30.0,
        "rs_percentile": 0.90,
        "rs_score": 0.90,
        "trend_score": 80.0,
        "volume": 10000.0,
        "volume_avg20": 9000.0,
    }
    builder._compute_tech_score(row)
    assert row["entry_style_selected"] == "VCP"


def test_vcp_identity_survives_entry_plan_and_exit_mapping():
    engine = object.__new__(PB1Engine)
    cf = SimpleNamespace(
        features={"entry_style_selected": "VCP", "vcp_score": 75.0},
    )
    assert engine._resolve_entry_setup_family(cf) == "ENTRY_VCP"
    assert engine._infer_entry_family(cf, trigger_ok=True) == (
        "VCP", "ENTRY_VCP", "VCP_BREAKOUT",
    )
    identity = engine._resolve_entry_identity_from_mapping(cf.features)
    assert identity["entry_reason"] == "ENTRY_VCP"
    assert identity["entry_style_selected"] == "ENTRY_VCP"
    assert identity["exit_policy_family"] == "SWING_STAGED_EXIT"


def test_explicit_minervini_identity_is_not_generic_or_pullback():
    engine = object.__new__(PB1Engine)
    identity = engine._resolve_entry_identity_from_mapping({
        "entry_style_selected": "ENTRY_MINERVINI",
    })
    assert identity["entry_reason"] == "ENTRY_MINERVINI"
    assert identity["exit_policy_family"] == "SWING_STAGED_EXIT"
