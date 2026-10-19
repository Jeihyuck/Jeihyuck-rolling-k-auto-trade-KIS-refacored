"""October 19 Final30 proof provenance and four-family runtime acceptance.

Evidence modeled after the uploaded scored Final30 JSON; these are offline
tests, and passing a family proof is not equivalent to buying the stock.
"""
from __future__ import annotations

from datetime import date

import pytest

from trader.kr.final30_proof import copy_final30_completed_daily_proof
from trader.pb1_engine import PB1Engine


@pytest.fixture
def final30_breakout():
    return {
        "code": "096530", "entry_style_selected": "BREAKOUT",
        "as_of": "2026-10-16",
        "candidate_family_proof_as_of": "2026-10-16",
        "candidate_family_screens": {
            "PULLBACK": False, "MOMENTUM": True,
            "BREAKOUT": True, "VCP": False,
        },
        "candidate_breakout_evidence": {
            "source": "completed_daily_ohlcv",
            "as_of": "20261016",
            "pivot55": 10000.0,
            "close": 11000.0,
            "volume": 30000.0,
            "average_volume20": 15000.0,
        },
        "breakout_completed_proof_valid": True,
        "breakout_score_source": "completed_daily_pb1_55d",
        "breakout_pivot_price": 10000.0,
        "candidate_family_quality_percentiles": {"BREAKOUT": 0.98},
        "selected_family_quality_percentile": 0.98,
        "exit_policy_family": "SWING_STAGED_EXIT",
    }


def test_final30_proof_survives_runtime_feature_projection(monkeypatch, final30_breakout):
    monkeypatch.setenv("PB1_KR_FOUR_FAMILY_CANDIDATE_ENABLED", "1")
    features = copy_final30_completed_daily_proof(
        {"entry_style_selected": "BREAKOUT", "score_final": 92.0},
        final30_breakout, expected_code="096530",
    )
    assert features["candidate_family_proof_as_of"] == "2026-10-16"
    assert features["candidate_family_screens"]["BREAKOUT"] is True
    engine = PB1Engine.__new__(PB1Engine)
    ok, reasons, _ = engine._evaluate_final30_entry_setup("096530", features, "KOSDAQ")
    assert "completed_daily_proof_date_mismatch" not in reasons
    assert "completed_daily_family_proof_missing" not in reasons
    assert "completed_daily_breakout_score_proof_missing" not in reasons


def test_missing_date_or_missing_evidence_fails_closed(monkeypatch, final30_breakout):
    monkeypatch.setenv("PB1_KR_FOUR_FAMILY_CANDIDATE_ENABLED", "1")
    engine = PB1Engine.__new__(PB1Engine)
    for corrupt in (
        {**final30_breakout, "candidate_family_proof_as_of": None},
        {**final30_breakout, "candidate_family_proof_as_of": "2026-10-15"},
        {**final30_breakout, "candidate_family_screens": None},
        {**final30_breakout, "breakout_completed_proof_valid": False},
    ):
        features = copy_final30_completed_daily_proof(
            {"entry_style_selected": "BREAKOUT"}, corrupt, expected_code="096530",
        )
        ok, reasons, _ = engine._evaluate_final30_entry_setup("096530", features, "KOSDAQ")
        assert not ok
        assert reasons


def test_source_code_mismatch_never_imports_foreign_candidate_proof(final30_breakout):
    result = copy_final30_completed_daily_proof(
        {"entry_style_selected": "BREAKOUT"}, final30_breakout,
        expected_code="192820",
    )
    assert "candidate_family_proof_as_of" not in result
    assert "candidate_family_screens" not in result


def test_proof_transfer_copies_source_values_not_reference(final30_breakout):
    copied = copy_final30_completed_daily_proof({}, final30_breakout, expected_code="096530")
    assert copied["as_of"] == "2026-10-16"
    copied["candidate_family_screens"]["BREAKOUT"] = False
    assert final30_breakout["candidate_family_screens"]["BREAKOUT"] is True


def test_missing_final30_row_never_invents_completed_daily_proof():
    result = copy_final30_completed_daily_proof(
        {"entry_style_selected": "BREAKOUT", "score_final": 100}, None,
        expected_code="096530",
    )
    assert "as_of" not in result
    assert "candidate_family_screens" not in result
