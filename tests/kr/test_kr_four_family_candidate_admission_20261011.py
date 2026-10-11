"""KR four-family candidate rescue against archived Supabase daily OHLCV."""
import json
from pathlib import Path

import pandas as pd
import pytest

from trader.kr_four_family_candidate_admission import (
    completed_daily_candidate_proofs,
    merge_verified_candidate_screens,
    apply_verified_family_final_arbitration,
)

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "supabase_ohlcv_proof_replay_20261011.json").read_text())


@pytest.mark.parametrize("symbol", ["006120", "096530"])
def test_real_db_completed_breakout_survives_before_final30(symbol):
    bars = FIXTURE["samples"][symbol]["bars"]
    assert bars[-1][0] == "20261007"
    df = pd.DataFrame(bars, columns=("date", "close", "high", "low", "volume"))
    proof = completed_daily_candidate_proofs(df, expected_as_of=FIXTURE["latest_completed"])
    assert proof["BREAKOUT"] is True
    assert proof["VCP"] is False  # Only the verified bridge can authorize VCP.


@pytest.mark.parametrize("symbol", ["000150", "000250", "277810"])
def test_stale_february_ohlcv_is_not_october_breakout_signal(symbol):
    bars = FIXTURE["samples"][symbol]["bars"]
    assert bars[-1][0] == "20260226"  # Archived stale-data regression.
    df = pd.DataFrame(bars, columns=("date", "close", "high", "low", "volume"))
    assert not any(completed_daily_candidate_proofs(df, expected_as_of=FIXTURE["latest_completed"]).values())


def test_proven_breakouts_absent_from_legacy_get_protected_without_quota():
    proven = []
    for code in ("006120", "096530"):
        bars = FIXTURE["samples"][code]["bars"]
        df = pd.DataFrame(bars, columns=("date", "close", "high", "low", "volume"))
        proven.append({"code": code, "score": 1, "candidate_family_screens": completed_daily_candidate_proofs(df, expected_as_of=FIXTURE["latest_completed"])})
    legacy = [{"code": str(i).zfill(6), "score": 1000-i, "candidate_family_screens": {}} for i in range(500000, 500040)]
    selected, funnel = merge_verified_candidate_screens(legacy, legacy + proven, target_size=40)
    assert len(selected) == 40
    assert len({r["code"] for r in selected}) == 40
    assert {r["code"] for r in proven} <= {r["code"] for r in selected}
    assert funnel["eligible_by_family"]["BREAKOUT"] == 2
    assert funnel["protected_count"] == len(proven)


def test_overcapacity_is_reported_without_silent_signal_loss_or_failing_prep():
    proof = {"BREAKOUT": True}
    rows = [{"code": str(i).zfill(6), "candidate_family_screens": proof} for i in range(100, 105)]
    selected, report = merge_verified_candidate_screens([], rows, target_size=4)
    assert len(selected) == 4
    assert report["eligible_total"] == 5
    assert report["capacity_rejected_count"] == 1
    assert report["capacity_rejected_sample"]
    assert sum("qualified_but_capacity_rejected" in r.get("reject_reasons", []) for r in rows) == 1


def test_incomplete_data_not_a_valid_setup():
    assert completed_daily_candidate_proofs(pd.DataFrame([{"close": 100, "high": 101, "low": 99, "volume": 100}])) == {
        "PULLBACK": False, "MOMENTUM": False, "BREAKOUT": False, "VCP": False,
    }


def test_final30_style_is_proof_owner_not_pullback_raw_score():
    rows = [{
        "code": "006120", "score_final": 80.0, "pullback_score": 99.,
        "breakout_score": 60., "momentum_score": 85.,
        "candidate_family_screens": {"PULLBACK": False, "MOMENTUM": True, "BREAKOUT": False, "VCP": False},
        "candidate_family_quality_percentiles": {"MOMENTUM": .95},
        "entry_style_selected": "PULLBACK", "meta": {},
    }]
    result = apply_verified_family_final_arbitration(rows)
    assert result[0]["entry_style_selected"] == "MOMENTUM"
    assert result[0]["meta"]["entry_style_selected"] == "MOMENTUM"
    assert result[0]["score_final"] == 87.5
    assert result[0]["selected_family_quality_percentile"] == .95


def test_overcapacity_selects_by_family_relative_strength_not_fixed_quota():
    rows = [
        {"code": str(i+100).zfill(6), "score": i+1,
         "candidate_family_screens": {"MOMENTUM": True}} for i in range(12)
    ] + [
        {"code": "001001", "score": 100,
         "candidate_family_screens": {"BREAKOUT": True}}
    ]
    selected, report = merge_verified_candidate_screens([], rows, target_size=5)
    assert len(selected) == 5
    assert report["eligible_total"] == 13
    assert report["capacity_rejected_count"] == 8
    assert "001001" in {r["code"] for r in selected}
    assert all("candidate_family_quality_percentiles" in row for row in selected)
