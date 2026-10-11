"""KR four-family candidate rescue against archived Supabase daily OHLCV."""
import json
from pathlib import Path

import pandas as pd
import pytest

from trader.kr_four_family_candidate_admission import (
    completed_daily_candidate_proofs,
    merge_verified_candidate_screens,
)

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "supabase_ohlcv_proof_replay_20261011.json").read_text())


@pytest.mark.parametrize("symbol", ["000150", "000250", "006120", "096530", "277810"])
def test_real_db_completed_breakout_survives_before_final30(symbol):
    bars = FIXTURE["samples"][symbol]["bars"]
    assert bars[-1][0] == "20261007"
    df = pd.DataFrame(bars, columns=("date", "close", "high", "low", "volume"))
    proof = completed_daily_candidate_proofs(df)
    assert proof["BREAKOUT"] is True
    assert proof["VCP"] is False  # Only the verified bridge can authorize VCP.


def test_proven_breakouts_absent_from_legacy_get_protected_without_quota():
    proven = []
    for code in ("000150", "000250", "006120", "096530", "277810"):
        bars = FIXTURE["samples"][code]["bars"]
        df = pd.DataFrame(bars, columns=("date", "close", "high", "low", "volume"))
        proven.append({"code": code, "score": 1, "candidate_family_screens": completed_daily_candidate_proofs(df)})
    legacy = [{"code": str(i).zfill(6), "score": 1000-i, "candidate_family_screens": {}} for i in range(500000, 500040)]
    selected, funnel = merge_verified_candidate_screens(legacy, legacy + proven, target_size=40)
    assert len(selected) == 40
    assert len({r["code"] for r in selected}) == 40
    assert {r["code"] for r in proven} <= {r["code"] for r in selected}
    assert funnel["eligible_by_family"]["BREAKOUT"] >= 5
    assert funnel["protected_count"] == len(proven)


def test_never_silently_discard_proven_signals_on_tight_cap():
    proof = {"BREAKOUT": True}
    rows = [{"code": str(i).zfill(6), "candidate_family_screens": proof} for i in range(5)]
    with pytest.raises(RuntimeError, match="FOUR_FAMILY_POOL_CAP_INSUFFICIENT"):
        merge_verified_candidate_screens([], rows, target_size=4)


def test_incomplete_data_not_a_valid_setup():
    assert completed_daily_candidate_proofs(pd.DataFrame([{"close": 100, "high": 101, "low": 99, "volume": 100}])) == {
        "PULLBACK": False, "MOMENTUM": False, "BREAKOUT": False, "VCP": False,
    }
