"""Real Supabase 2026-10-08 KR 55D breakout proof to unchanged PB1 BUY gate.

These are snapshot-based offline tests, not KIS order submission.
The 55-point BREAKOUT policy remains unchanged.
"""
from datetime import date
import json
from pathlib import Path

import pandas as pd
import pytest

from trader.kr_four_family_candidate_admission import (
    completed_daily_candidate_proofs, completed_breakout_evidence,
)
from trader.watchlist_builder import WatchlistBuilder
from trader.pb1_engine import PB1Engine

DATA = json.loads(
    (Path(__file__).parent / "fixtures" / "supabase_kr_breakout_63bars_20261008.json").read_text()
)
DERIVED_SCORES = {
    "083450": 40.0, "115450": 66.1929824561404,
    "131970": 30.0, "373220": 40.0,
}
# Source: public.derived_minervini 2026-10-08, env=practice (read-only).
# Keep actual RS and ATR gates, rather than pretending these were risk-free.
DERIVED_RISK = {
    "083450": {"rs_percentile": .969230769230769, "atr_pct": .044798, "ma50": 46611.0, "ma150": 45242.667},
    "115450": {"rs_percentile": .871794871794872, "atr_pct": .095996, "ma50": 2141.940, "ma150": 2572.413},
    "131970": {"rs_percentile": .876923076923077, "atr_pct": .073253, "ma50": 82074.0, "ma150": 104134.0},
    "373220": {"rs_percentile": .676923076923077, "atr_pct": .034917, "ma50": 355610.0, "ma150": 382776.667},
}


def _bars(code, *, index_only=False):
    raw = DATA["samples"][code]
    df = pd.DataFrame(raw, columns=["date", "close", "high", "low", "volume"])
    for col in ("close", "high", "low", "volume"):
        df[col] = df[col].astype(float)
    if index_only:
        df.index = pd.to_datetime(df.pop("date"), format="%Y%m%d")
    return df


def _entry_replay(code, monkeypatch):
    monkeypatch.setenv("PB1_KR_FOUR_FAMILY_CANDIDATE_ENABLED", "1")
    asof = date(2026, 10, 8)
    df = _bars(code)
    screens = completed_daily_candidate_proofs(df, expected_as_of=asof)
    assert screens["BREAKOUT"] is True
    proof = completed_breakout_evidence(df, expected_as_of=asof)
    risk = DERIVED_RISK[code]
    row = {
        "code": code, "as_of": "2026-10-08",
        "candidate_family_proof_as_of": "2026-10-08",
        "close": proof["close"], "volume": proof["volume"],
        "volume_avg20": proof["average_volume20"],
        "candidate_family_screens": screens, "candidate_breakout_evidence": proof,
        "rs_percentile": risk["rs_percentile"], "atr_pct": risk["atr_pct"], "score_final": 85.,
        "pullback_score": 32., "momentum_score": 75.,
        "ma20": float(df["close"].tail(20).mean()),
        "ma50": risk["ma50"],
        "ma150": risk["ma150"], "meta": {},
    }
    fake_derived = {
        code: {"symbol": code, "as_of": "2026-10-08", "close": proof["close"],
               "breakout_score": DERIVED_SCORES[code],
               "rs_percentile": risk["rs_percentile"], "atr_pct": risk["atr_pct"], "trend_score": 80.,
               "vcp_score": 0., "momentum_score": 75., "pullback_score": 32.}
    }
    builder = WatchlistBuilder.__new__(WatchlistBuilder)
    builder._load_minervini_source_map = lambda _asof: fake_derived
    merged = builder._merge_derived_scores([row], as_of=asof)[0]
    return merged, proof


@pytest.mark.parametrize("code", list(DERIVED_SCORES))
def test_real_oct_breakout_proof_uses_existing_pb1_score_and_buy_policy(code, monkeypatch):
    row, proof = _entry_replay(code, monkeypatch)
    assert row["breakout_derived_score_before_reconciliation"] == DERIVED_SCORES[code]
    assert row["breakout_completed_proof_valid"] is True
    assert row["breakout_score_source"] == "completed_daily_pb1_55d"
    assert row["breakout_score"] >= 60.0  # unchanged PB1 scorer, not a hardcoded pass
    assert row["breakout_pivot_price"] == proof["pivot55"]
    assert row["meta"]["breakout_score"] == row["breakout_score"]
    assert row["meta"]["candidate_breakout_evidence"]["as_of"] == "20261008"

    engine = PB1Engine.__new__(PB1Engine)
    engine.require_volume = False
    engine.env = "practice"
    row["entry_style_selected"] = "BREAKOUT"
    engine._precomputed_final30_map = {code: dict(row)}
    engine._precomputed_derived_map = {}
    engine._precomputed_universe_map = {}
    mapped, checks, rejected_reasons, usable, data_ok = engine._map_precomputed_candidate_row(code)
    assert checks["has_breakout_score"] == 1, rejected_reasons
    assert mapped["candidate_family_screens"]["BREAKOUT"] is True
    assert mapped["breakout_completed_proof_valid"] is True
    assert mapped["breakout_score_source"] == "completed_daily_pb1_55d"
    ok, why, contract = engine._evaluate_final30_entry_setup(code, mapped, market="KOSPI")
    assert ok is True, why
    assert contract["entry_reason"] == "ENTRY_BREAKOUT"

    for modified in [
        {**row, "candidate_family_proof_as_of": "2026-10-07"},
        {**row, "breakout_completed_proof_valid": False},
        {**row, "breakout_score_source": "derived_minervini"},
        {**row, "candidate_breakout_evidence": {**row["candidate_breakout_evidence"], "volume": 0}},
        {**row, "candidate_family_screens": {"BREAKOUT": False}},
        {**row, "breakout_score": 54.9},
        {**row, "rs_percentile": 0.40},
        {**row, "atr_pct": 0.20},
    ]:
        eligible, reasons, _ = engine._evaluate_final30_entry_setup(code, modified, market="KOSPI")
        assert eligible is False and reasons


@pytest.mark.parametrize("code", list(DERIVED_SCORES))
def test_real_supabase_completed_asof_index_or_column_is_verified(code):
    df = _bars(code)
    assert completed_daily_candidate_proofs(df, expected_as_of="2026-10-08")["BREAKOUT"]
    assert completed_daily_candidate_proofs(_bars(code, index_only=True), expected_as_of="2026-10-08")["BREAKOUT"]
    assert not completed_daily_candidate_proofs(df, expected_as_of="2026-10-07")["BREAKOUT"]
    assert completed_breakout_evidence(df, expected_as_of="2026-10-07") == {}


def test_range_index_without_date_column_cannot_authorize_buy():
    df = _bars("083450").drop(columns=["date"])
    assert not any(completed_daily_candidate_proofs(df, expected_as_of="2026-10-08").values())


def test_missing_ohlcv_provenance_blocks_all_new_style_families(monkeypatch):
    monkeypatch.setenv("PB1_KR_FOUR_FAMILY_CANDIDATE_ENABLED", "1")
    engine = PB1Engine.__new__(PB1Engine)
    engine.require_volume = False
    for style in ("BREAKOUT", "MOMENTUM", "PULLBACK", "VCP"):
        result = engine._evaluate_final30_entry_setup("083450", {
            "entry_style_selected": style, "breakout_score": 100,
            "momentum_score": 100, "pullback_score": 100, "vcp_score": 100,
        }, "KOSPI")
        assert result[0] is False
        assert result[1]


def test_kr_minervini_vcp_requires_full_verified_source_and_exact_asof(monkeypatch):
    """A volatility proxy or bare vcp_pass may never enter as Minervini."""
    monkeypatch.setenv("PB1_KR_FOUR_FAMILY_CANDIDATE_ENABLED", "1")
    engine = PB1Engine.__new__(PB1Engine)
    engine.require_volume = False
    engine.env = "practice"
    base = {
        "as_of": "2026-10-08", "entry_style_selected": "VCP",
        "vcp_pass": True, "minervini_pass": True,
        "vcp_evidence_as_of": "2026-10-08",
        "vcp_score": 80., "rs_percentile": .95, "atr_pct": .04,
        "close": 70000., "current_price": 70000.,
    }
    ok, reasons, meta = engine._evaluate_final30_entry_setup("083450", base, "KOSDAQ")
    assert ok, reasons
    assert meta["entry_reason"] == "ENTRY_VCP"
    for diff in (
        {"vcp_pass": False}, {"minervini_pass": False},
        {"vcp_evidence_as_of": "2026-10-07"},
        {"vcp_evidence_as_of": None},
        {"vcp_score": 44.9},
        {"rs_percentile": .40}, {"atr_pct": .20},
    ):
        allowed, rejection, _ = engine._evaluate_final30_entry_setup(
            "083450", {**base, **diff}, "KOSDAQ"
        )
        assert allowed is False and rejection
