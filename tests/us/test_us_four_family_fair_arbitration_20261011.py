"""Real Supabase OHLCV and negative-proof US four-family arbitration regression."""
import json
from pathlib import Path

import pytest

from trader.us.candidate_pool_builder import _compute_us_explicit_signal_proofs, _score_symbol_candidate
from trader.us.watchlist_builder import (
    _compute_pb1_score, _compute_momentum_score, _compute_breakout_score, _compute_vcp_score,
)
from trader.us.four_family_arbitration import eligible_family_scores, rank_verified_families

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "supabase_ohlcv_proof_replay_20261011.json").read_text())


def _replay(key):
    item = FIXTURE["samples"][key]
    bars = item["bars"]
    assert len(bars) == 260
    daily = [
        {"xymd": r[0], "clos": r[1], "high": r[2], "low": r[3], "tvol": r[4]}
        for r in bars
    ]
    close = bars[-1][1]
    sym = {"symbol": item["symbol"], "price": close, "atr_pct": .04}
    candidate = _score_symbol_candidate(sym, daily, [], [], [])
    candidate.update(_compute_us_explicit_signal_proofs(item["symbol"], daily))
    candidate.update({
        "pb1_score": _compute_pb1_score(candidate, daily),
        "momentum_score": _compute_momentum_score(candidate),
        "breakout_score": _compute_breakout_score(candidate),
        "vcp_score": _compute_vcp_score(candidate),
        "score_final": .6,
        "reason_json": {},
    })
    assert bars[-1][0] < item["trade_date"].replace("-", "")
    return candidate


def test_proofless_high_pullback_score_does_not_overrule_real_breakout():
    row = {
        "independent_entry_contract_v1": True, "entry_signal_proof_source": "completed_daily_ohlcv",
        "pullback_pass": False, "momentum_pass": False, "breakout_pass": True,
        "breakout_pivot_price": 90, "pb1_score": .99, "breakout_score": .50,
        "score_final": .5, "reason_json": {},
    }
    assert eligible_family_scores(row) == {"breakout": .5}
    got, meta = rank_verified_families([row])
    assert meta["family_qualified"]["pb1_pullback"] == 0
    assert got[0]["entry_style_selected"] == "breakout"


@pytest.mark.parametrize("key", list(FIXTURE["samples"]))
def test_real_supabase_ohlcv_is_evaluated_by_all_four_families(key):
    row = _replay(key)
    scores = eligible_family_scores(row)
    # Fake raw PB1 quality never itself proves pullback.
    assert ("pb1_pullback" in scores) is row["pullback_pass"]
    if row["breakout_pass"]:
        assert "breakout" in scores
    if row["momentum_pass"]:
        assert "momentum" in scores
    assert "vcp" not in scores  # no verified VCP from ATR proxy
    assert scores  # Each archived case has at least one actual proof.


def test_actual_supabase_cohort_no_arbitrary_strategy_quota():
    rows = [_replay(k) for k in FIXTURE["samples"]]
    selected, meta = rank_verified_families(rows)
    assert meta["qualified"] == len(rows)
    assert len({row["symbol"] for row in selected}) == len(rows)
    assert all(row["entry_style_selected"] in eligible_family_scores(row) for row in selected)
    assert any(row["entry_style_selected"] == "breakout" for row in selected)
    assert all(row["reason_json"]["entry_style_selected"] == row["entry_style_selected"] for row in selected)


def test_never_create_placeholder_pb1_signal():
    rows, meta = rank_verified_families([{"pb1_score": 1., "score_final": 1., "reason_json": {}}])
    assert rows == []
    assert meta["no_valid_family"] == 1
