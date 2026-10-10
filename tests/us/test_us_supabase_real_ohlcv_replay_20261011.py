"""Real historical Supabase OHLCV -> current four-style PREP code, orders disabled.

Snapshots are immutable historical 260-bar samples from public.price_daily,
as captured 2026-10-11. All tests use the completed *prior* US session,
not contemporary quote data. This tests signal eligibility and arbitration
in actual repository Python functions, NOT KIS orders, fills, or account gates.
"""
import json
from pathlib import Path

import pytest

from trader.us.candidate_pool_builder import (
    _compute_us_explicit_signal_proofs,
    _compute_verified_us_minervini_vcp,
    _score_symbol_candidate,
)
from trader.us.watchlist_builder import _compute_agent_b_score


_DATA = json.loads(
    (Path(__file__).parent / "fixtures" / "supabase_ohlcv_proof_replay_20261011.json").read_text()
)


@pytest.mark.parametrize(
    "key, expected_momentum, expected_breakout",
    [
        ("2026-10-08:ABBV", True, True),
        ("2026-10-08:AMD", True, True),
        ("2026-10-08:CIEN", False, True),
        ("2026-10-08:MSTR", True, False),
        ("2026-10-09:PLTR", True, True),
    ],
)
def test_supabase_completed_daily_ohlcv_yields_proven_independent_signals(
    key, expected_momentum, expected_breakout, monkeypatch,
):
    # A flag-on offline replay; production/default flags remain unchanged.
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("US_KIS_ORDER_ALLOWED", "0")
    monkeypatch.setenv("ALLOW_REAL_ORDER", "0")

    example = _DATA["samples"][key]
    rows = [
        {"xymd": date, "date": date, "clos": close, "high": high,
         "low": low, "tvol": volume}
        for date, close, high, low, volume in example["bars"]
    ]
    assert len(rows) == 260
    last_close = float(rows[-1]["clos"])
    assert last_close > 0

    # Use the real new PREP proof producers, not a reimplementation of
    # indicator calculations in a test or hand-crafted boolean flags.
    proofs = _compute_us_explicit_signal_proofs(example["symbol"], rows)
    assert proofs["independent_entry_contract_v1"] is True
    assert proofs["entry_signal_proof_source"] == "completed_daily_ohlcv"
    assert proofs["momentum_pass"] is expected_momentum
    assert proofs["breakout_pass"] is expected_breakout

    vcp = _compute_verified_us_minervini_vcp(rows)
    assert vcp["vcp_evidence_source"] == "completed_daily_ohlcv"
    assert vcp["vcp_pass"] is False

    # Candidate derived from real OHLCV; for deterministic holiday replay
    # use completed-session close rather than an unrecorded live quote.
    volumes = [float(row["tvol"]) for row in rows[-20:]]
    avg_volume = sum(volumes) / len(volumes)
    candidate = _score_symbol_candidate(
        {"symbol": example["symbol"], "exchange": "NASDAQ",
         "price": last_close, "avg_volume_20d": avg_volume,
         "avg_dollar_volume_20d": avg_volume * last_close,
         "history_days": 260, "asset_type": "stock"},
        rows, [], [], [],
    )
    candidate.update(proofs)
    candidate.update(vcp)
    candidate["close"] = last_close
    scores = _compute_agent_b_score(candidate, [])
    selected = scores[2]
    eligibility = candidate["independent_eligible_entry_styles"]

    assert "pb1_pullback" in eligibility
    assert ("momentum" in eligibility) is expected_momentum
    assert ("breakout" in eligibility) is expected_breakout
    assert candidate["independent_proof_status"]["momentum"] is expected_momentum
    assert candidate["independent_proof_status"]["breakout"] is expected_breakout
    assert selected in eligibility

    # Real proof does not guarantee this family wins the one-order-per-symbol
    # score arbitration; do not misreport an eligible signal as a broker BUY.
    if example["symbol"] == "CIEN":
        assert selected == "breakout"
    if example["symbol"] == "PLTR":
        assert selected == "pb1_pullback"


def test_historical_supabase_replay_is_strictly_pretrade_and_not_orders():
    assert _DATA["source"] == "Supabase public.price_daily, market=US"
    for example in _DATA["samples"].values():
        dates = [row[0] for row in example["bars"]]
        assert len(dates) == len(set(dates)) == 260
        assert dates == sorted(dates)
        assert dates[-1] < example["trade_date"].replace("-", "")
