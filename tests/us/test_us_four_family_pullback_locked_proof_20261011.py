"""Real Supabase ABBV OHLCV -> PB1 proof -> locked DB -> BUY gate -> frozen audit.

Test-only synthetic quote; never submits an order to a KIS broker.
"""
import json
from pathlib import Path

from trader.us.candidate_pool_builder import _compute_us_explicit_signal_proofs
from trader.us.db.repos import _merge_us_daily_metrics_meta
from trader.us.pb1.us_entry_engine import _validate_us_independent_new_buy_proof
from trader.us.score_columns import canonicalize_us_watchlist_row
from trader.us.entry_exit_contract import build_us_entry_exit_contract


def test_real_abbv_completed_pullback_proof_survives_locked_buy(monkeypatch):
    from trader.us.db import repos

    sample = json.loads(
        (Path(__file__).parent / "fixtures" / "supabase_ohlcv_proof_replay_20261011.json").read_text()
    )["samples"]["2026-10-08:ABBV"]
    rows = [
        {"xymd": date, "clos": close, "high": high, "low": low, "tvol": volume}
        for date, close, high, low, volume in sample["bars"]
    ]
    assert rows[-1]["xymd"] == "20261007"
    proof = _compute_us_explicit_signal_proofs("ABBV", rows)
    assert proof["pullback_pass"] is True
    assert proof["pullback_completed_close"] == rows[-1]["clos"]

    monkeypatch.setenv("US_FOUR_FAMILY_FAIR_ARBITRATION_ENABLED", "1")
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    monkeypatch.setenv("US_KIS_ORDER_ALLOWED", "0")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos.reset_memory_stores()

    entry = {
        **proof, "symbol": "ABBV", "exchange": "NASDAQ",
        "score": .90, "score_final": .90, "strategy": "pb1_pullback",
        "entry_style_raw": "pb1_pullback", "entry_style_selected": "pb1_pullback",
        "reasons": ["ENTRY_PULLBACK"], "filters_passed": ["score", "liquidity"],
        "score_breakdown": {"pullback": .9}, "rank_final30": 1,
        "data_source": "completed_daily", "reason_json": {"entry_style_selected": "pb1_pullback"},
        "meta": _merge_us_daily_metrics_meta(proof),
    }
    saved = repos.clear_and_save_locked_us_watchlist(
        entries=[entry], trade_date="2026-10-08", run_id="real-abbv-no-broker",
        prep_status="OK",
    )
    assert saved["saved_count"] == 1
    locked = repos.load_locked_us_watchlist("2026-10-08", min_count=1)[0]
    assert locked["meta"]["pullback_pass"] is True
    assert locked["meta"]["pullback_completed_close"] == proof["pullback_completed_close"]
    canonical = canonicalize_us_watchlist_row(locked)
    assert canonical["pullback_pass"] is True
    assert _validate_us_independent_new_buy_proof(canonical, "ENTRY_PULLBACK") == (True, "")

    frozen = build_us_entry_exit_contract(canonical)
    assert frozen["strategy_owner"] == "US_STANDARD"
    assert frozen["entry_provenance"]["pullback_pass"] is True

    for invalid in [
        {**canonical, "pullback_pass": False},
        {**canonical, "pullback_pass": None},
        {**canonical, "entry_signal_proof_source": "unknown"},
        {**canonical, "pullback_completed_close": 0},
    ]:
        ok, reason = _validate_us_independent_new_buy_proof(invalid, "ENTRY_PULLBACK")
        assert not ok and reason == "pullback_completed_bar_proof_missing"


def test_legacy_pb1_does_not_acquire_new_guard_when_feature_off(monkeypatch):
    monkeypatch.setenv("US_FOUR_FAMILY_FAIR_ARBITRATION_ENABLED", "0")
    assert _validate_us_independent_new_buy_proof({}, "ENTRY_PULLBACK") == (True, "")
