"""Combined KR/US real Supabase OHLCV -> independent signal -> safe BUY intent.

Archived bars are prior-session only. No Supabase writes, no broker submission.
"""
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from trader.kr_four_family_candidate_admission import completed_daily_candidate_proofs, merge_verified_candidate_screens
from trader.us.candidate_pool_builder import _compute_us_explicit_signal_proofs, _score_symbol_candidate
from trader.us.four_family_arbitration import eligible_family_scores, rank_verified_families
from trader.us.watchlist_builder import _compute_pb1_score, _compute_momentum_score, _compute_breakout_score, _compute_vcp_score

_ROOT = Path(__file__).parents[1]
KR = json.loads((_ROOT / "kr/fixtures/supabase_ohlcv_proof_replay_20261011.json").read_text())
US = json.loads((_ROOT / "us/fixtures/supabase_ohlcv_proof_replay_20261011.json").read_text())


def _us_candidate(label):
    sample = US["samples"][label]
    bars = sample["bars"]
    daily = [{"xymd": r[0], "clos": r[1], "high": r[2], "low": r[3], "tvol": r[4]} for r in bars]
    assert bars[-1][0] < sample["trade_date"].replace("-", "")
    row = _score_symbol_candidate({"symbol": sample["symbol"], "price": bars[-1][1], "atr_pct": .04}, daily, [], [], [])
    row.update(_compute_us_explicit_signal_proofs(sample["symbol"], daily))
    row.update({
        "pb1_score": _compute_pb1_score(row, daily),
        "momentum_score": _compute_momentum_score(row),
        "breakout_score": _compute_breakout_score(row),
        "vcp_score": _compute_vcp_score(row),
        "score_final": .8,
        "reason_json": {},
    })
    return row


def test_kr_real_breakouts_survive_top50_without_fake_vcp():
    legacy = [{"code": str(400000 + i), "score": 90-i, "candidate_family_screens": {}} for i in range(50)]
    proofs = []
    for code in ("000150", "000250", "006120", "096530", "277810"):
        bars = KR["samples"][code]["bars"]
        assert bars[-1][0] < KR["test_trade_date"].replace("-", "")
        df = pd.DataFrame(bars, columns=["date", "close", "high", "low", "volume"])
        signals = completed_daily_candidate_proofs(df)
        assert signals["BREAKOUT"] is True
        assert signals["VCP"] is False
        proofs.append({"code": code, "score": 0, "candidate_family_screens": signals})
    ranked, report = merge_verified_candidate_screens(legacy, legacy+proofs, target_size=50)
    assert len(ranked) == 50
    assert {r["code"] for r in proofs} <= {r["code"] for r in ranked}
    assert report["eligible_by_family"]["BREAKOUT"] == 5


def test_us_real_signal_qualifications_survive_raw_pb1_score_bias():
    rows = [_us_candidate(label) for label in US["samples"]]
    ranked, report = rank_verified_families(rows)
    assert report["qualified"] == 5
    assert any(r["entry_style_selected"] == "breakout" for r in ranked)
    assert all(r["entry_style_selected"] in eligible_family_scores(r) for r in ranked)
    assert all(r["reason_json"]["entry_style_selected"] == r["entry_style_selected"] for r in ranked)


def test_real_us_ohlcv_breakout_proof_reaches_signal_only_locked_buy(monkeypatch):
    from trader.us.db import repos
    from trader.us.score_columns import canonicalize_us_watchlist_row, validate_us_entry_provenance_contract
    from trader.us.pb1.us_entry_engine import generate_entry_intents
    from trader.us.execution.order_router import route_order

    for key, value in {
        "US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED": "1",
        "US_MIN_ENTRY_SCORE": "0.01",
        "US_MAX_NEW_ENTRIES_PER_TICK": "1",
        "US_MAX_ORDER_USD": "5000",
        "US_MAX_POSITION_WEIGHT": "1",
        "US_MIN_CASH_BUFFER_USD": "0",
        "US_MAX_DAILY_NOTIONAL_USD": "50000",
        "KIS_ENV": "practice",
        "DRY_RUN": "1",
        "US_KIS_ORDER_ALLOWED": "0",
    }.items():
        monkeypatch.setenv(key, value)

    row = _us_candidate("2026-10-08:CIEN")
    assert row["breakout_pass"] is True
    row["entry_style_selected"] = "breakout"
    row["entry_style_raw"] = "breakout"
    row["strategy"] = "breakout"
    row["score"] = .9
    row["score_final"] = .9
    row["reasons"] = ["ENTRY_BREAKOUT"]
    row["filters_passed"] = ["score", "liquidity"]
    row["score_breakdown"] = {"breakout": row["breakout_score"]}
    row["rank_final30"] = 1
    row["data_source"] = "completed_daily"
    repos.reset_memory_stores()
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    saved = repos.clear_and_save_locked_us_watchlist(
        entries=[row], trade_date="2026-10-08", run_id="real-bars-safe-replay", prep_status="OK"
    )
    assert saved["saved_count"] == 1
    db_row = repos.load_locked_us_watchlist("2026-10-08", min_count=1)[0]
    assert validate_us_entry_provenance_contract([db_row])["ok"] is True
    canonical = canonicalize_us_watchlist_row(db_row)

    class Provider:
        def get_current_price(self, symbol, exchange):
            return {"last": 100.}

    diagnostics = {}
    intents = generate_entry_intents(
        tickers=None, provider=Provider(), sold_today=set(), available_cash_usd=10000.,
        position_count=0, capital_usd_cap=10000.,
        now=datetime(2026, 10, 8, 10, 5, tzinfo=ZoneInfo("America/New_York")),
        max_new_entries=1, watchlist_entries=[canonical],
        current_position_symbols=set(), diagnostics=diagnostics,
    )
    assert len(intents) == 1, diagnostics
    intent = intents[0]
    assert intent["entry_style_selected"] == "ENTRY_BREAKOUT"
    assert intent["entry_signal_type"] == "breakout"
    routed = route_order(intent, signal_only=True, current_position_symbols=set(), allowed_symbols={"CIEN"})
    assert routed["status"] == "SIGNAL_ONLY"
    contract = routed["intent"]["meta"]["entry_exit_contract"]
    assert contract["strategy_owner"] == "US_STANDARD"
    assert contract["entry_provenance"]["entry_style_selected"] == "ENTRY_BREAKOUT"
