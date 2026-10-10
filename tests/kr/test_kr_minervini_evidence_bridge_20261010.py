"""KR Minervini evidence bridge must preserve original policy and date scope."""
from datetime import date

from trader.watchlist_builder import WatchlistBuilder


def _replay(monkeypatch, evidence, enabled="1"):
    monkeypatch.setenv("PB1_KR_MINERVINI_VCP_PROOF_ENABLED", enabled)
    builder = object.__new__(WatchlistBuilder)
    builder._load_minervini_source_map = lambda _date: {"036930": evidence}
    rows = [{
        "code": "036930",
        "entry_style_selected": "PULLBACK",
        "vcp_score": 10.0,
        "pullback_score": 50.0,
        "momentum_score": 70.0,
        "breakout_score": 60.0,
        "close": 101.0,
    }]
    return builder._merge_derived_scores(rows, date(2026, 10, 8))[0]


def _proof():
    return {
        "code": "036930",
        "as_of": "2026-10-08",
        "vcp_ok": True,
        "minervini_pass": True,
        "vcp_score": 12.0,  # Minervini detector native 0..15 scale
        "pivot": 100.0,
        "close": 101.0,
        "entry_style_selected": "PULLBACK",
    }


def test_kr_current_verified_vcp_becomes_independent_entry(monkeypatch):
    row = _replay(monkeypatch, _proof())
    assert row["entry_style_selected"] == "VCP"
    assert row["vcp_pass"] is True
    assert row["vcp_score"] == 80.0
    assert row["vcp_evidence_as_of"] == "2026-10-08"
    assert row["pivot_price"] == 100.0


def test_kr_stale_minervini_proof_never_promotes_entry(monkeypatch):
    evidence = {**_proof(), "as_of": "2026-10-07"}
    row = _replay(monkeypatch, evidence)
    assert row["entry_style_selected"] == "PULLBACK"
    assert row.get("vcp_pass") is not True


def test_kr_unproven_vcp_remains_existing_style(monkeypatch):
    evidence = {**_proof(), "minervini_pass": False}
    row = _replay(monkeypatch, evidence)
    assert row["entry_style_selected"] == "PULLBACK"


def test_kr_wrong_vcp_score_scale_fails_closed(monkeypatch):
    evidence = {**_proof(), "vcp_score": 80.0}
    row = _replay(monkeypatch, evidence)
    assert row["entry_style_selected"] == "PULLBACK"


def test_kr_opt_in_off_preserves_current_policy(monkeypatch):
    row = _replay(monkeypatch, _proof(), enabled="0")
    assert row["entry_style_selected"] == "PULLBACK"


def test_kr_vcp_without_pivot_break_is_not_entry(monkeypatch):
    evidence = {**_proof(), "pivot": 110.0}
    row = _replay(monkeypatch, evidence)
    assert row["entry_style_selected"] == "PULLBACK"
