from __future__ import annotations

from types import MethodType, SimpleNamespace
from datetime import datetime
from zoneinfo import ZoneInfo

from trader.pb1_engine import CandidateFeature, PB1Engine


def _candidate(code="005930", price=101.0, ma20=100.0, rs=80.0, score=80.0):
    return CandidateFeature(
        code=code,
        market="KOSPI",
        features={
            "data_ok": True,
            "current_price": price,
            "ma20": ma20,
            "rs_percentile": rs,
            "score_final": score,
            "atr_pct": 0.03,
            "value20": 10_000_000_000,
        },
        setup_ok=False,
        reasons=["close_below_ma20"],
        mode=1,
        mode_reasons=[],
    )


def _fake_engine(monkeypatch, env="practice", price=101.0):
    monkeypatch.setenv("STRATEGY_ENV", env)
    fake = SimpleNamespace(
        env=env,
        phase="pm_entry",
        final30_locked=True,
        _precomputed_final30_map={"005930": {}},
        _relax_bridge_summary={},
        _now_kst=datetime(2026, 6, 9, 13, 41, tzinfo=ZoneInfo("Asia/Seoul")),
        _to_float=PB1Engine._to_float,
    )
    fake._get_current_price_for_entry = lambda code, features=None: price
    fake._relax_bridge_enabled = MethodType(PB1Engine._relax_bridge_enabled, fake)
    return fake


def test_practice_minervini_bridge_activates(monkeypatch):
    monkeypatch.delenv("PB1_ENABLE_RELAX_BRIDGE", raising=False)
    fake = _fake_engine(monkeypatch, env="practice")
    candidates = [_candidate()]
    selected = PB1Engine._activate_relax_bridge_candidates(
        fake,
        candidates,
        setup_ok_count=0,
        scanner_passed_codes=set(),
        minervini_passed_codes={"005930"},
        order_allowed=True,
    )
    assert len(selected) == 1
    assert selected[0].setup_ok is True
    assert "RELAX_BRIDGE" in selected[0].features["quality_flags"]


def test_real_requires_explicit_opt_in(monkeypatch):
    monkeypatch.setenv("PB1_ENABLE_RELAX_BRIDGE", "1")
    monkeypatch.delenv("PB1_REAL_ALLOW_RELAX_BRIDGE", raising=False)
    fake = _fake_engine(monkeypatch, env="real")
    selected = PB1Engine._activate_relax_bridge_candidates(fake, [_candidate()], setup_ok_count=0, scanner_passed_codes=set(), minervini_passed_codes={"005930"}, order_allowed=True)
    assert selected == []
    assert fake._relax_bridge_summary["reason"] == "real_requires_explicit_opt_in"


def test_current_price_missing_excludes_candidate(monkeypatch):
    monkeypatch.setenv("PB1_ENABLE_RELAX_BRIDGE", "1")
    fake = _fake_engine(monkeypatch, price=None)
    selected = PB1Engine._activate_relax_bridge_candidates(fake, [_candidate()], setup_ok_count=0, scanner_passed_codes=set(), minervini_passed_codes={"005930"}, order_allowed=True)
    assert selected == []


def test_ma20_reclaim_candidate_included(monkeypatch):
    monkeypatch.setenv("PB1_ENABLE_RELAX_BRIDGE", "1")
    fake = _fake_engine(monkeypatch, price=100.2)
    selected = PB1Engine._activate_relax_bridge_candidates(fake, [_candidate(price=100.2, ma20=100.0)], setup_ok_count=0, scanner_passed_codes={"005930"}, minervini_passed_codes=set(), order_allowed=True)
    assert [c.code for c in selected] == ["005930"]


def test_bridge_not_order_candidate_before_gates(monkeypatch):
    monkeypatch.setenv("PB1_ENABLE_RELAX_BRIDGE", "1")
    fake = _fake_engine(monkeypatch)
    selected = PB1Engine._activate_relax_bridge_candidates(fake, [_candidate()], setup_ok_count=0, scanner_passed_codes=set(), minervini_passed_codes={"005930"}, order_allowed=True)
    assert selected[0].planned_qty == 0
    assert selected[0].planned_value == 0


def test_max_candidates_limit(monkeypatch):
    monkeypatch.setenv("PB1_ENABLE_RELAX_BRIDGE", "1")
    monkeypatch.setenv("PB1_RELAX_BRIDGE_MAX_CANDIDATES", "3")
    fake = _fake_engine(monkeypatch)
    candidates = [_candidate(f"{i:06d}") for i in range(1, 6)]
    codes = {c.code for c in candidates}
    fake._precomputed_final30_map = {code: {} for code in codes}
    selected = PB1Engine._activate_relax_bridge_candidates(fake, candidates, setup_ok_count=0, scanner_passed_codes=codes, minervini_passed_codes=set(), order_allowed=True)
    assert len(selected) == 3
