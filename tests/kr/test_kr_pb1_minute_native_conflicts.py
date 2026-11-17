"""Reproduction of quarantine/revalidation for native KR completed 1m bars."""
from __future__ import annotations

from datetime import datetime, timedelta

from trader.kr import opening_bar_contract as ob


def _at(hour: int, minute: int):
    return datetime(2026, 11, 17, hour, minute, tzinfo=ob.KST)


def _bar(close: int):
    return {
        "date": "2026-11-17", "time": "091200",
        "open": 10000, "high": max(10010, close), "low": 9990,
        "close": close, "volume": 1000,
    }


def _save(t, raw):
    return ob.save_opening_proof(
        env="practice", account_scope="scope", code="012750", now=t,
        raw_bars=raw, vwap=None, vwap_source="UNKNOWN",
    )


def _load(t):
    return ob.load_opening_proof(
        env="practice", account_scope="scope", code="012750", now=t,
        max_age_seconds=None,
    )


def test_native_completed_bar_conflict_requires_three_spaced_revalidations(tmp_path, monkeypatch):
    monkeypatch.setattr(ob, "_evidence_file",
                        lambda env, account_scope, code, now: tmp_path / "proof.json")
    _save(_at(10, 0), [_bar(10000)])
    t1 = _at(10, 1)
    first = _save(t1, [_bar(10010)])
    assert len(first["source_conflicts"]) == 1
    assert _load(t1)["bars"][0]["close"] == 10000
    assert ob.evaluate_proof_for_candidate(
        env="practice", account_scope="scope", code="012750", now=t1,
        style="ENTRY_MOMENTUM", setup_ok=True, trigger_ok=True,
        pivot=None, order_price=10000,
    )["reason"] == "CONFLICTING_NATIVE_COMPLETED_BAR"
    again = _save(_at(10, 2), [_bar(10010)])
    assert again["source_conflicts"]
    accepted = _save(_at(10, 3), [_bar(10010)])
    assert accepted["source_conflicts"] == []
    proof = _load(_at(10, 3))
    assert proof["bars"][0]["close"] == 10010
    assert proof["resolved_source_conflicts"][-1]["confirmed_native_reads"] == 3
    assert proof["resolved_source_conflicts"][-1]["minute"].endswith("+09:00")


def test_conflict_is_sticky_across_readback_and_single_pass_disagreement(tmp_path, monkeypatch):
    monkeypatch.setattr(ob, "_evidence_file",
                        lambda env, account_scope, code, now: tmp_path / "proof.json")
    first = _save(_at(10, 0), [_bar(10000), _bar(10010)])
    assert first["source_conflicts"] and first["intra_pass_conflicts"]
    stored = _load(_at(10, 0))
    assert stored["source_conflicts"]
    assert stored["conflict_observations"]
    # Repeated reads within the same instant cannot independently resolve it.
    for _ in range(4):
        state = _save(_at(10, 0), [_bar(10010)])
    assert state["source_conflicts"]
