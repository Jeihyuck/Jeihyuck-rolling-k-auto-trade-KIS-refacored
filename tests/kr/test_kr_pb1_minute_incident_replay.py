"""Offline native 1m observer incident replays. No KIS credentials or BUY IO."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from scripts import kr_opening_observer as observer
from trader.kr import opening_bar_contract as proof


DATE = "2026-11-17"


def at(hour, minute):
    return datetime(2026, 11, 17, hour, minute, tzinfo=proof.KST)


def native_mins(start, end, *, base=10000):
    rows = []
    for minute in range(start, end):
        timestamp = at(9 + minute // 60, minute % 60)
        px = base + (minute % 4) * 10
        rows.append({
            "date": "20261117",
            "time": timestamp.strftime("%H%M%S"),
            "open": px, "high": px + 10, "low": px - 10,
            "close": px, "volume": 200,
        })
    return rows


@pytest.fixture
def setup_readonly(tmp_path, monkeypatch):
    monkeypatch.setattr(
        proof, "_evidence_file",
        lambda env, account_scope, code, now:
            tmp_path / (str(code).zfill(6) + ".json"),
    )
    monkeypatch.setattr(observer, "get_account_key", lambda **kw: "replay-only")
    observer._OPENING_REPAIR_NEXT.clear()
    return tmp_path


def fake_client(call_log, reply):
    class Kis:
        def get_intraday_candles_today(self, code, start_hhmm, *, attempts=1):
            call_log.append((code, start_hhmm))
            return reply(code, start_hhmm)

        def get_price_snapshot(self, code, market="J"):
            return {}  # non-native VWAP must never be manufactured

        def buy_stock(self, *a, **kw):
            raise AssertionError("observer must never place orders")

        def sell_stock(self, *a, **kw):
            raise AssertionError("observer must never place orders")
    return Kis()


def test_sparse_opening_native_18_bars_repaired_only_from_genuine_kis(setup_readonly, monkeypatch):
    now = at(9, 36)
    monkeypatch.setattr(observer, "now_kst", lambda: now)
    prior = native_mins(0, 18)
    proof.save_opening_proof(
        env="practice", account_scope="replay-only",
        code="093320", now=at(9, 32),
        raw_bars=prior, vwap=None, vwap_source="UNKNOWN",
    )
    calls = []

    def reply(code, anchor):
        if anchor == "092900":
            return native_mins(0, 18)
        if anchor == "091500":
            return native_mins(18, 30)
        return native_mins(30, 36)

    result = observer.run_once(
        kis=fake_client(calls, reply), codes=["093320"],
        observed_at=now, sleep_between=0,
    )
    persisted = proof.load_opening_proof(
        env="practice", account_scope="replay-only", code="093320", now=now,
    )
    summary = proof.inspect_completed_bar_coverage(rows=persisted["bars"], now=now)
    assert result["completed"] == 1
    assert [a for _, a in calls] == ["092900", "093600", "091500"]
    assert summary["opening_count"] == 30
    assert summary["state"] == "COVERED"
    assert not persisted["source_conflicts"]


def test_stale_native_last_completed_minute_recovers_without_full_opening_replay(
    setup_readonly, monkeypatch
):
    now = at(10, 3)
    monkeypatch.setattr(observer, "now_kst", lambda: now)
    proof.save_opening_proof(
        env="practice", account_scope="replay-only",
        code="251970", now=at(9, 34),
        raw_bars=native_mins(0, 34), vwap=None,
        vwap_source="UNKNOWN",
    )
    calls = []

    def reply(code, anchor):
        assert anchor == "100300"
        return native_mins(60, 63)

    result = observer.run_once(
        kis=fake_client(calls, reply), codes=["251970"],
        observed_at=now, sleep_between=0,
    )
    persisted = proof.load_opening_proof(
        env="practice", account_scope="replay-only",
        code="251970", now=now,
    )
    summary = proof.inspect_completed_bar_coverage(rows=persisted["bars"], now=now)
    assert result["completed"] == 1
    assert calls == [("251970", "100300")]
    assert summary["state"] == "COVERED"
    assert summary["latest_bar_age_sec"] <= 180


def test_missing_native_minutes_do_not_turn_into_synthetic_proof(setup_readonly, monkeypatch):
    now = at(9, 36)
    monkeypatch.setattr(observer, "now_kst", lambda: now)
    calls = []

    def reply(code, anchor):
        return native_mins(0, 18) if anchor != "093600" else native_mins(30, 36)

    result = observer.run_once(
        kis=fake_client(calls, reply), codes=["093320"],
        observed_at=now, sleep_between=0,
    )
    persisted = proof.load_opening_proof(
        env="practice", account_scope="replay-only",
        code="093320", now=now,
    )
    summary = proof.inspect_completed_bar_coverage(rows=persisted["bars"], now=now)
    assert result["completed"] == 1
    assert summary["opening_count"] == 18
    assert summary["state"] == "UNKNOWN"
    assert summary["reason"] == "OPENING_BARS_MISSING_OR_INCOMPLETE"
    assert proof.evaluate_proof_for_candidate(
        env="practice", account_scope="replay-only",
        code="093320", now=now, style="ENTRY_PULLBACK",
        setup_ok=True, trigger_ok=False, pivot=None, order_price=10000,
    )["state"] != "READY_SHADOW"
