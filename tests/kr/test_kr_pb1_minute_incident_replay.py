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
    assert [a for _, a in calls] == ["093600", "092900", "091500"]
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



def test_old_conflicted_completed_minute_gets_exact_native_anchor(
    setup_readonly, monkeypatch
):
    # At 11:05, a native conflict from 10:05 is too old to be returned by
    # the most recent 30-minute KIS window.  An exact 10:06 anchor is needed.
    def bar_1005(close):
        return {
            "date": "20261117", "time": "100500",
            "open": 10000, "high": max(close, 10010),
            "low": 9990, "close": close, "volume": 200,
        }
    earlier = native_mins(0, 30)
    proof.save_opening_proof(
        env="practice", account_scope="replay-only", code="012750",
        now=at(10, 8), raw_bars=earlier + [bar_1005(10000)],
        vwap=None, vwap_source="UNKNOWN",
    )
    proof.save_opening_proof(
        env="practice", account_scope="replay-only", code="012750",
        now=at(10, 9), raw_bars=[bar_1005(10010)],
        vwap=None, vwap_source="UNKNOWN",
    )
    assert proof.load_opening_proof(
        env="practice", account_scope="replay-only",
        code="012750", now=at(10, 9),
    )["source_conflicts"]
    now = at(11, 5)
    monkeypatch.setattr(observer, "now_kst", lambda: now)
    calls = []

    def reply(code, anchor):
        if anchor == "092900":
            return earlier
        if anchor == "100600":
            return [bar_1005(10010)]
        return native_mins(60 + 63, 60 + 65)

    result = observer.run_once(
        kis=fake_client(calls, reply), codes=["012750"],
        observed_at=now, sleep_between=0,
    )
    assert result["completed"] == 1
    assert "100600" in [a for _, a in calls], calls
    persisted = proof.load_opening_proof(
        env="practice", account_scope="replay-only",
        code="012750", now=now,
    )
    assert persisted["source_conflicts"]  # no three independent KIS responses
    # Offline KIS stub provides native-formatted rows but no response-bound
    # receipt; it may not count towards live conflict clearance.
    assert persisted["conflict_observations"]["2026-11-17T10:05:00+09:00"][
        "stable_native_reads"
    ] == 0


def test_loop_waits_for_late_prep_then_starts_readonly_collection(tmp_path, monkeypatch):
    import json
    import sys
    import trader.kis_wrapper as kw
    from trader.kr import entry_priority_queue as pq

    scope = "practice:12345678:01"
    times = iter([at(9, 0), at(9, 1), at(15, 6)])
    monkeypatch.setattr(observer, "now_kst", lambda: next(times))
    monkeypatch.setattr(sys, "argv", [
        "scripts.kr_opening_observer", "--loop", "--date", DATE,
    ])
    monkeypatch.setattr(observer, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(pq, "repo_root", lambda: tmp_path)
    monkeypatch.setattr(pq, "_current_sha", lambda: "a" * 40)
    monkeypatch.setenv("KR_PREP_COLLECTION_PRIORITY_ENABLED", "1")
    monkeypatch.setattr(observer, "get_account_key", lambda **kw: scope)
    counts = {"client_creations": 0, "runs": 0, "final30_loads": 0}
    actual_load = observer.final30_codes

    def load_final30(**kw):
        counts["final30_loads"] += 1
        return actual_load(**kw)

    monkeypatch.setattr(observer, "final30_codes", load_final30)

    class ReadOnly:
        def buy_stock(self, *a, **kw):
            raise AssertionError("observer must never place BUY")

        def sell_stock(self, *a, **kw):
            raise AssertionError("observer must never place SELL")

    def get_client(**kw):
        counts["client_creations"] += 1
        return ReadOnly()

    monkeypatch.setattr(kw, "KisAPI", get_client)
    published = []

    def sleep_then_late_prep(_):
        if published:
            return
        # No broker client or collection before verified same-day PREP.
        assert counts == {"client_creations": 0, "runs": 0, "final30_loads": 0}
        path = pq.final30_path(DATE)
        path.parent.mkdir(parents=True)
        rows = [{"code": f"{i:06d}"} for i in range(1, 30)]
        rows.append({"code": "093320", "rank_final30": 30,
                     "entry_style_selected": "MOMENTUM", "momentum_score": 98})
        path.write_text(json.dumps({"rows": rows, "trade_date": DATE,
                                    "market": "KR", "env": "practice",
                                    "as_of": "2026-11-16"}), encoding="utf-8")
        path.with_name("prep_contract.json").write_text(json.dumps({
            "trade_date": DATE, "market": "KR", "env": "practice",
            "contract_ok": True, "trade_can_proceed": True, "final30_rows": 30,
            "expected_as_of": "2026-11-16", "actual_as_of": "2026-11-16",
            "contract_hash": "late-prep-test",
        }), encoding="utf-8")
        result = pq.publish_prep_collection(date=DATE, account_scope=scope, now=at(9, 0))
        assert result["state"] == "PREP_COLLECTION_PUBLISHED"
        assert result["hot"] == ["093320"] and result["warm"] == []
        assert pq.collection_priorities(date=DATE, account_scope=scope,
                                         now=at(9, 0))["hot"] == []
        published.append(result)

    monkeypatch.setattr(observer.time, "sleep", sleep_then_late_prep)

    def collect(**kw):
        counts["runs"] += 1
        assert kw["codes"] == ["093320"]
        assert isinstance(kw["kis"], ReadOnly)
        return {
            "completed": 1, "unknown": 0, "coverage_ok": 1,
            "vwap_unverified": 0, "symbol_scan_p95_sec": 0,
            "symbol_scan_max_sec": 0, "native_minute_request_attempts": 1,
            "native_quote_request_attempts": 1, "native_request_failures": 0,
        }

    monkeypatch.setattr(observer, "run_once", collect)
    observer.main()
    assert len(published) == 1
    assert counts == {"client_creations": 1, "final30_loads": 1, "runs": 1}


def test_quote_failure_keeps_native_minute_proof_but_never_fabricates_vwap(
    setup_readonly, monkeypatch
):
    now = at(9, 36)
    monkeypatch.setattr(observer, "now_kst", lambda: now)
    class NativeOnly:
        def get_intraday_candles_today(self, code, start_hhmm, *, attempts=1):
            if start_hhmm == "092900":
                return native_mins(0, 30)
            return native_mins(30, 36)
        def get_price_snapshot(self, code, market="J"):
            raise TimeoutError("offline quote timeout")
        def buy_stock(self, *a, **kw):
            raise AssertionError("never place BUY")
    result = observer.run_once(
        kis=NativeOnly(), codes=["251970"], observed_at=now, sleep_between=0,
    )
    persisted = proof.load_opening_proof(
        env="practice", account_scope="replay-only", code="251970", now=now,
    )
    assert result["completed"] == 1
    assert result["native_request_failures"] == 1
    assert result["native_minute_request_attempts"] == 2
    assert result["native_quote_request_attempts"] == 1
    assert persisted and len(persisted["bars"]) >= 30
    assert persisted["vwap"] is None
    assert persisted["vwap_source"] == "UNKNOWN"


def test_new_native_5plus3_skips_redundant_opening_repair(setup_readonly, monkeypatch):
    now = at(9, 39)
    monkeypatch.setattr(observer, "now_kst", lambda: now)
    proof.save_opening_proof(
        env="practice", account_scope="replay-only", code="093320",
        now=at(9, 32), raw_bars=native_mins(0, 18),
        vwap=None, vwap_source="UNKNOWN")
    calls = []
    result = observer.run_once(
        kis=fake_client(calls, lambda code, anchor: native_mins(30, 39)),
        codes=["093320"], observed_at=now, sleep_between=0)
    assert calls == [("093320", "093900")]
    assert result["coverage_ok"] == 1
    assert result["native_minute_request_attempts"] == 1
    persisted = proof.load_opening_proof(env="practice", account_scope="replay-only",
                                        code="093320", now=now)
    coverage = proof.inspect_completed_bar_coverage(rows=persisted["bars"], now=now)
    assert coverage["native_post_open_window_ready"]
    assert coverage["opening_count"] == 18  # no fabricated opening bars


def test_provisional_coverage_does_not_hide_contradictory_native_rows():
    now = at(9, 39)
    older = native_mins(30, 39)
    changed = {**older[-1], "close": older[-1]["close"] + 10}
    assert observer._current_batch_coverage({"bars": older}, [changed], now) == {}


def test_cold_discovery_native_reads_are_bounded_and_missing_bars_stay_missing(
    setup_readonly, monkeypatch
):
    now = at(9, 36)
    monkeypatch.setattr(observer, "now_kst", lambda: now)
    calls = []
    def reply(code, anchor):
        if anchor == "092900":
            return native_mins(0, 18)
        assert anchor == "093600"
        return native_mins(30, 36)
    result = observer.run_once(
        kis=fake_client(calls, reply), codes=["093320"], observed_at=now,
        sleep_between=0, discovery_only=True,
        observation_styles={"093320":"PULLBACK"},
    )
    assert calls == [("093320","093600"),("093320","092900")]
    assert result["native_minute_request_attempts"] == 2
    assert result["native_quote_request_attempts"] == 1
    persisted = proof.load_opening_proof(
        env="practice", account_scope="replay-only", code="093320", now=now,
    )
    coverage = proof.inspect_completed_bar_coverage(rows=persisted["bars"],now=now)
    assert coverage["opening_count"] == 18
    assert coverage["state"] == "UNKNOWN"
    assert proof.evaluate_proof_for_candidate(
        env="practice", account_scope="replay-only", code="093320", now=now,
        style="ENTRY_PULLBACK", setup_ok=True, trigger_ok=False,
        pivot=None, order_price=10000,
    )["state"] != "READY_SHADOW"
