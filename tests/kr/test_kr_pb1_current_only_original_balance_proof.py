"""KR CURRENT_ONLY post-tick broker provenance regressions.

No real KIS HTTP, DB mutations or broker orders. A successful native forced
balance GET is attested only by KisAPI._balance_cache_at, not by a runner's
copy time. Previous owner/cycle/order guards remain outside this test.
"""
from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest

from trader.time_utils import now_kst
from trader.kr import broker_truth_hardening as truth


def _snapshot(*, marker=None, valid=True):
    item = {
        "rt_cd": "0" if valid else "1",
        "output1": [],
        "output2": {"dnca_tot_amt": "100000"},
    }
    if marker is not None:
        item["_kr_verified_fresh_at_kst"] = marker.isoformat()
    return item


class FakeKis:
    def __init__(self, original_fetch_at, snapshot=None):
        self._balance_cache_at = original_fetch_at
        self.snapshot = snapshot if snapshot is not None else _snapshot()
        self.calls = []

    def invalidate_balance_cache(self, **kwargs):
        self.calls.append(("invalidate", kwargs))

    def get_balance_cached(self, *, force=False):
        self.calls.append(("get_balance_cached", force))
        assert force is True
        return dict(self.snapshot)


def _setup(monkeypatch, kis, *, crossed, tick_snapshot):
    import trader.reconcile_kis as rk

    passed = []
    def mock_reconcile(**kwargs):
        assert kwargs["historical_recovery"] is False
        passed.append(kwargs)
        actual = kwargs["balance_snapshot"]
        proven = truth._valid_original_kr_balance_snapshot(actual)
        return {
            "ok": proven,
            "reason": "CURRENT_ONLY_FRESH_BROKER_UNVERIFIED" if not proven else "OK",
            "orders": 0, "fills": 0, "_final_holdings_rows": [],
        }
    monkeypatch.setattr(rk, "reconcile_kis", mock_reconcile)
    monkeypatch.setattr(truth, "_broker_crossed_this_tick", lambda _: crossed)
    monkeypatch.setattr(truth, "kr_tick_remaining_sec", lambda: 120.0)
    monkeypatch.setattr(
        truth, "_recover_proven_policy_positions",
        lambda **kwargs: {"recovered": [], "review_required": []},
    )
    monkeypatch.setattr(
        truth, "_health_after_reconcile",
        lambda **kwargs: {"qty_mismatch_count": 0, "stale_open_order_count": 0},
    )
    obj = SimpleNamespace(
        dry_run=False, intended_live=True, env="practice",
        engine=object(), kis=kis, run_id="mock-no-broker-order",
        _balance_snapshot=tick_snapshot, _run_summary_payload={},
        STRATEGY_NAME="pb1_pullback_close",
    )
    return obj, passed


def test_broker_crossed_force_refresh_is_attested_from_original_native_fetch_time(monkeypatch):
    original = now_kst() - timedelta(seconds=2)
    kis = FakeKis(original)
    obj, passed = _setup(monkeypatch, kis, crossed=True, tick_snapshot=None)
    truth._post_pb1_tick_reconcile(obj)
    assert len(passed) == 1
    assert passed[0]["balance_snapshot"]["_kr_verified_fresh_at_kst"] == original.isoformat()
    assert obj._run_summary_payload["broker_truth_reconcile_degraded"] == 0
    assert obj._run_summary_payload["broker_truth_balance_source"] == "fresh_kis"
    assert ("get_balance_cached", True) in kis.calls


def test_stale_engine_tick_snapshot_triggers_one_fresh_broker_lookup(monkeypatch):
    original = now_kst() - timedelta(seconds=1)
    kis = FakeKis(original)
    stale = _snapshot(marker=now_kst() - timedelta(minutes=5))
    obj, passed = _setup(monkeypatch, kis, crossed=False, tick_snapshot=stale)
    truth._post_pb1_tick_reconcile(obj)
    assert len(passed) == 1
    assert passed[0]["balance_snapshot"]["_kr_verified_fresh_at_kst"] == original.isoformat()
    assert [x for x in kis.calls if x[0] == "get_balance_cached"] == [
        ("get_balance_cached", True)
    ]
    assert obj._run_summary_payload["broker_truth_reconcile_degraded"] == 0


def test_fresh_engine_snapshot_is_reused_without_extra_kis_calls(monkeypatch):
    original = now_kst() - timedelta(seconds=2)
    kis = FakeKis(original)
    fresh = _snapshot(marker=original)
    obj, passed = _setup(monkeypatch, kis, crossed=False, tick_snapshot=fresh)
    truth._post_pb1_tick_reconcile(obj)
    assert len(passed) == 1
    assert passed[0]["balance_snapshot"] is fresh
    assert kis.calls == []
    assert obj._run_summary_payload["broker_truth_balance_source"] == "engine_tick_snapshot"


@pytest.mark.parametrize("invalid", [
    "missing_time", "stale_time", "future_time", "bad_native_status",
    "wrong_balance_shape",
])
def test_unproven_forced_balance_never_becomes_fresh_for_reconcile(monkeypatch, invalid):
    if invalid == "missing_time":
        at = None
    elif invalid == "stale_time":
        at = now_kst() - timedelta(minutes=3)
    elif invalid == "future_time":
        at = now_kst() + timedelta(minutes=3)
    else:
        at = now_kst() - timedelta(seconds=1)
    native = _snapshot(valid=invalid != "bad_native_status")
    if invalid == "wrong_balance_shape":
        native["output1"] = None
    kis = FakeKis(at, native)
    obj, passed = _setup(monkeypatch, kis, crossed=True, tick_snapshot=None)
    truth._post_pb1_tick_reconcile(obj)
    assert len(passed) == 1
    assert "_kr_verified_fresh_at_kst" not in passed[0]["balance_snapshot"]
    assert obj._run_summary_payload["broker_truth_reconcile_degraded"] == 1
    assert obj._run_summary_payload["broker_truth_health_status"] == "RED"


def test_real_kr_runner_forces_stale_wrapper_cache_once(monkeypatch):
    from trader import pb1_runner as runner

    class RealKisEmulator:
        def __init__(self):
            self._balance_cache_at = now_kst() - timedelta(minutes=4)
            self.calls = []

        def get_balance_cached(self, *, force, return_source, return_raw):
            assert return_source and return_raw
            self.calls.append(force)
            if force:
                self._balance_cache_at = now_kst() - timedelta(seconds=1)
                source = "api"
            else:
                source = "wrapper_cache"
            row = _snapshot()
            return row, source, dict(row)

    monkeypatch.setattr(runner, "KisAPI", RealKisEmulator)
    kis = RealKisEmulator()
    state, snapshot, source = runner.get_balance_state(kis=kis, now=now_kst())
    assert state == runner.BALANCE_STATE_OK
    assert kis.calls == [False, True]
    assert source == "api"
    assert snapshot["_kr_verified_fresh_at_kst"] == kis._balance_cache_at.isoformat()


def test_real_kr_runner_does_not_certify_invalid_refreshed_timestamp(monkeypatch):
    from trader import pb1_runner as runner

    class RealKisEmulator:
        def __init__(self):
            self._balance_cache_at = now_kst() - timedelta(minutes=4)
            self.calls = []

        def get_balance_cached(self, *, force, return_source, return_raw):
            self.calls.append(force)
            # Even when KIS returns a success-looking dict, failure to prove
            # original HTTP fetch timestamp remains a hard UNKNOWN state.
            self._balance_cache_at = None if force else self._balance_cache_at
            row = _snapshot()
            return row, "api" if force else "wrapper_cache", dict(row)

    monkeypatch.setattr(runner, "KisAPI", RealKisEmulator)
    kis = RealKisEmulator()
    state, snapshot, source = runner.get_balance_state(kis=kis, now=now_kst())
    assert kis.calls == [False, True]
    assert state == runner.BALANCE_STATE_UNKNOWN
    assert snapshot is None
    assert source is None
