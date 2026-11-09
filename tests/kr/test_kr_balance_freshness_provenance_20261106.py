"""Regression: original KIS balance age, not the runner's read time, gates SELL proof."""

from datetime import timedelta

import pytest

from trader.pb1_runner import BALANCE_STATE_OK, get_balance_state
from trader.kr.protective_sell_handoff import _fresh_balance_evidence
from trader.time_utils import now_kst


class FakeKis:
    def __init__(self, source, fetched_at):
        self.source = source
        self._balance_cache_at = fetched_at

    def get_balance_cached(self, *, force, return_source, return_raw):
        assert force is False and return_source and return_raw
        snapshot = {"output1": [], "output2": {"dnca_tot_amt": "100000"}}
        return snapshot, self.source, snapshot


@pytest.mark.parametrize("source", ["api", "wrapper_cache"])
def test_verified_balance_timestamp_enables_existing_protective_sell_proof(source):
    fetched_at = now_kst() - timedelta(seconds=2)
    state, snapshot, actual_source = get_balance_state(kis=FakeKis(source, fetched_at), now=now_kst())
    assert state == BALANCE_STATE_OK
    assert actual_source == source
    assert snapshot["_kr_verified_fresh_at_kst"] == fetched_at.isoformat()
    assert _fresh_balance_evidence(snapshot) == (True, "FRESH_KIS_BALANCE")


@pytest.mark.parametrize(
    "source,age_seconds",
    [
        ("wrapper_cache", 45),
        ("api", 45),
        ("api", -45),
        ("synthetic_fixture", 0),
    ],
)
def test_unknown_stale_and_future_balance_never_get_fresh_timestamp(source, age_seconds):
    state, snapshot, _ = get_balance_state(
        kis=FakeKis(source, now_kst() - timedelta(seconds=age_seconds)),
        now=now_kst(),
    )
    assert state == BALANCE_STATE_OK
    assert "_kr_verified_fresh_at_kst" not in snapshot
    assert _fresh_balance_evidence(snapshot) == (False, "KIS_BALANCE_FRESHNESS_NOT_PROVEN")


@pytest.mark.parametrize("fetched_at", [None, now_kst().replace(tzinfo=None)])
def test_missing_or_unzoned_kis_fetch_time_fails_closed(fetched_at):
    state, snapshot, _ = get_balance_state(
        kis=FakeKis("api", fetched_at),
        now=now_kst(),
    )
    assert state == BALANCE_STATE_OK
    assert "_kr_verified_fresh_at_kst" not in snapshot
    assert _fresh_balance_evidence(snapshot) == (False, "KIS_BALANCE_FRESHNESS_NOT_PROVEN")


def test_inherited_fresh_marker_is_not_trusted_without_original_kis_proof():
    class MarkedSnapshot(FakeKis):
        def get_balance_cached(self, *, force, return_source, return_raw):
            snapshot, source, raw = super().get_balance_cached(
                force=force, return_source=return_source, return_raw=return_raw,
            )
            snapshot["_kr_verified_fresh_at_kst"] = now_kst().isoformat()
            return snapshot, source, snapshot

    state, snapshot, _ = get_balance_state(
        kis=MarkedSnapshot("wrapper_cache", now_kst() - timedelta(minutes=10)),
        now=now_kst(),
    )
    assert state == BALANCE_STATE_OK
    assert "_kr_verified_fresh_at_kst" not in snapshot
    assert _fresh_balance_evidence(snapshot) == (False, "KIS_BALANCE_FRESHNESS_NOT_PROVEN")
