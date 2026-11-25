"""Reconstructed 11/25 DB evidence; not native KIS replay or live proof."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from trader.kr.entry_opportunity_v2 import evaluate_reclaim
from trader.kr.entry_opportunity_v3 import candidate_geometry
from trader.kr import opening_bar_contract as contract

KST = ZoneInfo("Asia/Seoul")
DAY = datetime(2026, 11, 25, 9, 0, tzinfo=KST)


def bar(minute, close, low, volume, high=None):
    return dict(timestamp=(DAY+timedelta(minutes=minute)).isoformat(),
                close=close, low=low, volume=volume,
                high=close if high is None else high)


def opening():
    # Synthetic opening bars consistent with logged 79,000 high.
    return [bar(i, 78000, 77900, 100, 79000 if i == 0 else 78000)
            for i in range(30)]


def test_pumtech_rise_hold_0940():
    post = [bar(36, 76100, 76000, 200),
            bar(37, 76300, 76000, 207),
            bar(38, 76400, 76300, 250),
            bar(39, 76400, 76400, 252)]
    now = DAY+timedelta(minutes=40, seconds=57)
    old = evaluate_reclaim(family="MOMENTUM", opening=opening(), post=post,
                           now=now, order_price=76500, independent=False)
    assert old["state"] == "WAIT"
    new = candidate_geometry(family="MOMENTUM", opening=opening(), post=post,
                             now=now, original_setup_ok=True)
    assert new["state"] == "SIGNAL"
    assert new["pattern"] == "RISE_AND_HOLD"
    assert new["price_floor"] <= 76500 <= new["price_ceiling"]
    assert new["not_order_authorization"] is True


def test_pumtech_0944_expensive_ask_still_wait():
    post = [bar(40, 76600, 76300, 180),
            bar(41, 76300, 76300, 193),
            bar(42, 76700, 76400, 370),
            bar(43, 77200, 76400, 375)]
    now = DAY+timedelta(minutes=44, seconds=56)
    old = evaluate_reclaim(family="MOMENTUM", opening=opening(), post=post,
                           now=now, order_price=77400, independent=False)
    assert old["state"] == "WAIT"
    assert old["reason"] == "INSUFFICIENT_ROOM_VS_STOP"
    assert old["diagnostic_entry_ceiling"] == 77300
    assert old.get("max_entry_price") is None
    new = candidate_geometry(family="MOMENTUM", opening=opening(), post=post,
                             now=now, original_setup_ok=True)
    assert new["price_ceiling"] == 77300
    assert new["not_order_authorization"] is True


def test_later_confirmed_local_peak_not_future_high():
    post = [bar(60, 79700, 79600, 100, 79800),
            bar(61, 80000, 79800, 100, 80100),
            bar(62, 79500, 79300, 100, 79600),
            bar(63, 79200, 79100, 100, 79500),
            bar(64, 79300, 79100, 100),
            bar(65, 79400, 79200, 100),
            bar(66, 79500, 79300, 120, 80500)]
    new = candidate_geometry(family="PULLBACK", opening=opening(), post=post,
                             now=DAY+timedelta(minutes=67, seconds=20),
                             original_setup_ok=True)
    assert new["state"] == "SIGNAL"
    assert new["reference_source"] == "CONFIRMED_LOCAL_PEAK"
    assert new["target"] == 80100
    assert new["not_order_authorization"]


@pytest.mark.parametrize("mode", ["missing", "conflict"])
def test_v2_error_policy_identity(monkeypatch, mode):
    monkeypatch.setenv("KR_PB1_ENTRY_OPPORTUNITY_POLICY", "US_ALIGNED_V2")
    monkeypatch.setattr(contract, "load_opening_proof",
                        lambda **_: None if mode == "missing" else {"bars": []})
    if mode == "conflict":
        monkeypatch.setattr(contract, "_evaluate_us_aligned_reclaim",
                            lambda **_: {"state": "UNKNOWN", "reason": "CONFLICTING_NATIVE_COMPLETED_BAR"})
    proof = contract.evaluate_proof_for_candidate(
        env="practice", account_scope="test", code="251970",
        now=DAY+timedelta(minutes=45), style="ENTRY_MOMENTUM",
        setup_ok=True, trigger_ok=False, pivot=None, order_price=77400)
    assert proof["state"] == "UNKNOWN"
    assert proof["entry_policy"] == "US_ALIGNED_V2"


def test_v3_no_owner_bypass():
    assert candidate_geometry(family="BREAKOUT", opening=[], post=[], now=DAY,
                              original_setup_ok=True)["reason"] == "OWNER_PATH_REQUIRED"
