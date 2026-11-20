"""US_STANDARD 2026-11-20 executable BUY price versus raw SST timing cap.

Offline only: no KIS network, DB writes, synthetic fills or order submission.
"""
from __future__ import annotations

import hashlib
import json

import pytest

from trader.us.entry_timing.opportunity import (
    attach_opportunity_proof,
    pre_submit_timing_check,
    select_us_executable_buy_price,
)


@pytest.mark.parametrize("cap,expected,ask,stop,target", [
    ("96.5164", 96.51, "96.51", "95.00", "99.00"),    # CL
    ("1088.0169", 1088.01, "1088.01", "1070", "1150"),  # COST
    ("117.7276", 117.72, "117.72", "115", "123"),      # ED
])
def test_original_four_decimal_ceiling_uses_native_cent_floor(
    cap, expected, ask, stop, target,
):
    price, reason = select_us_executable_buy_price(
        requested_limit=cap, raw_cap=cap, fresh_ask=ask,
        stop_reference=stop, target_reference=target,
        min_rr=1.5, max_stop_distance=0.025,
    )
    assert (price, reason) == (expected, "EXECUTABLE_PRICE_VALID")
    assert price <= float(cap)


@pytest.mark.parametrize("cap,ask,stop,target", [
    ("96.5164", "96.52", "95", "99"),
    ("1088.0169", "1088.02", "1070", "1150"),
    ("117.7276", "117.73", "115", "123"),
])
def test_above_native_cap_ask_is_normal_wait(cap, ask, stop, target):
    assert select_us_executable_buy_price(
        requested_limit=cap, raw_cap=cap, fresh_ask=ask,
        stop_reference=stop, target_reference=target,
        min_rr=1.5, max_stop_distance=0.025,
    ) == (None, "ASK_ABOVE_EXECUTABLE_LIMIT")


def test_requested_limit_lower_than_ask_must_not_raise_limit():
    assert select_us_executable_buy_price(
        requested_limit="96.50", raw_cap="96.5164", fresh_ask="96.51",
        stop_reference="95", target_reference="99",
        min_rr="1.5", max_stop_distance="0.025",
    ) == (None, "ASK_ABOVE_EXECUTABLE_LIMIT")


@pytest.mark.parametrize("value", ["nan", "Infinity", "-1", None, ""])
def test_invalid_price_inputs_fail_closed(value):
    assert select_us_executable_buy_price(
        requested_limit=value, raw_cap="96.5164", fresh_ask="96.51",
        stop_reference="95", target_reference="99",
        min_rr=1.5, max_stop_distance=.025,
    )[0] is None


def _signed_intent(limit):
    decision = {
        "state": "READY", "decision_price": 96.51,
        "max_entry_price": 96.5164,
        "structural_stop_reference": 95.0,
        "structural_target_reference": 99.0,
    }
    decision["evidence_sha256"] = hashlib.sha256(json.dumps(
        decision, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode()).hexdigest()
    intent = {
        "symbol": "CL", "side": "BUY", "strategy_owner": "US_STANDARD",
        "position_action": "NEW_POSITION_BUY",
        "limit_price": limit, "meta": {},
    }
    attach_opportunity_proof(intent, decision, mode="ENFORCE", decision_epoch=1000.0)
    return intent


def test_http_check_accepts_native_limit_and_requires_ask_below_actual_limit():
    intent = _signed_intent(96.51)
    fresh = {"received_at": 1002, "last": 96.51,
             "bid": 96.50, "ask": 96.51}
    assert pre_submit_timing_check(
        intent, now_epoch=1003, fresh_ws_quote=fresh,
    ) == (True, "TIMING_FRESH_PRICE_CONFIRMED")
    # Ask 96.515 is still UNDER signed cap 96.5164, but ABOVE actual order
    # limit 96.51. This must not be mistaken for a marketable BUY.
    assert pre_submit_timing_check(
        intent, now_epoch=1003,
        fresh_ws_quote={**fresh, "ask": 96.515},
    )[1] == "TIMING_BROKER_PRICE_MOVED"


@pytest.mark.parametrize("non_native_limit", [96.5164, 96.52, 96.515])
def test_http_check_cannot_reprice_signed_limit(non_native_limit):
    intent = _signed_intent(non_native_limit)
    assert pre_submit_timing_check(
        intent, now_epoch=1003,
        fresh_ws_quote={"received_at": 1002, "last": 96.51,
                        "bid": 96.50, "ask": 96.51},
    )[1] == "TIMING_PRICE_CAP_VIOLATED"


def test_sell_and_infinite_sleeve_unmodified():
    for intent in [
        {"side": "SELL", "symbol": "CL", "strategy_owner": "US_STANDARD"},
        {"side": "BUY", "symbol": "TQQQ", "strategy_owner": "TQQQ_INFINITE",
         "position_action": "NEW_POSITION_BUY"},
    ]:
        assert pre_submit_timing_check(
            intent, now_epoch=1003, fresh_ws_quote=None,
        ) == (True, "NOT_APPLICABLE")
