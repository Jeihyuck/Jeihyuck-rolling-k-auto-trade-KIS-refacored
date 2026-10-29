"""Price-aware US opening entry policy: no REST/network/order side effects."""
from __future__ import annotations

import copy
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from trader.us.entry_timing.opportunity import (
    attach_opportunity_proof, effective_mode, evaluate_price_opportunity,
    normalize_bars, opening_coverage, pre_submit_timing_check,
)

NY = ZoneInfo("America/New_York")
DAY = "2026-10-28"


def sample_native_rows(count: int = 34) -> list[dict]:
    start = datetime(2026, 10, 28, 9, 30, tzinfo=NY)
    result = []
    for i in range(count):
        at = start + timedelta(minutes=i)
        if i < 30:
            close = round(100 - 0.18 * i, 4)
        else:
            close = round(95.1 + 0.3 * (i - 30), 4)
        result.append({
            "xymd": "20261028", "xhms": at.strftime("%H%M%S"),
            "open": str(close - 0.1), "high": str(close + 0.15),
            "low": str(close - 0.25), "last": str(close),
            "evol": "200", "eamt": str(close * 200),
        })
    return result


def sample_bars():
    return normalize_bars(sample_native_rows(), trade_date=DAY,
                          now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY))


def sample_quote(last: float = 96.05, bid: float = 96.0, ask: float = 96.05):
    return {"last": last, "bid": bid, "ask": ask,
            "age_sec": 1.0, "source": "KIS_WEBSOCKET_COLLECTOR"}


def test_native_bars_are_complete_correct_day_and_have_full_opening_range():
    rows = sample_native_rows()
    rows.append({**rows[-1], "xhms": "100500"})
    bars = normalize_bars(rows, trade_date=DAY,
                          now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY))
    assert len(bars) == 34
    coverage = opening_coverage(bars, DAY)
    assert coverage["observed_1m"] == 30
    assert coverage["missing_1m"] == 0
    assert coverage["opening_high"] > coverage["opening_low"]


def test_sparse_quote_snapshots_never_become_minute_bars():
    fake = [{"timestamp": "09:30", "price": 100},
            {"timestamp": "09:33", "price": 98},
            {"timestamp": "09:36", "price": 99}]
    bars = normalize_bars(fake, trade_date=DAY,
                          now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY))
    assert bars == []


def test_reversal_price_room_ready_requires_true_contiguous_minute_bars():
    decision = evaluate_price_opportunity(
        style="ENTRY_PULLBACK", trade_date=DAY,
        now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY),
        verified_bars=sample_bars(), quote=sample_quote(),
    )
    assert decision["state"] == "READY", decision
    assert decision["max_entry_price"] >= sample_quote()["ask"]
    assert decision["risk_reward_reference"] >= 1.5
    assert decision["facts"]["phase"] == "READY"
    assert decision["risk_reference_is_live_stop_order"] is False
    assert decision["broker_authorized"] is False


def test_falling_knife_waits_even_with_high_daily_score():
    bars = sample_bars()
    # A falling last three completed 1m bars has not confirmed a reversal.
    for j, close in enumerate((95.8, 95.4, 95.0)):
        bar = bars[-3 + j]
        bar.update(open=close + 0.1, high=close + 0.2, low=close - 0.25,
                   close=close)
    decision = evaluate_price_opportunity(
        style="ENTRY_PULLBACK", trade_date=DAY,
        now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY),
        verified_bars=bars, quote=sample_quote(95.0, 94.95, 95.0),
        prep={"score_final": 0.99},
    )
    assert decision["state"] == "WAIT"
    assert decision["reason"] == "reversal_not_confirmed"


def test_missing_opening_minutes_and_current_bar_cannot_authorize():
    bars = sample_bars()[::3]
    decision = evaluate_price_opportunity(
        style="ENTRY_PULLBACK", trade_date=DAY,
        now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY),
        verified_bars=bars, quote=sample_quote(),
    )
    assert decision["state"] == "WAIT"
    assert decision["reason"] == "opening_minute_bars_insufficient"


def test_pre_ten_am_is_watch_even_with_favorable_prices():
    decision = evaluate_price_opportunity(
        style="ENTRY_PULLBACK", trade_date=DAY,
        now_et=datetime(2026, 10, 28, 9, 59, tzinfo=NY),
        verified_bars=sample_bars(), quote=sample_quote(),
    )
    assert decision["state"] == "WATCH"
    assert decision["reason"] == "opening_window_collecting"


@pytest.mark.parametrize("quote,reason", [
    ({"last": 96.05, "age_sec": 30, "source": "KIS_WEBSOCKET_COLLECTOR"},
     "fresh_broker_quote_missing"),
    ({"last": 96.05, "age_sec": 1, "source": "PREP_CACHE"},
     "fresh_broker_quote_missing"),
    ({"last": 96.05, "age_sec": 1, "source": "KIS_LIVE"},
     "executable_bid_ask_missing"),
    (sample_quote(96.05, 96.0, 96.5), "spread_exceeds_limit"),
])
def test_missing_data_and_wide_spread_wait(quote, reason):
    result = evaluate_price_opportunity(
        style="ENTRY_PULLBACK", trade_date=DAY,
        now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY),
        verified_bars=sample_bars(), quote=quote,
    )
    assert result["state"] == "WAIT"
    assert result["reason"] == reason


def test_breakout_pivot_and_volume_proof_are_distinct_from_pullback():
    result = evaluate_price_opportunity(
        style="ENTRY_BREAKOUT", trade_date=DAY,
        now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY),
        verified_bars=sample_bars(), quote=sample_quote(),
        prep={"breakout_pivot_price": 95.5, "timing_target_reference": 100},
    )
    assert result["state"] == "READY", result
    absent = evaluate_price_opportunity(
        style="ENTRY_BREAKOUT", trade_date=DAY,
        now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY),
        verified_bars=sample_bars(), quote=sample_quote(),
        prep={},
    )
    assert absent["reason"] == "prep_pivot_missing"


def test_missing_target_or_risk_reward_wont_approve_breakout():
    decision = evaluate_price_opportunity(
        style="ENTRY_BREAKOUT", trade_date=DAY,
        now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY),
        verified_bars=sample_bars(), quote=sample_quote(),
        prep={"breakout_pivot_price": 95.5},
    )
    assert decision["state"] == "WAIT"
    assert decision["reason"] == "unfavorable_reference_reward_risk"


def test_tqqq_and_vcp_are_not_touched_by_generic_price_policy():
    for family in ("ENTRY_VCP", "TQQQ_INFINITE"):
        result = evaluate_price_opportunity(
            style=family, trade_date=DAY,
            now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY),
            verified_bars=sample_bars(), quote=sample_quote(),
        )
        assert result["state"] == "NOT_APPLICABLE"


def test_trial_gate_is_never_unconditionally_enabled():
    assert effective_mode({"US_ENTRY_OPPORTUNITY_MODE": "SHADOW"}) == "SHADOW"
    assert effective_mode({"US_ENTRY_OPPORTUNITY_MODE": "ENFORCE",
                           "KIS_ENV": "practice"}) == "ENFORCE_NOT_APPROVED"
    assert effective_mode({"US_ENTRY_OPPORTUNITY_MODE": "ENFORCE",
                           "KIS_ENV": "real",
                           "US_ENTRY_OPPORTUNITY_APPROVED": "1"}) == "ENFORCE_NOT_APPROVED"
    assert effective_mode({"US_ENTRY_OPPORTUNITY_MODE": "ENFORCE",
                           "KIS_ENV": "practice",
                           "US_ENTRY_OPPORTUNITY_APPROVED": "1"}) == "ENFORCE"


def test_pre_submit_uses_verified_price_cap_and_fresh_quote():
    result = evaluate_price_opportunity(
        style="ENTRY_PULLBACK", trade_date=DAY,
        now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY),
        verified_bars=sample_bars(), quote=sample_quote(),
    )
    now_epoch = datetime(2026, 10, 28, 10, 5, tzinfo=NY).timestamp()
    intent = {"symbol": "ELV", "side": "BUY",
              "strategy_owner": "US_STANDARD", "position_action": "NEW_POSITION_BUY",
              "limit_price": result["max_entry_price"], "meta": {}}
    attach_opportunity_proof(intent, result, mode="ENFORCE", decision_epoch=now_epoch)
    quote = {"last": 96.05, "bid": 96.0, "ask": 96.05,
             "received_at": now_epoch + 2}
    assert pre_submit_timing_check(
        intent, now_epoch=now_epoch + 3, fresh_ws_quote=quote,
    ) == (True, "TIMING_FRESH_PRICE_CONFIRMED")
    assert pre_submit_timing_check(
        intent, now_epoch=now_epoch + 125, fresh_ws_quote=quote,
    )[1] == "TIMING_SIGNAL_EXPIRED"
    rebound_failed = {**quote, "last": 95.0, "ask": 95.0, "bid": 94.98}
    assert pre_submit_timing_check(
        intent, now_epoch=now_epoch + 3, fresh_ws_quote=rebound_failed,
    )[1] == "TIMING_REBOUND_INVALIDATED"
    expensive = copy.deepcopy(intent)
    expensive["limit_price"] = 101.0
    assert pre_submit_timing_check(
        expensive, now_epoch=now_epoch + 3, fresh_ws_quote=quote,
    )[1] == "TIMING_PRICE_CAP_VIOLATED"
    tampered = copy.deepcopy(intent)
    tampered["meta"]["us_entry_opportunity_v1"]["facts"]["recent_closes"] = [999]
    assert pre_submit_timing_check(
        tampered, now_epoch=now_epoch + 3, fresh_ws_quote=quote,
    )[1] == "TIMING_PROOF_INVALID"


def test_protective_sell_and_tqqq_buys_never_wait_for_generic_entry_quote():
    for intent in [
        {"symbol": "ELV", "side": "SELL", "position_action": "CLOSE_POSITION",
         "strategy_owner": "US_STANDARD"},
        {"symbol": "TQQQ", "side": "BUY", "position_action": "NEW_POSITION_BUY",
         "strategy_owner": "TQQQ_INFINITE"},
    ]:
        assert pre_submit_timing_check(
            intent, now_epoch=100, fresh_ws_quote=None,
        ) == (True, "NOT_APPLICABLE")

def test_offline_replay_does_not_invent_missing_executions():
    from scripts.us_entry_opportunity_replay import replay
    native = sample_native_rows()
    instant = datetime(2026, 10, 28, 10, 5, tzinfo=NY)
    payload = {
        "trade_date": DAY, "style": "ENTRY_PULLBACK",
        "native_bars": native, "original_fill_price": 99.0,
        "snapshots": [{
            "at_et": instant.isoformat(), "quote": sample_quote(),
        }],
    }
    report = replay(payload)
    assert report["first_ready"]["state"] == "READY"
    assert report["timing_effectiveness"] == "NOT_ESTABLISHED"
    assert report["baseline_comparison"]["entry_price_delta_pct"] < 0
    assert report["forward_60m"]["status"] == "UNKNOWN_MISSING_FUTURE_BARS"

def test_frozen_position_contract_retains_exact_price_opportunity_evidence():
    from trader.us.entry_exit_contract import (
        build_us_entry_exit_contract, verify_us_entry_exit_contract,
    )
    observed = evaluate_price_opportunity(
        style="ENTRY_PULLBACK", trade_date=DAY,
        now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY),
        verified_bars=sample_bars(), quote=sample_quote(),
    )
    meta = {}
    intent = {"meta": meta, "symbol": "ELV", "strategy_owner": "US_STANDARD",
              "entry_style_selected": "ENTRY_PULLBACK"}
    attach_opportunity_proof(
        intent, observed, mode="SHADOW",
        decision_epoch=datetime(2026, 10, 28, 10, 5, tzinfo=NY).timestamp(),
    )
    contract = build_us_entry_exit_contract(intent)
    assert verify_us_entry_exit_contract(contract)
    assert (
        contract["entry_provenance"]["us_entry_opportunity_v1"]["evidence_sha256"]
        == observed["evidence_sha256"]
    )

def test_max_entry_price_satisfies_rr_and_stop_distance_at_final_limit():
    bars = sample_bars()
    stop = min(x["low"] for x in bars[-3:])
    ask = sample_quote()["ask"]
    target = ask + 1.6 * (ask - stop)
    decision = evaluate_price_opportunity(
        style="ENTRY_BREAKOUT", trade_date=DAY,
        now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY),
        verified_bars=bars, quote=sample_quote(),
        prep={"breakout_pivot_price": 95.5,
              "timing_target_reference": target},
    )
    assert decision["state"] == "READY", decision
    ceiling = decision["max_entry_price"]
    rr_at_order_limit = (target - ceiling) / (ceiling - stop)
    risk_at_order_limit = (ceiling - stop) / ceiling
    assert rr_at_order_limit >= 1.5 - 1e-8
    assert risk_at_order_limit <= 0.025 + 1e-8
