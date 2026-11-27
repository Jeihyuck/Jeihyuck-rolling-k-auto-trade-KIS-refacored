"""Price-aware US opening entry policy: no REST/network/order side effects."""
from __future__ import annotations

import copy
from decimal import Decimal, ROUND_DOWN
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
              "limit_price": float(Decimal(str(result["max_entry_price"])).quantize(
                   Decimal("0.01"), rounding=ROUND_DOWN)), "meta": {}}
    attach_opportunity_proof(intent, result, mode="ENFORCE", decision_epoch=now_epoch)
    quote = {"last": 96.05, "bid": 96.0, "ask": 96.05,
             "received_at": now_epoch + 2}
    assert pre_submit_timing_check(
        intent, now_epoch=now_epoch + 3, fresh_ws_quote=quote,
    ) == (True, "TIMING_FRESH_PRICE_CONFIRMED")
    assert intent["meta"]["us_entry_opportunity_v1"]["max_age_sec"] == 180.0
    assert pre_submit_timing_check(
        intent, now_epoch=now_epoch + 185, fresh_ws_quote=quote,
    )[1] == "TIMING_SIGNAL_EXPIRED"
    # An 180s signal is not invalidated at the old 120s boundary when
    # its quote is genuinely fresh at the KIS HTTP pre-submit check.
    fresh_late = {**quote, "received_at": now_epoch + 174}
    assert pre_submit_timing_check(
        intent, now_epoch=now_epoch + 175, fresh_ws_quote=fresh_late,
    ) == (True, "TIMING_FRESH_PRICE_CONFIRMED")
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

def test_stale_approved_signal_cannot_be_refreshed_by_mutating_timestamp_or_order_key():
    decision = evaluate_price_opportunity(
        style="ENTRY_PULLBACK", trade_date=DAY,
        now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY),
        verified_bars=sample_bars(), quote=sample_quote(),
    )
    epoch = datetime(2026, 10, 28, 10, 5, tzinfo=NY).timestamp()
    intent = {
        "symbol": "ELV", "side": "BUY", "strategy_owner": "US_STANDARD",
        "position_action": "NEW_POSITION_BUY", "client_order_key": "k1",
        "position_lifecycle_id": "cycle-1",
        "limit_price": float(Decimal(str(decision["max_entry_price"])).quantize(
            Decimal("0.01"), rounding=ROUND_DOWN)), "meta": {},
    }
    attach_opportunity_proof(intent, decision, mode="ENFORCE", decision_epoch=epoch)
    quote = {"received_at": epoch + 3, "last": 96.05, "bid": 96.0, "ask": 96.05}
    assert pre_submit_timing_check(
        intent, now_epoch=epoch + 3, fresh_ws_quote=quote,
    )[0] is True
    changed_epoch = copy.deepcopy(intent)
    changed_epoch["meta"]["us_entry_opportunity_v1"]["decision_epoch"] += 60
    assert pre_submit_timing_check(
        changed_epoch, now_epoch=epoch + 3, fresh_ws_quote=quote,
    )[1] == "TIMING_BINDING_INVALID"
    changed_key = copy.deepcopy(intent)
    changed_key["client_order_key"] = "wrong-order"
    assert pre_submit_timing_check(
        changed_key, now_epoch=epoch + 3, fresh_ws_quote=quote,
    )[1] == "TIMING_BINDING_INVALID"
    changed_mode = copy.deepcopy(intent)
    changed_mode["meta"]["us_entry_opportunity_v1"]["mode"] = "SHADOW"
    assert pre_submit_timing_check(
        changed_mode, now_epoch=epoch + 3, fresh_ws_quote=quote,
    )[0] is False


def test_stale_and_missing_opening_bars_report_native_evidence_without_relaxing_gate():
    now = datetime(2026, 10, 28, 10, 9, tzinfo=NY)
    bars = sample_bars()
    stale = evaluate_price_opportunity(
        style="ENTRY_PULLBACK", trade_date=DAY, now_et=now,
        verified_bars=bars, quote=sample_quote(),
    )
    assert stale["state"] == "WAIT"
    assert stale["reason"] == "recent_minute_bars_stale"
    facts = stale["facts"]
    assert facts["last_completed_bar"] == bars[-1]["time_et"]
    assert facts["last_completed_bar_age_sec"] == 360.0
    assert facts["opening"]["observed_1m"] == 30

    sparse = evaluate_price_opportunity(
        style="ENTRY_PULLBACK", trade_date=DAY,
        now_et=datetime(2026, 10, 28, 10, 5, tzinfo=NY),
        verified_bars=bars[::3], quote=sample_quote(),
    )
    assert sparse["state"] == "WAIT"
    assert sparse["reason"] == "opening_minute_bars_insufficient"
    assert sparse["facts"]["opening"]["observed_1m"] < 30
    assert sparse["facts"]["last_completed_bar"] is not None
    assert sparse["facts"]["last_completed_bar_age_sec"] is not None


def _independent_native_window():
    # Synthetic complete native-shaped rows, not original Kroger replay.
    start = datetime(2026, 10, 28, 10, 0, tzinfo=NY)
    values = [102, 101, 100, 99, 98.5, 98.6, 98.8, 99.0]
    rows = []
    for i, price in enumerate(values):
        rows.append({"xymd": "20261028", "xhms": (start + timedelta(minutes=i)).strftime("%H%M%S"),
                     "open": str(price), "high": str(price + .1), "low": str(price - .2),
                     "last": str(price), "evol": "200", "eamt": str(price * 200)})
    return normalize_bars(rows, trade_date=DAY, now_et=start + timedelta(minutes=8))


@pytest.mark.parametrize("family", ["ENTRY_PULLBACK", "ENTRY_MOMENTUM"])
def test_independent_5_plus_3_does_not_authorize_us_without_opening(family):
    bars = _independent_native_window()
    result = evaluate_price_opportunity(style=family, trade_date=DAY,
        now_et=datetime(2026, 10, 28, 10, 8, tzinfo=NY), verified_bars=bars,
        quote=sample_quote(99, 98.98, 99))
    assert result["state"] == "WAIT", result
    assert result["reason"] == "opening_minute_bars_insufficient"
    assert result["facts"]["reference_path"] == "OPENING_RANGE"
    assert result["facts"]["opening"]["observed_1m"] == 0
    assert result["broker_authorized"] is False


@pytest.mark.parametrize("case", ["gap", "stale", "breakout", "no_rebound", "late_peak"])
def test_independent_window_never_bypasses_missing_dependencies(case):
    bars = _independent_native_window()
    now = datetime(2026, 10, 28, 10, 8, tzinfo=NY)
    family = "ENTRY_PULLBACK"
    if case == "gap":
        bars.pop(2)
    elif case == "stale":
        now += timedelta(minutes=4)
    elif case == "breakout":
        family = "ENTRY_BREAKOUT"
    elif case == "no_rebound":
        bars[-1]["close"] = 98.7
    elif case == "late_peak":
        bars[4]["high"] = 103
    result = evaluate_price_opportunity(style=family, trade_date=DAY, now_et=now,
        verified_bars=bars, quote=sample_quote(99, 98.98, 99))
    assert result["state"] == "WAIT", result


def test_static_feature_reuse_preserves_decision_and_invalidates_correction():
    from trader.marketdata.feature_reuse import feature_cache_stats, clear_feature_cache
    clear_feature_cache()
    bars = sample_bars()
    kwargs = dict(style="ENTRY_PULLBACK", trade_date=DAY,
                  now_et=datetime(2026,10,28,10,5,tzinfo=NY), verified_bars=bars,
                  quote=sample_quote())
    plain = evaluate_price_opportunity(**kwargs)
    scope=("practice-account-epoch","ABC","NASDAQ","US_STANDARD")
    first = evaluate_price_opportunity(**kwargs, feature_scope=scope)
    second = evaluate_price_opportunity(**kwargs, feature_scope=scope)
    assert first == second == plain
    assert feature_cache_stats()["hits"] == 2
    corrected = copy.deepcopy(bars)
    corrected[0]["high"] += 1.0
    kwargs["verified_bars"] = corrected
    changed = evaluate_price_opportunity(**kwargs, feature_scope=scope)
    assert changed == evaluate_price_opportunity(**kwargs)
    assert changed["facts"]["opening"]["opening_high"] != plain["facts"]["opening"]["opening_high"]
    assert feature_cache_stats()["misses"] == 3
    # A later time never reuses a stale decision; only static operands.
    kwargs["now_et"] += timedelta(minutes=5)
    assert evaluate_price_opportunity(**kwargs, feature_scope=scope)["reason"] == "recent_minute_bars_stale"
    clear_feature_cache()


def test_cross_process_native_features_exact_scope_and_correction(tmp_path, monkeypatch):
    import json
    import subprocess
    import sys
    from trader.marketdata.feature_reuse import shared_native_features
    kw=dict(market="US",scope=("acct:epoch","ABC","NASDAQ","US_STANDARD"),
            trade_date=DAY,settings=("TEST_V1",),base=tmp_path,rows=[{"close":100}])
    assert shared_native_features(**kw,compute=lambda rows: {"close":rows[0]["close"]})=={"close":100}
    script = """import json,sys
from trader.marketdata.feature_reuse import shared_native_features
def forbidden(rows):
    raise RuntimeError("expected shared hit, not recomputation")
kw=json.loads(sys.argv[1])
print(json.dumps(shared_native_features(**kw,compute=forbidden)))
"""
    wire={**kw,"base":str(tmp_path)}
    assert json.loads(subprocess.check_output([sys.executable,"-c",script,json.dumps(wire)],text=True))=={"close":100}
    for change in ({"rows":[{"close":101}]},{"scope":("other:epoch","ABC","NASDAQ","US_STANDARD")},
                   {"settings":("TEST_V2",)},{"trade_date":"2026-10-29"},
                   {"market":"KR"}):
        calls=[]
        def compute(rows):
            calls.append(rows)
            return {"close":rows[0]["close"]}
        assert shared_native_features(**(kw|change),compute=compute)=={"close":(kw|change)["rows"][0]["close"]}
        assert len(calls)==1


def test_shared_feature_cache_failure_never_replaces_native_calculation(tmp_path):
    import sqlite3
    from trader.marketdata.feature_reuse import shared_native_features
    kw=dict(market="US",scope=("acct:epoch","ABC","NASDAQ","US_STANDARD"),
            trade_date=DAY,settings=("TEST_V1",),base=tmp_path,rows=[{"close":100}])
    assert shared_native_features(**kw,compute=lambda rows:{"close":100})=={"close":100}
    path=next(tmp_path.glob("native-features-*.sqlite3"))
    db=sqlite3.connect(path)
    try:
        db.execute("UPDATE native_features SET payload='bad-json'")
        db.commit()
        assert shared_native_features(**kw,compute=lambda rows:{"close":100})=={"close":100}
        db.execute("BEGIN EXCLUSIVE")
        assert shared_native_features(**kw,compute=lambda rows:{"close":100})=={"close":100}
    finally:
        db.rollback()
        db.close()
    path.chmod(0o644)
    calls=[]
    assert shared_native_features(**kw,compute=lambda rows:calls.append(rows) or {"close":100})=={"close":100}
    assert len(calls)==1


def test_shared_opening_and_rolling_feature_adapter_preserves_live_decision(tmp_path,monkeypatch):
    from trader.marketdata.feature_reuse import clear_feature_cache
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("WSL_RUN_MARKET","US")
    monkeypatch.setenv("KIS_ENV","practice")
    monkeypatch.setenv("US_SHARED_NATIVE_FEATURES_ENABLED","1")
    scope=("acct:epoch","ABC","NASDAQ","US_STANDARD")
    kwargs=dict(style="ENTRY_PULLBACK",trade_date=DAY,now_et=datetime(2026,10,28,10,5,tzinfo=NY),
                verified_bars=sample_bars(),quote=sample_quote())
    clear_feature_cache()
    assert evaluate_price_opportunity(**kwargs,feature_scope=scope)==evaluate_price_opportunity(**kwargs)
    clear_feature_cache()
    assert evaluate_price_opportunity(**kwargs,feature_scope=scope)==evaluate_price_opportunity(**kwargs)
    corrected=copy.deepcopy(kwargs["verified_bars"])
    corrected[-1]["volume"]*=2
    kwargs["verified_bars"]=corrected
    assert evaluate_price_opportunity(**kwargs,feature_scope=scope)==evaluate_price_opportunity(**kwargs)
    kwargs["now_et"]+=timedelta(minutes=5)
    assert evaluate_price_opportunity(**kwargs,feature_scope=scope)["reason"]=="recent_minute_bars_stale"
    clear_feature_cache()

