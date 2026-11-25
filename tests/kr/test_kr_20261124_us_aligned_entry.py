"""Offline regressions for the KR US-aligned practice entry policy."""
from datetime import datetime, timedelta
import pytest

from trader.kr import opening_bar_contract as ob
from trader.kr.entry_opportunity_v2 import evaluate_reclaim, validate_live_quote

NOW = datetime(2026, 11, 24, 10, 36, tzinfo=ob.KST)


def bar(at, close, low, high, volume):
    return dict(timestamp=at.isoformat(), date=at.strftime("%Y%m%d"),
                time=at.strftime("%H%M%S"), open=close,
                close=close, low=low, high=high, volume=volume)


def fixture():
    start = NOW.replace(hour=9, minute=0)
    opening = [bar(start+timedelta(minutes=i), 10000, 9950, 10100, 10000)
               for i in range(30)]
    # Equal lows, 0.9% pullback, 80% local volume, no last-high breakout.
    post = [bar(NOW-timedelta(minutes=15-i), 10000, 10000, 10050, 100)
            for i in range(11)]
    post += [bar(NOW-timedelta(minutes=4), 10000, 10000, 10050, 100),
             bar(NOW-timedelta(minutes=3), 10000, 10000, 10050, 100),
             bar(NOW-timedelta(minutes=2), 10010, 10000, 10050, 80),
             bar(NOW-timedelta(minutes=1), 10020, 10010, 10050, 80)]
    return opening, post


def evaluate(opening, post, **kw):
    args = dict(family="PULLBACK", opening=opening, post=post, now=NOW,
                order_price=10050, independent=False)
    args.update(kw)
    return evaluate_reclaim(**args)


def test_us_local_volume_and_equal_low_not_opening_volume():
    opening, post = fixture()
    # At 10000 ask, risk is zero; 10050 ask is too expensive at high 10100.
    # Use the actual executable grid with a sufficiently wide target.
    opening = [dict(b, high=10200) for b in opening]
    result = evaluate(opening, post)
    assert result["state"] == "READY_SHADOW"
    assert result["observed_volume_ratio"] == .8
    assert result["higher_lows"] is True
    assert result["risk_reward_reference"] >= 1.5
    assert result["max_entry_price"] <= result["rr_price_ceiling"]
    assert result["not_order_authorization"] is True


def test_shallow_pullback_allowed_without_last_high_breakout():
    opening, post = fixture()
    # 0.596% drawdown; smaller nominal price grid permits the geometry.
    opening = [dict(b, high=10060) for b in opening]
    result = evaluate(opening, post, order_price=10020)
    # 10000-10050 tick grid means a raw valid geometry need not have a
    # valid native executable tick. Verify depth independently of rounding.
    assert .005 <= result["intraday_pullback_depth"] < .01
    assert result["reason"] != "PULLBACK_SUPPORT_REBOUND_NOT_CONFIRMED"


@pytest.mark.parametrize("change,reason", [
    ("stale", "COMPLETED_BAR_STALE"),
    ("gap", "RECENT_MINUTE_GAP"),
    ("falling", "PULLBACK_SUPPORT_REBOUND_NOT_CONFIRMED"),
    ("volume", "PULLBACK_SUPPORT_REBOUND_NOT_CONFIRMED"),
    ("expensive", "PULLBACK_NO_ROOM_VS_STOP"),
])
def test_fail_closed(change, reason):
    opening, post = fixture()
    args = {}
    if change == "stale":
        args["now"] = NOW+timedelta(minutes=4)
    elif change == "gap":
        post.pop(-3)
    elif change == "falling":
        post[-1] = dict(post[-1], close=10000)
    elif change == "volume":
        post[-1] = dict(post[-1], volume=1)
    elif change == "expensive":
        args["order_price"] = 10080
    result = evaluate(opening, post, **args)
    assert result["state"] != "READY_SHADOW"
    assert result["reason"] == reason


def test_active_proof_path_and_conflicts(monkeypatch):
    opening, post = fixture()
    opening = [dict(b, high=10200) for b in opening]
    payload = {"bars": opening+post, "source_conflicts": [], "vwap": None,
               "vwap_source": "UNKNOWN"}
    monkeypatch.setenv("KR_PB1_ENTRY_OPPORTUNITY_POLICY", "US_ALIGNED_V2")
    monkeypatch.setattr(ob, "load_opening_proof", lambda **kw: payload)
    kw = dict(env="practice", account_scope="test", code="093320", now=NOW,
              style="ENTRY_PULLBACK", setup_ok=True, trigger_ok=False,
              pivot=None, order_price=10050)
    assert ob.evaluate_proof_for_candidate(**kw)["entry_policy"] == "US_ALIGNED_V2"
    assert ob.evaluate_proof_for_candidate(**kw)["state"] == "READY_SHADOW"
    payload["source_conflicts"] = [post[-1]["timestamp"]]
    assert ob.evaluate_proof_for_candidate(**kw)["state"] == "UNKNOWN"
    payload["source_conflicts"] = []
    assert ob.evaluate_proof_for_candidate(**{**kw, "setup_ok":False})["state"] == "INVALID"
    assert ob.active_entry_policy("real") == "LEGACY_V1"


def test_no_opening_count_gate(monkeypatch):
    opening, post = fixture()
    post = [dict(b, high=10200) for b in post[:-3]] + post[-3:]
    payload = {"bars": post, "source_conflicts": [], "vwap":None,
               "vwap_source":"UNKNOWN"}
    monkeypatch.setenv("KR_PB1_ENTRY_OPPORTUNITY_POLICY", "US_ALIGNED_V2")
    monkeypatch.setattr(ob, "load_opening_proof", lambda **kw: payload)
    result = ob.evaluate_proof_for_candidate(env="practice", account_scope="test",
        code="093320", now=NOW, style="ENTRY_PULLBACK", setup_ok=True,
        trigger_ok=False, pivot=None, order_price=10050)
    assert result["state"] == "READY_SHADOW"
    assert result["opening_count"] == 0
    assert result["opening_high"] is None


def test_authenticated_quote_and_final_limit():
    opening, post = fixture()
    opening = [dict(b, high=10200) for b in opening]
    result = evaluate(opening, post)
    quote = dict(source="KIS_REST_INQUIRE_PRICE",
                 source_identity="AUTHENTICATED_REST_REQUEST_BOUND",
                 requested_symbol="093320", market="KR", received_at=NOW.timestamp()-1,
                 last=10020, ask=10050, bid=10025)
    kw = dict(code="093320", timing=result, quote=quote, now=NOW, order_price=10050)
    assert validate_live_quote(**kw)["state"] == "READY"
    for update, state in [
        ({"received_at": NOW.timestamp()-16}, "UNKNOWN"),
        ({"requested_symbol": "005930"}, "UNKNOWN"),
        ({"ask": 10100}, "WAIT"),
        ({"bid": 9900}, "WAIT"),
        ({"ask": None}, "UNKNOWN"),
    ]:
        assert validate_live_quote(**{**kw,"quote":{**quote,**update}})["state"] == state
    assert validate_live_quote(**{**kw,"order_price":result["max_entry_price"]+50})["state"] == "WAIT"


def test_active_engine_rejects_stale_quote_and_market_order(monkeypatch):
    from types import SimpleNamespace
    import trader.pb1_engine as pb1
    opening, post = fixture()
    opening = [dict(b, high=10200) for b in opening]
    proof = evaluate(opening, post)
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_MODE", "ENFORCE")
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_APPROVED", "1")
    monkeypatch.setattr(pb1, "now_kst", lambda: NOW)
    monkeypatch.setattr(pb1, "evaluate_proof_for_candidate", lambda **kw: proof)
    engine = pb1.PB1Engine.__new__(pb1.PB1Engine)
    engine.env = "practice"
    engine._now_kst = NOW
    quote = dict(source="KIS_REST_INQUIRE_PRICE",
                 source_identity="AUTHENTICATED_REST_REQUEST_BOUND",
                 requested_symbol="093320", market="KR", received_at=NOW.timestamp()-16,
                 last=10020, ask=10050, bid=10050)
    engine.kis = SimpleNamespace(get_price_snapshot=lambda *a, **kw: quote)
    cf = SimpleNamespace(code="093320", setup_ok=True,
                         features={"entry_style_selected":"ENTRY_PULLBACK",
                                   "setup_ok":True, "order_price_source":"ask"})
    decision = engine._resolve_entry_pre_submit(
        cf=cf, stage="PB1-AM", order_price=10050, order_type="LIMIT")
    assert not decision.ok
    assert decision.context["timing_reason"] == "V2_NATIVE_QUOTE_STALE"
    assert cf.features["entry_timing_evidence"]["entry_policy"] == "US_ALIGNED_V2"
    quote["received_at"] = NOW.timestamp()-1
    decision = engine._resolve_entry_pre_submit(
        cf=cf, stage="PB1-AM", order_price=10050, order_type="MARKET")
    assert not decision.ok
    assert decision.reason_codes == ["KR_ENTRY_TIMING_PRICE_CAP_UNENFORCEABLE_ORDER_TYPE"]


def test_v2_profile_is_active_not_shadow():
    import os
    import subprocess
    env = {**os.environ, "STRATEGY_ENV":"practice", "KIS_ENV":"practice"}
    for key in ("KR_PB1_ENTRY_OPPORTUNITY_POLICY", "KR_PB1_ENTRY_TIMING_MODE",
                "KR_PB1_ENTRY_TIMING_APPROVED"):
        env.pop(key, None)
    cmd = ('source scripts/wsl/kr-entry-timing-activation.sh; '
           'kr_pb1_entry_timing_defaults; '
           'printf "%s %s %s" "$KR_PB1_ENTRY_OPPORTUNITY_POLICY" '
           '"$KR_PB1_ENTRY_TIMING_MODE" "$KR_PB1_ENTRY_TIMING_APPROVED"')
    output = subprocess.check_output(["bash", "-c", cmd], env=env, text=True)
    assert output.endswith("US_ALIGNED_V2 ENFORCE 1")
    env["KR_PB1_ENTRY_OPPORTUNITY_POLICY"] = "LEGACY_V1"
    output = subprocess.check_output(["bash", "-c", cmd], env=env, text=True)
    assert output.endswith("LEGACY_V1 ENFORCE 1")


def test_realistic_kr_tick_grid_shallow_rebound_ready(monkeypatch):
    # Native 50-won ticks on a 25,000-won stock. Equal lows,
    # no previous high breakout and only 0.8% observed pullback.
    start = NOW.replace(hour=9, minute=0)
    opening = [bar(start+timedelta(minutes=i), 25050, 25000, 25200, 10000)
               for i in range(30)]
    post = [bar(NOW-timedelta(minutes=15-i), 25000, 25000, 25150, 100)
            for i in range(11)]
    post += [bar(NOW-timedelta(minutes=4), 25000, 25000, 25150, 100),
             bar(NOW-timedelta(minutes=3), 25000, 25000, 25150, 100),
             bar(NOW-timedelta(minutes=2), 25050, 25000, 25150, 80),
             bar(NOW-timedelta(minutes=1), 25100, 25050, 25150, 80)]
    # 1.5 R:R cannot be met at 25,100 with target 25,200. The price
    # guard must reject this, even though the new rebound criteria pass.
    blocked = evaluate(opening, post, order_price=25100)
    assert blocked["reason"] == "PULLBACK_NO_ROOM_VS_STOP"
    assert .005 <= blocked["intraday_pullback_depth"] < .01
    # Rebound at 25,100 with stop 25,050 and target 25,200 -> 2R.
    post[-3] = dict(post[-3], low=25050, open=25050, close=25050)
    post[-2] = dict(post[-2], low=25050, open=25100, close=25100)
    post[-1] = dict(post[-1], low=25050, open=25150, close=25150)
    result = evaluate(opening, post, order_price=25100)
    assert result["state"] == "READY_SHADOW", result
    assert result["max_entry_price"] == 25100
    assert result["risk_reward_reference"] == 2
    assert result["higher_lows"] is True
    assert result["intraday_pullback_depth"] < .01
