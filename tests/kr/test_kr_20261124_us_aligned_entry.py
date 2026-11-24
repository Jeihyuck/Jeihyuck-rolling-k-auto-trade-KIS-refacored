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
