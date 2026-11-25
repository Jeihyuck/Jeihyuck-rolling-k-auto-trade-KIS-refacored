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


def test_v3_runtime_adapter_keeps_existing_live_quote_guard(monkeypatch):
    from trader.kr.entry_opportunity_v2 import validate_live_quote
    post = [bar(36, 76100, 76000, 200),
            bar(37, 76300, 76000, 207),
            bar(38, 76400, 76300, 250),
            bar(39, 76400, 76400, 252)]
    now = DAY+timedelta(minutes=40, seconds=57)
    rows = []
    for b in opening()+post:
        at = datetime.fromisoformat(b["timestamp"])
        rows.append(dict(b, date=at.strftime("%Y%m%d"), time=at.strftime("%H%M%S"),
                         open=b["close"]))
    payload = {"bars": rows, "source_conflicts": [], "vwap": None, "vwap_source": "UNKNOWN"}
    monkeypatch.setenv("KR_PB1_ENTRY_OPPORTUNITY_POLICY", "US_ALIGNED_V2")
    monkeypatch.setattr(contract, "load_opening_proof", lambda **_: payload)
    kw = dict(env="practice", account_scope="test", code="251970", now=now,
              style="ENTRY_MOMENTUM", setup_ok=True, trigger_ok=False,
              pivot=None, order_price=76500)
    monkeypatch.setenv("KR_PB1_ADAPTIVE_ENTRY_V3_ENABLED", "0")
    assert contract.evaluate_proof_for_candidate(**kw)["state"] == "WAIT"
    monkeypatch.setenv("KR_PB1_ADAPTIVE_ENTRY_V3_ENABLED", "1")
    proof = contract.evaluate_proof_for_candidate(**kw)
    assert proof["state"] == "READY_SHADOW", proof
    assert proof["entry_policy"] == "US_ALIGNED_V2"
    assert proof["adaptive_revision"] == "V3"
    assert proof["risk_reward_reference"] == 5.0
    quote = dict(source="KIS_REST_INQUIRE_PRICE",
                 source_identity="AUTHENTICATED_REST_REQUEST_BOUND",
                 requested_symbol="251970", market="KR", received_at=now.timestamp()-1,
                 last=76400, ask=76500, bid=76400)
    check = dict(code="251970", timing=proof, quote=quote, now=now, order_price=76500)
    assert validate_live_quote(**check)["state"] == "READY"
    assert validate_live_quote(**{**check, "quote": {**quote, "ask":76600}})["state"] == "WAIT"
    assert validate_live_quote(**{**check, "quote": {**quote, "received_at":now.timestamp()-16}})["state"] == "UNKNOWN"
    payload["source_conflicts"] = [post[-1]["timestamp"]]
    assert contract.evaluate_proof_for_candidate(**kw)["state"] == "UNKNOWN"
    payload["source_conflicts"] = []
    assert contract.evaluate_proof_for_candidate(**{**kw, "setup_ok":False})["state"] == "INVALID"


def test_minute_captured_before_close_cannot_mature_during_other_io():
    from scripts.kr_opening_observer import _request_completed_rows, _stale_minute_anchor
    request = DAY+timedelta(minutes=39, seconds=59)
    rows = [dict(date="20261125", time="093800", open=76400, high=76400,
                 low=76300, close=76400, volume=100, _kis_http_observation_id="a"*32),
            dict(date="20261125", time="093900", open=76400, high=76500,
                 low=76400, close=76500, volume=20, _kis_http_observation_id="b"*32)]
    assert _request_completed_rows(rows, request) == rows[:1]
    assert _request_completed_rows(rows, request+timedelta(seconds=2)) == rows
    assert _stale_minute_anchor({"reason":"COMPLETED_BAR_STALE"}, request) == "093900"


def test_hashkey_retry_proof_is_zero_fill_and_same_unit_only():
    from trader.kr.infinite.repository import InfiniteRepository
    from trader.kr.infinite.models import State, BrokerPosition
    now = DAY+timedelta(minutes=40)
    state = State(cycle_id="owned", units_used=3)
    pos = BrokerPosition(39, 39, 54268.256, 47070)
    row = dict(status="REJECTED", broker_order_id=None, filled_qty=0,
               action_state="RETRYABLE", active_attempt_id=None, claim_conflicts=0,
               cumulative_filled_qty=0, requested_qty=15, target_qty=15,
               remaining_target_qty=15, unit_sequence=4, idempotency_key="key",
               updated_at=now-timedelta(seconds=61),
               metadata=dict(strategy_owner="KR_INFINITE", pre_order_holding_qty=39,
                             pre_order_avg_price=54268.256, pre_order_orderable_qty=39,
                             rejection="request failed after retries: /uapi/hashkey err=HTTP 500"))
    attempt = dict(attempt_state="REJECTED_EXPLICIT", authoritative=True,
                   cumulative_filled_qty=0, client_order_key="key")
    proof = InfiniteRepository._prehash_retry_proven
    assert proof(row, [attempt], state, pos, now)
    for bad in ({"broker_order_id":"999"}, {"filled_qty":1}, {"active_attempt_id":"pending"},
                {"action_state":"UNCERTAIN"}, {"remaining_target_qty":14},
                {"unit_sequence":5}, {"updated_at":now}):
        assert not proof({**row, **bad}, [attempt], state, pos, now)
    assert not proof(row, [{**attempt, "authoritative":False}], state, pos, now)
    assert not proof(row, [attempt]*3, state, pos, now)
    assert not proof(row, [attempt], state, BrokerPosition(40,40,54268.256,47070), now)


@pytest.mark.parametrize("response,expected", [
    ({"rt_cd":"0", "output1":[]}, True),
    ({"rt_cd":"0", "output1":[{"pdno":"005930"}]}, True),
    ({"rt_cd":"0", "output1":[{"pdno":"122630"}]}, False),
    ({"rt_cd":"0", "output1":[{}]}, False),
    ({"rt_cd":"0"}, False),
    ({"output1":[]}, False),
    ({"rt_cd":"1", "output1":[]}, False),
    ({"rt_cd":"0", "output1":[], "_fill_source":"daily_ccld_fail_open"}, False),
])
def test_hashkey_retry_native_no_order_requires_explicit_complete_success(response, expected):
    from types import SimpleNamespace
    from trader.kr.infinite.executor import KISExecutor
    kis = SimpleNamespace(inquire_daily_ccld=lambda **_: response)
    assert KISExecutor(kis, "practice").verify_no_same_day_symbol_order("122630", DAY.date()) is expected
    assert not KISExecutor(kis, "real").verify_no_same_day_symbol_order("122630", DAY.date())


@pytest.mark.parametrize("ask,expected", [(77300, "BOUNDED_LIMIT"), (77400, "BLOCK"), (None, "BLOCK")])
def test_adaptive_actual_ask_reprice_no_passive_or_chase(monkeypatch, ask, expected):
    from types import SimpleNamespace
    from trader.kr import entry_timing_price as price
    from trader import account_state
    now = DAY+timedelta(minutes=44, seconds=56)
    class FixedClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now
    monkeypatch.setattr(price, "datetime", FixedClock)
    monkeypatch.setenv("KR_PB1_ADAPTIVE_ENTRY_V3_ENABLED", "1")
    monkeypatch.setenv("KR_PB1_ENTRY_OPPORTUNITY_POLICY", "US_ALIGNED_V2")
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_MODE", "ENFORCE")
    monkeypatch.setenv("KR_PB1_ENTRY_TIMING_APPROVED", "1")
    monkeypatch.setattr(account_state, "get_account_key", lambda **_: "offline")
    monkeypatch.setattr(contract, "load_opening_proof", lambda **_: {"bars":[{"close":77200}]})
    monkeypatch.setattr(contract, "evaluate_proof_for_candidate", lambda **_: {
        "state":"READY_SHADOW", "entry_policy":"US_ALIGNED_V2",
        "reference_price":77200, "max_entry_price":77300})
    quote = dict(source="KIS_REST_INQUIRE_PRICE", source_identity="AUTHENTICATED_REST_REQUEST_BOUND",
                 requested_symbol="251970", market="KR", received_at=now.timestamp()-1,
                 last=77200, bid=77200, ask=ask)
    engine = SimpleNamespace(STRATEGY_NAME="pb1_pullback_close", env="practice",
                             kis=SimpleNamespace(get_price_snapshot=lambda *a,**k: quote))
    cf = SimpleNamespace(code="251970", features={"order_price_source":"ask",
                        "entry_style_selected":"MOMENTUM", "setup_ok":True})
    result = price.maybe_bound_pb1_prpr_fallback(engine=engine, cf=cf, stage="PB1-AM",
                         order_type="LIMIT", original_price=77400, now=now)
    assert result["state"] == expected, result
    if expected == "BOUNDED_LIMIT":
        assert result["new_limit"] == 77300 and result["not_order_authorization"]
        plan = {"qty":15}
        assert price.apply_verified_bounded_limit(cf=cf,plan=plan,price_proof=result,
                            original_price=77400,qty=15) == 77300
        assert plan["qty"] == 15 and plan["planned_value"] == 77300*15
        assert cf.features["order_price_source"] == "timing_bounded_verified_ask"


def test_historical_buy_native_evidence_never_confuses_ack_and_fill():
    from scripts.kr_historical_buy_native_evidence import exact_buy_evidence
    from datetime import date
    day = date(2026, 10, 27)
    kw = dict(day=day, code="293490", odno="0000011280", qty=174)
    # Reconstructed ACK vs hypothetical completed native row; not actual fill.
    ack = dict(pdno="293490", odno="0000011280", ord_dt="20261027",
               sll_buy_dvsn_cd="02", ord_qty="174", tot_ccld_qty="0",
               rmn_qty="174", tot_ccld_amt="0", avg_prvs="0", cncl_yn="N")
    complete = dict(ack, tot_ccld_qty="174", rmn_qty="0",
                    tot_ccld_amt=str(174*11750), avg_prvs="11750")
    def page(rows):
        return {"rt_cd":"0", "_sep29_complete":True, "output1":rows}
    assert exact_buy_evidence(page([ack]), **kw)["state"] == "ORIGINAL_FULL_EXECUTION_NOT_PROVEN"
    result = exact_buy_evidence(page([complete]), **kw)
    assert result["state"] == "ORIGINAL_FULL_EXECUTION_PROVEN"
    assert result["notional"] == str(174*11750)
    assert exact_buy_evidence(page([complete, complete]), **kw)["state"] == "ORIGINAL_ORDER_NOT_UNIQUE"
    assert exact_buy_evidence(page([dict(complete, sll_buy_dvsn_cd="01")]), **kw)["state"] == "ORIGINAL_FULL_EXECUTION_NOT_PROVEN"
    assert exact_buy_evidence(page([dict(complete, ord_dt="20261028")]), **kw)["state"] == "ORIGINAL_ORDER_NOT_UNIQUE"
    assert exact_buy_evidence(dict(page([complete]), _sep29_complete=False), **kw)["state"] == "NATIVE_HISTORY_UNVERIFIED"
