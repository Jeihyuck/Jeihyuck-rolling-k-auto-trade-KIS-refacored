from datetime import datetime, timedelta

from trader.kr import quote_wake as w
from trader.kr.opening_bar_contract import KST

NOW = datetime(2026,11,27,9,44,tzinfo=KST)


def target(**override):
    timing = dict(entry_policy="US_ALIGNED_V2",state="WAIT",
                  reason="ADAPTIVE_V3_PRICE_OUTSIDE_INTERVAL",
                  diagnostic_entry_ceiling=79800,diagnostic_entry_floor=79300,
                  structural_stop_reference=79200,latest_bar_ts=(NOW-timedelta(minutes=1)).isoformat())
    return w.make_wait_target(code="251970",style="ENTRY_MOMENTUM",setup_ok=True,
                             trigger_ok=False,pivot=None,timing={**timing,**override},
                             now=NOW,account_scope="practice:12345678:01")


def test_target_requires_actual_price_wait():
    assert target()["is_order_authorization"] is False
    assert target(state="READY_SHADOW") is None
    assert target(reason="NO_CONFIRMED_MOMENTUM_RECLAIM") is None
    assert target(diagnostic_entry_ceiling=79000) is None


def test_live_quote_rechecks_actual_native_timing(monkeypatch):
    calls = []
    def evaluate(**kwargs):
        calls.append(kwargs)
        return dict(state="READY_SHADOW",entry_policy="US_ALIGNED_V2",
                    max_entry_price=79800,reference_price=79800,
                    latest_bar_ts=(NOW-timedelta(minutes=1)).isoformat())
    monkeypatch.setattr(w,"evaluate_proof_for_candidate",evaluate)
    quote = dict(source="KIS_WEBSOCKET",source_identity="KIS_WS_PARSED_SYMBOL",
                 market="KR",symbol="251970",last=79800,bid=79700,ask=79800,
                 received_at=NOW.timestamp())
    assert w.probe_wait_target(target(),NOW,read_quote=lambda code:quote)["code"] == "251970"
    assert calls[0]["order_price"] == 79800
    assert w.probe_wait_target(target(),NOW,read_quote=lambda code:{**quote,"ask":79900}) is None
    assert w.probe_wait_target(target(),NOW,read_quote=lambda code:{**quote,"symbol":"012750"}) is None
    assert w.probe_wait_target(target(),NOW,read_quote=lambda code:{**quote,"received_at":NOW.timestamp()-6}) is None
    assert len(calls)==1
    monkeypatch.setattr(w,"evaluate_proof_for_candidate",lambda **kw: {"state":"WAIT"})
    assert w.probe_wait_target(target(),NOW,read_quote=lambda code:quote) is None


def test_serial_idle_wake_and_dedupe():
    elapsed = [0.]
    state = w.QuoteWakeState()
    def sleep(seconds): elapsed[0] += seconds
    def now(): return NOW+timedelta(seconds=elapsed[0])
    def probe(t,n):
        if elapsed[0]>=2:
            return {"code":t["code"],"latest_bar_ts":t["latest_bar_ts"]}
    args=dict(targets=[target()],state=state,seconds=10,session_end=NOW+timedelta(minutes=1),
              now=now,monotonic=lambda:elapsed[0],sleep=sleep,probe=probe)
    assert w.wait_for_quote_wake(**args)["code"]=="251970"
    assert elapsed[0]==2
    assert w.wait_for_quote_wake(**args) is None
    assert elapsed[0]==12


def test_late_probe_does_not_wake_or_consume_dedupe():
    for deadline, session_seconds in ((2, 60), (60, 2)):
        elapsed = [0.0]
        state = w.QuoteWakeState()
        def probe(t, n):
            elapsed[0] += 3
            return {"code": t["code"], "latest_bar_ts": t["latest_bar_ts"]}
        assert w.wait_for_quote_wake(
            targets=[target()], state=state, seconds=deadline,
            session_end=NOW+timedelta(seconds=session_seconds),
            now=lambda: NOW+timedelta(seconds=elapsed[0]),
            monotonic=lambda: elapsed[0], sleep=lambda s: None, probe=probe,
        ) is None
        assert not state.seen


def test_wrong_symbol_probe_does_not_consume_dedupe():
    elapsed = [0.0]
    state = w.QuoteWakeState()
    def sleep(seconds): elapsed[0] += seconds
    assert w.wait_for_quote_wake(
        targets=[target()], state=state, seconds=2,
        session_end=NOW+timedelta(minutes=1),
        now=lambda: NOW+timedelta(seconds=elapsed[0]),
        monotonic=lambda: elapsed[0], sleep=sleep,
        probe=lambda t, n: {"code": "012750", "latest_bar_ts": t["latest_bar_ts"]},
    ) is None
    assert not state.seen


def test_incomplete_local_quote_uses_existing_shared_owner(monkeypatch):
    from trader.marketdata import kis_ws_price, kr_ws_shared
    quote = dict(source="KIS_WEBSOCKET",source_identity="KIS_WS_PARSED_SYMBOL",
                 market="KR",symbol="251970",last=79800,bid=79700,ask=79800,
                 received_at=datetime.now(KST).timestamp())
    class Service:
        current = {**quote, "ask": None}
        def get_fresh_quote(self, *args, **kwargs):
            return self.current
    service = Service()
    calls = []
    def shared(svc, code, **kw):
        calls.append((code,kw))
        return True, quote
    monkeypatch.setattr(kis_ws_price,"get_kis_ws_price_service",lambda:service)
    monkeypatch.setattr(kr_ws_shared,"read_owner_quote",shared)
    assert w._local_quote("251970") == quote
    assert calls[0][1]["required_fields"] == ("last","bid","ask")
    service.current = quote
    assert w._local_quote("251970") == quote
    assert len(calls) == 1


def test_protective_wake_is_owner_stop_hint_not_order_authorization():
    evaluation = {"code":"251970","exit_policy_family":"SWING_STAGED_EXIT",
                  "exit_ok":False,"signal_hit":False,"orderable_qty":12,
                  "current_price":10000,"condition_details":{"paper_stop36":
                  {"eligible":True,"effective_hard_stop":9400}}}
    hint = w.make_protective_target(evaluation,NOW)
    assert hint and hint["is_order_authorization"] is False
    for changes in ({"code":"122630"},{"exit_policy_family":"DAY_TRADE"},
                    {"signal_hit":True},{"current_price":9400},
                    {"order_skip_reasons":["SAME_DAY_SOFT_EXIT_MIN_HOLD"]}):
        assert w.make_protective_target({**evaluation,**changes},NOW) is None
    quote = dict(source="KIS_WEBSOCKET",source_identity="KIS_WS_PARSED_SYMBOL",
                 market="KR",symbol="251970",last=9390,received_at=NOW.timestamp())
    result = w.probe_protective_targets([hint],NOW,read_quote=lambda c:quote)
    assert result == {"code":"251970","kind":"PROTECTIVE_HARD_STOP_HINT",
                      "is_order_authorization":False}
    for changes in ({"symbol":"012750"},{"received_at":NOW.timestamp()-6},
                    {"last":9500},{"source":"KIS_REST_INQUIRE_PRICE",
                    "source_identity":"AUTHENTICATED_REST_REQUEST_BOUND"}):
        assert w.probe_protective_targets([hint],NOW,read_quote=lambda c:{**quote,**changes}) is None


def test_protection_poll_precedes_entry_and_does_not_consume_buy_dedupe():
    elapsed=[0.]
    state=w.QuoteWakeState()
    hint={"code":"251970","kind":"PROTECTIVE_HARD_STOP_HINT","is_order_authorization":False}
    result=w.wait_for_quote_wake(
        targets=[target()],protective_targets=[hint],state=state,
        seconds=10,session_end=NOW+timedelta(minutes=1),now=lambda:NOW,
        monotonic=lambda:elapsed[0],sleep=lambda n:elapsed.__setitem__(0,elapsed[0]+n),
        probe=lambda *a: (_ for _ in ()).throw(AssertionError("BUY probe must not run first")),
        protective_probe=lambda *a:hint)
    assert result == hint
    assert elapsed[0] == 0
    assert not state.seen
