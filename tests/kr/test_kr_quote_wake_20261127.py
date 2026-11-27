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
