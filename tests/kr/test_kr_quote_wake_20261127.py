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


def test_protective_yield_throttles_and_latches_only_valid_hint():
    calls = []
    hint = {"code":"251970","kind":"PROTECTIVE_HARD_STOP_HINT","is_order_authorization":False}
    results = iter([None, {**hint,"code":"012750"}, hint])
    def probe(targets,now):
        calls.append(now)
        return next(results)
    monitor = w.ProtectiveYield([{"code":"251970"}],probe=probe)
    assert monitor.check(now=NOW,monotonic=0) is None
    assert monitor.check(now=NOW,monotonic=.5) is None
    assert monitor.check(now=NOW,monotonic=1) is None
    assert monitor.check(now=NOW,monotonic=2) == hint
    assert monitor.check(now=NOW,monotonic=2.1) == hint
    assert len(calls) == 3


def test_protective_yield_resume_is_clean_pb1_only(monkeypatch):
    from trader.pb1_runner import _kr_protective_yield_resume
    monkeypatch.setenv("KR_PB1_PROTECTIVE_WAKE_ENABLED","1")
    monkeypatch.setenv("PB1_PENDING_RECONCILE_ONLY","0")
    metrics = {"tick_result_reason":"KR_PROTECTIVE_REEVALUATION_REQUIRED",
               "kr_infinite_decision":"WAIT","api_submitted":0,"buy_orders":0,"sell_orders":0}
    args = dict(env="practice",strategy="pb1_pullback_close",session_kind="am",
                status="OK_NO_TRADE",metrics=metrics)
    assert _kr_protective_yield_resume(**args)
    for key,value in (("api_submitted",1),("sell_orders",1),("kr_infinite_decision","BUY"),
                      ("warning_counts",{"timeout_count":1}),("degraded",True)):
        assert not _kr_protective_yield_resume(**{**args,"metrics":{**metrics,key:value}})
    assert not _kr_protective_yield_resume(**{**args,"env":"real"})
    assert not _kr_protective_yield_resume(**{**args,"status":"BLOCKED_RECONCILE"})
    monkeypatch.setenv("PB1_PENDING_RECONCILE_ONLY","1")
    assert not _kr_protective_yield_resume(**args)


def test_protective_daily_read_discards_outstanding_select_without_second_worker(monkeypatch):
    import threading
    slot = threading.BoundedSemaphore(1)
    monkeypatch.setattr(w, "_PRICE_READ_SLOT", slot)
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    calls = []
    def loader():
        calls.append("read")
        started.set()
        try:
            assert release.wait(3)
            return {"stale": "discard"}
        finally:
            finished.set()
    try:
        assert w.protective_daily_read(loader, checkpoint=started.is_set, poll_sec=.01) == (False, None)
        assert started.is_set()
        assert w.protective_daily_read(lambda: calls.append("second"), checkpoint=lambda: False) == (False, None)
        assert calls == ["read"]
    finally:
        release.set()
        assert finished.wait(3)
    assert slot.acquire(timeout=3)
    slot.release()
    assert w.protective_daily_read(lambda: {"fresh": 1}, checkpoint=lambda: False) == (True, {"fresh": 1})


def test_protective_daily_read_error_and_preexisting_hint(monkeypatch):
    import threading
    import pytest
    monkeypatch.setattr(w, "_PRICE_READ_SLOT", threading.BoundedSemaphore(1))
    def explode():
        raise ValueError("native read failed")
    assert w.protective_daily_read(explode, checkpoint=lambda: True) == (False, None)
    with pytest.raises(ValueError, match="native read failed"):
        w.protective_daily_read(explode, checkpoint=lambda: False)
    assert w.protective_daily_read(lambda: 3, checkpoint=lambda: False) == (True, 3)


def test_protective_batch_never_relabels_submitted_or_uncertain_order():
    from types import SimpleNamespace
    from trader.pb1_engine import _submit_finalized_order_candidates, _kr_protective_zero_submit
    candidates = [SimpleNamespace(code=code) for code in ("111111", "222222", "333333")]
    called = []
    def submitter(candidate):
        called.append(candidate.code)
        return {"terminal_event": "ORDER_SUBMIT_UNRESOLVED", "submit_attempted": 1,
                "api_submitted": 1, "failed": 1}
    result = _submit_finalized_order_candidates(
        candidates, submitter=submitter, protective_checkpoint=lambda: bool(called))
    assert called == ["111111"]
    assert result["attempted"] == result["api_submitted"] == result["failed"] == 1
    assert result["skipped"] == 2
    assert result["results"][0]["terminal_event"] == "ORDER_SUBMIT_UNRESOLVED"
    assert not _kr_protective_zero_submit(result, expected_count=3)


def test_protective_batch_clean_deferral_and_no_false_zero_trade():
    from types import SimpleNamespace
    from trader.pb1_engine import _submit_finalized_order_candidates, _kr_protective_zero_submit
    candidates = [SimpleNamespace(code=code) for code in ("111111", "222222")]
    def forbidden(candidate):
        raise AssertionError("deferred BUY must not reach submission")
    result = _submit_finalized_order_candidates(
        candidates, submitter=forbidden, protective_checkpoint=lambda: True)
    assert result["skipped"] == 2
    assert _kr_protective_zero_submit(result, expected_count=2)
    for changed in (
        {**result, "attempted": 1},
        {**result, "failed": 1},
        {**result, "results": result["results"][:1]},
        {**result, "results": [{**result["results"][0], "terminal_event": "ORDER_SUBMIT_UNRESOLVED"},
                                result["results"][1]]},
    ):
        assert not _kr_protective_zero_submit(changed, expected_count=2)
    called = []
    def timing_wait(candidate):
        called.append(candidate.code)
        return {"terminal_event":"FINAL_SKIP", "skipped":1,
                "skipped_reason":"KR_ENTRY_TIMING_NOT_READY_OR_PRICE_TOO_HIGH",
                "timing_state":"WAIT", "timing_reason":"ADAPTIVE_V3_PRICE_OUTSIDE_INTERVAL"}
    mixed = _submit_finalized_order_candidates(
        candidates, submitter=timing_wait, protective_checkpoint=lambda: bool(called))
    assert called == ["111111"]
    assert _kr_protective_zero_submit(mixed, expected_count=2)
    mixed["results"][0]["timing_state"] = "UNKNOWN"
    assert not _kr_protective_zero_submit(mixed, expected_count=2)


def test_active_quote_event_dedupes_across_fresh_ticks_and_idle():
    state = w.QuoteWakeState()
    t = target()
    good = {"code":t["code"], "latest_bar_ts":t["latest_bar_ts"], "is_order_authorization":False}
    monitor = w.ActiveEntryYield(state, probe=lambda *a:good)
    assert monitor.check([t],now=lambda:NOW,monotonic=lambda:0) == good
    assert not state.accept(t, good["latest_bar_ts"])
    assert w.ActiveEntryYield(state,probe=lambda *a:good).check(
        [t],now=lambda:NOW,monotonic=lambda:0) is None


def test_active_quote_event_rejects_wrong_symbol_stale_and_authorization():
    t = target()
    good = {"code":t["code"], "latest_bar_ts":t["latest_bar_ts"], "is_order_authorization":False}
    for result in ({**good,"code":"012750"}, {**good,"is_order_authorization":True},
                   {**good,"latest_bar_ts":(NOW-timedelta(minutes=4)).isoformat()}):
        state = w.QuoteWakeState()
        assert w.ActiveEntryYield(state,probe=lambda *a:result).check(
            [t],now=lambda:NOW,monotonic=lambda:0) is None
        assert not state.seen
    state = w.QuoteWakeState()
    calls = iter([0., 2.])
    assert w.ActiveEntryYield(state,probe=lambda *a:good).check(
        [t],now=lambda:NOW,monotonic=lambda:next(calls)) is None
    assert not state.seen


def test_active_quote_event_defers_only_after_actual_timing_wait():
    from types import SimpleNamespace
    from trader.pb1_engine import _submit_finalized_order_candidates, _kr_deferred_zero_submit_reason
    candidates = [SimpleNamespace(code=code) for code in ("111111","222222","333333")]
    calls = []
    def wait(candidate):
        calls.append(candidate.code)
        return {"terminal_event":"FINAL_SKIP","skipped":1,
                "skipped_reason":"KR_ENTRY_TIMING_NOT_READY_OR_PRICE_TOO_HIGH",
                "timing_state":"WAIT","timing_reason":"ADAPTIVE_V3_PRICE_OUTSIDE_INTERVAL"}
    result = _submit_finalized_order_candidates(
        candidates,submitter=wait,entry_event_checkpoint=lambda:True)
    assert calls == ["111111"]
    assert result["skipped"] == 3 and result["api_submitted"] == 0
    assert _kr_deferred_zero_submit_reason(result,expected_count=3) == "KR_ENTRY_EVENT_REEVALUATION_REQUIRED"
    # Unknown data may not be treated as a clean wake.
    result["results"][0]["timing_state"] = "UNKNOWN"
    assert _kr_deferred_zero_submit_reason(result,expected_count=3) is None
    result["results"][0]["timing_state"] = "WAIT"
    result["results"][2] = {"terminal_event":"FINAL_SKIP","skipped":1,
        "skipped_reason":"KR_PROTECTIVE_REEVALUATION_REQUIRED","protective_deferred":True}
    assert _kr_deferred_zero_submit_reason(result,expected_count=3) == "KR_PROTECTIVE_REEVALUATION_REQUIRED"


def test_active_event_does_not_interrupt_after_any_submit_attempt():
    from types import SimpleNamespace
    from trader.pb1_engine import _submit_finalized_order_candidates, _kr_deferred_zero_submit_reason
    candidates = [SimpleNamespace(code=code) for code in ("111111","222222")]
    calls = []
    def submit(candidate):
        calls.append(candidate.code)
        return {"terminal_event":"ORDER_SUBMIT_UNRESOLVED","submit_attempted":1,"api_submitted":1}
    result = _submit_finalized_order_candidates(
        candidates, submitter=submit,
        entry_event_checkpoint=lambda: (_ for _ in ()).throw(AssertionError("must not probe after a submit")))
    assert calls == ["111111","222222"]
    assert _kr_deferred_zero_submit_reason(result,expected_count=2) is None


def test_active_entry_resume_requires_clean_practice_tick(monkeypatch):
    from trader.pb1_runner import _kr_active_entry_yield_resume
    monkeypatch.setenv("KR_PB1_ACTIVE_QUOTE_WAKE_ENABLED","1")
    monkeypatch.setenv("KR_PB1_QUOTE_WAKE_ENABLED","1")
    monkeypatch.setenv("KR_PB1_FRESH_POLICY_WAIT_RESUME","1")
    monkeypatch.setenv("PB1_PENDING_RECONCILE_ONLY","0")
    metrics = {"tick_result_reason":"KR_ENTRY_EVENT_REEVALUATION_REQUIRED",
               "kr_infinite_decision":"WAIT","api_submitted":0,"buy_orders":0,"sell_orders":0}
    args = dict(env="practice",strategy="pb1_pullback_close",session_kind="am",
                status="OK_NO_TRADE",metrics=metrics)
    assert _kr_active_entry_yield_resume(**args)
    assert not _kr_active_entry_yield_resume(**{**args,"env":"real"})
    assert not _kr_active_entry_yield_resume(**{**args,"metrics":{**metrics,"api_submitted":1}})
    assert not _kr_active_entry_yield_resume(**{**args,"metrics":{**metrics,"degraded":True}})
    monkeypatch.setenv("KR_PB1_ACTIVE_QUOTE_WAKE_ENABLED","0")
    assert not _kr_active_entry_yield_resume(**args)


def test_concurrent_protective_reader_latches_without_order_authority(monkeypatch):
    import threading
    import time
    seen = threading.Event()
    calls = []
    class FakeClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW if tz is not None else NOW.replace(tzinfo=None)
    monkeypatch.setattr(w, "datetime", FakeClock)
    hint = {"kind":"PROTECTIVE_HARD_STOP_HINT","code":"251970",
            "is_order_authorization":False}
    def probe(targets, now):
        calls.append(now)
        seen.set()
        return hint
    monitor = w.ConcurrentProtectiveYield([{"code":"251970"}],
                probe=probe, interval=.05, max_runtime_sec=.6).start()
    try:
        assert seen.wait(1)
        got = monitor.check(now=NOW, monotonic=0)
        assert got and got["code"] == "251970"
        assert got["is_order_authorization"] is False
        assert got["observed_at"] == NOW.isoformat()
        assert monitor.check(now=NOW+timedelta(seconds=11), monotonic=0) is None
        assert monitor.check(now=NOW+timedelta(days=1), monotonic=0) is None
        time.sleep(.1)
        # Repeated read-only probes refresh the latch during a long I/O wait;
        # a one-shot observation could expire before the owner resumes.
        assert len(calls) >= 2
    finally:
        monitor.close()
    assert monitor.check(now=NOW,monotonic=0) is None


def test_concurrent_protective_poll_preempts_blocked_private_select(monkeypatch):
    import threading
    class FakeClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW if tz is not None else NOW.replace(tzinfo=None)
    monkeypatch.setattr(w, "datetime", FakeClock)
    monkeypatch.setattr(w, "_PRICE_READ_SLOT", threading.BoundedSemaphore(1))
    read_entered, release_read, read_done = (threading.Event() for _ in range(3))
    observed = threading.Event()
    hint = {"kind":"PROTECTIVE_HARD_STOP_HINT","code":"251970",
            "is_order_authorization":False}
    def probe(targets, now):
        if not read_entered.wait(1):
            return None
        observed.set()
        return hint
    monitor = w.ConcurrentProtectiveYield([{"code":"251970"}],
            probe=probe, interval=.05, max_runtime_sec=1).start()
    def select():
        read_entered.set()
        try:
            assert release_read.wait(2)
            return {"data":"must_not_cache_after_hint"}
        finally:
            read_done.set()
    try:
        completed, payload = w.protective_daily_read(
            select,
            checkpoint=lambda: monitor.check(now=NOW,monotonic=0),
            poll_sec=.01,
        )
        assert observed.is_set()
        assert (completed, payload) == (False, None)
        assert monitor.check(now=NOW,monotonic=0)["is_order_authorization"] is False
    finally:
        monitor.close()
        release_read.set()
        assert read_done.wait(2)


def test_concurrent_protective_rejects_unknown_symbol_and_closing_race(monkeypatch):
    import threading
    class FakeClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW if tz is not None else NOW.replace(tzinfo=None)
    monkeypatch.setattr(w, "datetime", FakeClock)
    for invalid in (
        {"kind":"PROTECTIVE_HARD_STOP_HINT","code":"012750","is_order_authorization":False},
        {"kind":"PROTECTIVE_HARD_STOP_HINT","code":"251970","is_order_authorization":True},
        {"kind":"WRONG_KIND","code":"251970","is_order_authorization":False},
    ):
        ready = threading.Event()
        def probe(targets, now):
            ready.set()
            return invalid
        monitor = w.ConcurrentProtectiveYield([{"code":"251970"}],
                    probe=probe, interval=.05, max_runtime_sec=.12).start()
        try:
            assert ready.wait(.6)
            assert monitor.check(now=NOW,monotonic=0) is None
        finally:
            monitor.close()
    started, release = threading.Event(), threading.Event()
    def waiting(targets, now):
        started.set()
        assert release.wait(1)
        return {"kind":"PROTECTIVE_HARD_STOP_HINT","code":"251970",
                "is_order_authorization":False}
    monitor = w.ConcurrentProtectiveYield([{"code":"251970"}],
              probe=waiting, interval=.05, max_runtime_sec=.9).start()
    assert started.wait(.5)
    monitor.close()
    release.set()
    if monitor._thread is not None:
        monitor._thread.join(timeout=1)
    assert monitor.check(now=NOW,monotonic=0) is None
