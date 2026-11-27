"""Native REST read-only shared price tests; no broker credentials or traffic."""
import multiprocessing as mp
import time
import uuid

import pytest
from trader.marketdata import kr_rest_shared as shared


def _quote(code="003490", receipt=None):
    return {"source":"KIS_REST_INQUIRE_PRICE",
            "source_identity":"AUTHENTICATED_REST_REQUEST_BOUND",
            "market":"KR", "requested_symbol":code, "last":29250,
            "bid":29200, "ask":29250, "received_at":time.time() if receipt is None else receipt}


def _leader(key, ready, release, results):
    def fetch():
        ready.set()
        if not release.wait(5):
            raise RuntimeError("test leader timeout")
        return _quote()
    results.put(shared.read_or_fetch(appkey=key, code="003490", fetch=fetch))


def _cleanup(key, code="003490"):
    for path in shared._paths(key, code):
        path.unlink(missing_ok=True)


def test_cross_process_busy_then_hit_same_native_receipt():
    key = "fixture-" + uuid.uuid4().hex
    ctx = mp.get_context("spawn")
    ready, release, results = ctx.Event(), ctx.Event(), ctx.Queue()
    child = ctx.Process(target=_leader, args=(key, ready, release, results))
    child.start()
    def unexpected():
        pytest.fail("follower must not issue an additional broker request")
    try:
        assert ready.wait(10)
        assert shared.read_or_fetch(appkey=key, code="003490", fetch=unexpected,
                                    defer_on_busy=True) == ("BUSY", {})
        release.set()
        status, original = results.get(timeout=10)
        assert status == "FETCH"
        status, row = shared.read_or_fetch(appkey=key, code="003490", fetch=unexpected)
        assert status == "HIT"
        assert row["received_at"] == original["received_at"]
        assert row["source_identity"] == original["source_identity"]
        assert row["last"] == original["last"]
    finally:
        release.set()
        child.join(5)
        if child.is_alive():
            child.terminate()
            child.join(5)
        _cleanup(key)
    assert child.exitcode == 0


@pytest.mark.parametrize("invalid", [{}, {"source":"KIS_WEBSOCKET"},
                                     {"requested_symbol":"012750"},
                                     {"received_at":1.0}, {"last":float("nan")}])
def test_invalid_response_not_reused(invalid):
    key = "fixture-" + uuid.uuid4().hex
    calls = []
    def fetch():
        calls.append(1)
        return {**_quote(), **invalid} if invalid else {}
    try:
        for _ in range(2):
            status, _ = shared.read_or_fetch(appkey=key, code="003490", fetch=fetch)
            assert status == "FETCH"
        assert len(calls) == 2
    finally:
        _cleanup(key)


def test_expiry_and_credential_scope(monkeypatch):
    key = "fixture-" + uuid.uuid4().hex
    other = key + "-other"
    now = time.time()
    monkeypatch.setattr(shared.time, "time", lambda: now)
    try:
        assert shared.read_or_fetch(appkey=key, code="003490", fetch=lambda: _quote(receipt=now))[0] == "FETCH"
        assert shared.read_or_fetch(appkey=other, code="003490", fetch=lambda: _quote(receipt=now))[0] == "FETCH"
        monkeypatch.setattr(shared.time, "time", lambda: now+6)
        assert shared.read_or_fetch(appkey=key, code="003490", fetch=lambda: _quote(receipt=now+6))[0] == "FETCH"
    finally:
        _cleanup(key)
        _cleanup(other)


def test_disabled_by_default_and_real_is_not_enabled(monkeypatch):
    monkeypatch.delenv("KR_WS_SHARED_QUOTE_ENABLED", raising=False)
    assert not shared.enabled("practice")
    monkeypatch.setenv("KR_WS_SHARED_QUOTE_ENABLED", "1")
    assert shared.enabled("practice")
    assert not shared.enabled("real")
