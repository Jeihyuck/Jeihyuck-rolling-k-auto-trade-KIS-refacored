"""KR read-only owner IPC contract, without any KIS network requests."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import multiprocessing as mp
import os
from pathlib import Path
import tempfile
import time
import uuid

import pytest

from trader.marketdata import kr_ws_shared as shared


class FakeConnected:
    def is_set(self):
        return True


class FakeOwner:
    _connected = FakeConnected()

    def __init__(self, key):
        self.key = key

    def _credentials(self):
        return self.key, "synthetic-test-secret"

    def network_allowed(self, market):
        return market == "KR"

    def subscribe_kr(self, symbol):
        pass

    def get_fresh_quote(self, market, symbol, *, max_age_sec):
        if symbol != "003490":
            return None
        return _quote(symbol)


def _quote(symbol="003490", now=None):
    now = time.time() if now is None else now
    kst = datetime.fromtimestamp(now, timezone(timedelta(hours=9)))
    return dict(source="KIS_WEBSOCKET", source_identity="KIS_WS_PARSED_SYMBOL",
                market="KR", symbol=symbol, requested_symbol=symbol, last=29250,
                ask=29250, bid=29200, received_at=now, stale=False,
                stck_bsop_date=kst.strftime("%Y%m%d"), stck_cntg_hour=kst.strftime("%H%M%S"),
                acml_vol=123456, wghn_avrg_stck_prc=29100)


def _owner_process(key, ready, stop):
    async def run():
        handle = await shared.start_owner_server(FakeOwner(key))
        ready.set()
        try:
            while not stop.is_set():
                await asyncio.sleep(.01)
        finally:
            await shared.close_owner_server(handle)
    asyncio.run(run())


def test_two_process_read_and_owner_teardown(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("KR_WS_SHARED_QUOTE_ENABLED", "1")
    key = "test-" + uuid.uuid4().hex
    service = FakeOwner(key)
    ctx = mp.get_context("spawn")
    ready, stop = ctx.Event(), ctx.Event()
    child = ctx.Process(target=_owner_process, args=(key, ready, stop))
    child.start()
    try:
        assert ready.wait(10)
        owner, quote = shared.read_owner_quote(service, "003490", max_age_sec=5)
        assert owner and quote
        assert quote["last"] == 29250 and quote["bid"] == 29200 and quote["ask"] == 29250
        assert quote["acml_vol"] == 123456
        assert quote["shared_owner_pid"] == child.pid != os.getpid()
        assert quote["shared_generation"]
        assert quote["source"] == "KIS_WEBSOCKET"
        assert quote["source_identity"] == "KIS_WS_PARSED_SYMBOL"
        assert shared.read_owner_quote(service, "012750", max_age_sec=5) == (True, None)
        assert shared.read_owner_quote(FakeOwner(key + "other"), "003490", max_age_sec=5) == (False, None)
    finally:
        stop.set()
        child.join(5)
        if child.is_alive():
            child.terminate()
            child.join(5)
    assert child.exitcode == 0
    assert shared.read_owner_quote(service, "003490", max_age_sec=5) == (False, None)


@pytest.mark.parametrize("field,value", [
    ("market", "US"), ("symbol", "012750"), ("requested_symbol", "012750"),
    ("source", "REST"), ("source_identity", "UNKNOWN"), ("stale", True),
    ("stck_bsop_date", "20200101"), ("stck_bsop_date", None),
    ("stck_cntg_hour", None), ("ask", None), ("bid", None), ("bid", 30000),
    ("last", float("nan")), ("last", True),
])
def test_reject_incomplete_or_wrong_source(field, value):
    now = time.time()
    row = _quote(now=now)
    row[field] = value
    assert shared._validated_quote(row, symbol="003490", max_age_sec=5, now=now) is None


def test_stale_and_future_receipt_and_native_time():
    now = time.time()
    for event in (now-10, now+5):
        assert shared._validated_quote(_quote(now=event), symbol="003490",
                                       max_age_sec=5, now=now) is None
    row = _quote(now=now-30)
    row["received_at"] = now
    assert shared._validated_quote(row, symbol="003490", max_age_sec=5, now=now) is None


def test_missing_volume_not_fabricated():
    now = time.time()
    row = _quote(now=now)
    row.pop("acml_vol")
    q = shared._validated_quote(row, symbol="003490", max_age_sec=5, now=now)
    assert q and "acml_vol" not in q and "acml_vol" not in q["shared_valid_fields"]


def test_feature_flag_and_real_environment(monkeypatch):
    monkeypatch.setenv("KR_WS_SHARED_QUOTE_ENABLED", "1")
    monkeypatch.setenv("KIS_ENV", "real")
    assert not shared.enabled()
    monkeypatch.setenv("KIS_ENV", "practice")
    assert shared.enabled()
    monkeypatch.delenv("KR_WS_SHARED_QUOTE_ENABLED")
    assert not shared.enabled()
