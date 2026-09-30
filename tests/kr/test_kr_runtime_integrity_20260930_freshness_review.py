from __future__ import annotations

import threading
import time
from types import SimpleNamespace

import pytest

from trader.cache_ttl import PRICE_SNAPSHOT_TTL_SEC, price_cache
from trader.kr import runtime_integrity_20260930 as base
from trader.kr import runtime_integrity_20260930_freshness_review as fix


class FakeWsService:
    def __init__(self, quote=None):
        self.quote = quote
        self.subscriptions: list[str] = []
        self.get_max_ages: list[float] = []
        self.wait_max_ages: list[float] = []

    def subscribe_kr(self, code: str) -> None:
        self.subscriptions.append(code)

    def get_fresh_quote(self, market: str, code: str, *, max_age_sec: float):
        self.get_max_ages.append(float(max_age_sec))
        return None if self.quote is None else dict(self.quote)

    def wait_for_fresh_quote(self, market: str, code: str, *, max_age_sec: float, wait_sec: float):
        self.wait_max_ages.append(float(max_age_sec))
        return None


class FakeRestKis:
    """Mimic get_price_quote's inner two-second TTL cache before HTTP."""

    def __init__(self, http_quote=None, *, http_source_age_sec: float = 0.0, cache_result: bool = True):
        self.http_quote = http_quote
        self.http_source_age_sec = float(http_source_age_sec)
        self.cache_result = bool(cache_result)
        self.calls = 0
        self.cache_hits = 0
        self.http_calls = 0

    def get_price_quote(self, code: str, *, diag_mode: bool = False, attempts: int = 2):
        self.calls += 1
        key = ("inquire-price", code)
        cached = price_cache.get(key)
        if cached:
            self.cache_hits += 1
            return cached

        self.http_calls += 1
        if self.http_quote is None:
            return {}
        quote = dict(self.http_quote)
        if self.cache_result:
            source_ts = time.time() - self.http_source_age_sec
            expires_at = source_ts + float(PRICE_SNAPSHOT_TTL_SEC)
            price_cache.cache[key] = (quote, expires_at)
        return quote


@pytest.fixture(autouse=True)
def _freshness_state(monkeypatch):
    price_cache.cache.clear()
    fake_outer = SimpleNamespace(cache={}, lock=threading.Lock())
    monkeypatch.setattr(fix.kis_wrapper_module, "_price_cache", fake_outer)
    monkeypatch.setenv("KR_PB1_PRETRADE_QUOTE_MAX_AGE_SEC", "5")
    monkeypatch.setenv("KIS_WS_FRESH_MAX_AGE_SEC_KR", "5")
    monkeypatch.setenv("KIS_WS_INITIAL_WAIT_SEC", "0")
    yield fake_outer
    price_cache.cache.clear()


def _set_ws(monkeypatch, quote=None) -> FakeWsService:
    service = FakeWsService(quote)
    monkeypatch.setattr(fix.kis_wrapper_module, "get_kis_ws_price_service", lambda: service)
    return service


def _put_inner_rest(code: str, quote: dict, *, source_age_sec: float) -> None:
    source_ts = time.time() - float(source_age_sec)
    expires_at = source_ts + float(PRICE_SNAPSHOT_TTL_SEC)
    price_cache.cache[("inquire-price", code)] = (dict(quote), expires_at)


def test_strict_ws_uses_exact_pb1_max_age_and_rechecks_received_at(monkeypatch):
    monkeypatch.setenv("KR_PB1_PRETRADE_QUOTE_MAX_AGE_SEC", "1")
    monkeypatch.setenv("KIS_WS_FRESH_MAX_AGE_SEC_KR", "5")
    now = time.time()
    service = _set_ws(
        monkeypatch,
        {"prpr": 10000.0, "received_at": now - 0.4, "age_sec": 0.4, "source": "KIS_WEBSOCKET"},
    )
    kis = FakeRestKis({"prpr": 9999.0})

    quote, reason = fix._resolve_authoritative_pretrade_quote_strict(kis, "078340")

    assert reason == "ok"
    assert quote is not None
    assert quote["_source_kind"] == "KIS_WEBSOCKET"
    assert quote["_source_age_sec"] < 1.0
    assert service.get_max_ages == [1.0]
    assert kis.calls == 0


def test_ws_return_older_than_strict_contract_cannot_authorize_buy(monkeypatch):
    monkeypatch.setenv("KR_PB1_PRETRADE_QUOTE_MAX_AGE_SEC", "1")
    now = time.time()
    service = _set_ws(
        monkeypatch,
        {"prpr": 10000.0, "received_at": now - 1.2, "age_sec": 1.2, "source": "KIS_WEBSOCKET"},
    )
    kis = FakeRestKis(None)

    quote, reason = fix._resolve_authoritative_pretrade_quote_strict(kis, "078340")

    assert quote is None
    assert reason.startswith("quote_stale:")
    assert service.get_max_ages == [1.0]
    assert kis.calls == 1


def test_ws_quote_without_source_time_fails_closed_when_rest_unavailable(monkeypatch):
    _set_ws(monkeypatch, {"prpr": 10000.0, "source": "KIS_WEBSOCKET"})
    kis = FakeRestKis(None)

    quote, reason = fix._resolve_authoritative_pretrade_quote_strict(kis, "078340")

    assert quote is None
    assert reason == "quote_freshness_unknown"


def test_double_cache_timestamp_reset_cannot_make_6_8_second_quote_fresh(monkeypatch, _freshness_state):
    """Reproduce: inner source age 1.9s -> outer re-cache -> 4.9s later = 6.8s source age."""
    _set_ws(monkeypatch, None)
    code = "078340"
    outer = _freshness_state
    # The outer row looks only 4.9s old and would pass the old PR149 5s check,
    # even though the market quote itself was acquired 6.8s ago.
    outer.cache[("J", code)] = SimpleNamespace(ts=time.time() - 4.9, data={"prpr": 10000.0})
    _put_inner_rest(code, {"prpr": 10000.0}, source_age_sec=6.8)
    kis = FakeRestKis(None)

    quote, _reason = fix._resolve_authoritative_pretrade_quote_strict(kis, code)

    assert quote is None
    assert ("J", code) not in outer.cache
    assert ("inquire-price", code) not in price_cache.cache
    assert kis.http_calls == 1


def test_inner_1_9_second_quote_is_refreshed_when_strict_contract_is_one_second(monkeypatch):
    monkeypatch.setenv("KR_PB1_PRETRADE_QUOTE_MAX_AGE_SEC", "1")
    _set_ws(monkeypatch, None)
    code = "078340"
    _put_inner_rest(code, {"prpr": 10000.0}, source_age_sec=1.9)
    kis = FakeRestKis({"prpr": 10010.0}, http_source_age_sec=0.0)

    quote, reason = fix._resolve_authoritative_pretrade_quote_strict(kis, code)

    assert reason == "ok"
    assert quote is not None
    assert quote["prpr"] == 10010.0
    assert quote["_source_kind"] == "KIS_REST"
    assert quote["_source_age_sec"] < 1.0
    assert kis.cache_hits == 0
    assert kis.http_calls == 1


def test_inner_1_9_second_quote_preserves_original_age_under_five_second_contract(monkeypatch):
    monkeypatch.setenv("KR_PB1_PRETRADE_QUOTE_MAX_AGE_SEC", "5")
    _set_ws(monkeypatch, None)
    code = "078340"
    _put_inner_rest(code, {"prpr": 10000.0}, source_age_sec=1.9)
    kis = FakeRestKis({"prpr": 20000.0})

    quote, reason = fix._resolve_authoritative_pretrade_quote_strict(kis, code)

    assert reason == "ok"
    assert quote is not None
    assert quote["prpr"] == 10000.0
    assert 1.5 <= quote["_source_age_sec"] <= 2.5
    assert kis.cache_hits == 1
    assert kis.http_calls == 0


def test_rest_quote_without_inner_acquisition_timestamp_is_not_trusted(monkeypatch):
    _set_ws(monkeypatch, None)
    kis = FakeRestKis({"prpr": 10000.0}, cache_result=False)

    quote, reason = fix._resolve_authoritative_pretrade_quote_strict(kis, "078340")

    assert quote is None
    assert reason == "quote_freshness_unknown"


def test_review_installer_replaces_only_sep30_quote_resolver(monkeypatch):
    original = base._resolve_authoritative_pretrade_quote
    old_installed = fix._INSTALLED
    old_marker = getattr(base, "_kr_p1_20260930_source_time_freshness_installed", None)
    try:
        fix._INSTALLED = False
        fix.install_kr_20260930_freshness_review()
        assert base._resolve_authoritative_pretrade_quote is fix._resolve_authoritative_pretrade_quote_strict
        assert base._kr_p1_20260930_source_time_freshness_installed is True
    finally:
        base._resolve_authoritative_pretrade_quote = original
        fix._INSTALLED = old_installed
        if old_marker is None:
            try:
                delattr(base, "_kr_p1_20260930_source_time_freshness_installed")
            except AttributeError:
                pass
        else:
            base._kr_p1_20260930_source_time_freshness_installed = old_marker
