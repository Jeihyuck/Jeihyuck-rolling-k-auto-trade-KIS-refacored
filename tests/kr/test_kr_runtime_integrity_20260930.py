from __future__ import annotations

import os
import threading
import time
from types import SimpleNamespace

import pytest

from trader.cache_ttl import PRICE_SNAPSHOT_TTL_SEC, price_cache
from trader.kr import runtime_integrity_20260930 as fix
from trader.universe.validation import validate_tradeable_quote


class FakeKis:
    def __init__(self, quote=None):
        self.quote = quote if quote is not None else {"prpr": 10000.0, "ask": 10010.0}
        self.snapshot_calls = 0
        self.rest_calls = 0
        self.safe_calls = 0

    def get_price_snapshot(self, code, market="J"):
        # The final Sep-30 source-time review intentionally bypasses this outer
        # cache path for strict BUY freshness. Keep the method only to prove it
        # is not used by the final production contract.
        self.snapshot_calls += 1
        return dict(self.quote)

    def get_price_quote(self, code, *, diag_mode=False, attempts=2):
        self.rest_calls += 1
        quote = dict(self.quote)
        # Production get_price_quote writes the direct REST result into the
        # inner two-second cache. The freshness review recovers source time as
        # expires_at - PRICE_SNAPSHOT_TTL_SEC, so mirror that contract here.
        price_cache.cache[("inquire-price", str(code).zfill(6))] = (
            quote,
            time.time() + float(PRICE_SNAPSHOT_TTL_SEC),
        )
        return quote

    def get_quote_safe(self, *args, **kwargs):
        self.safe_calls += 1
        raise AssertionError("PR140 legacy independent pretrade quote path must not be used")


class FakeEngine:
    def __init__(self, kis):
        self.kis = kis
        self.ledger = []
        self.calc_calls = 0

    def _order_precheck_gate_reasons(self, *, side, stage):
        return []

    def _display_code(self, code):
        return str(code)

    def _append_ledger_event(self, **payload):
        self.ledger.append(payload)

    def _calc_order_price(self, code, quote, daily_close):
        self.calc_calls += 1
        raise AssertionError("pretrade must not rewrite durable order economics")


@pytest.fixture(autouse=True)
def _clear_inner_price_cache():
    price_cache.cache.clear()
    yield
    price_cache.cache.clear()


def _never_original(*args, **kwargs):
    raise AssertionError("original BUY pretrade must not reacquire a quote")


def _call_pretrade(guard, engine, *, code="078340", side="BUY", price=10000.0):
    return guard(
        engine,
        code=code,
        market="KOSDAQ",
        mode=1,
        side=side,
        qty=10,
        price=price,
        client_order_key="test-key",
        stage="PB1-AM",
    )


def test_tradeable_quote_validation_is_pure_and_preserves_halt_guard():
    assert validate_tradeable_quote({"prpr": 10000.0}) == (True, "ok")
    assert validate_tradeable_quote({"prpr": 10000.0, "halted": "Y"}) == (False, "halted:halted")
    assert validate_tradeable_quote({"rt_cd": "1", "prpr": 10000.0}) == (False, "rt_cd_1")
    assert validate_tradeable_quote({}) == (False, "price_unavailable")


def test_pb1_buy_pretrade_uses_source_time_proven_path_and_never_get_quote_safe(monkeypatch):
    monkeypatch.setenv("KR_PB1_PRETRADE_QUOTE_MAX_AGE_SEC", "5")
    kis = FakeKis()
    engine = FakeEngine(kis)
    guard = fix._build_pb1_pretrade_canonical_quote_guard(_never_original)

    assert _call_pretrade(guard, engine) is True
    # The final review no longer trusts get_price_snapshot's outer cache time.
    assert kis.snapshot_calls == 0
    assert kis.rest_calls == 1
    assert kis.safe_calls == 0
    assert engine.calc_calls == 0


def test_pretrade_evicts_only_rest_cache_older_than_five_seconds(monkeypatch):
    monkeypatch.setenv("KR_PB1_PRETRADE_QUOTE_MAX_AGE_SEC", "5")
    key = ("J", "078340")
    fake_cache = SimpleNamespace(
        cache={key: SimpleNamespace(ts=time.time() - 10.0, data={"prpr": 10000.0})},
        lock=threading.Lock(),
    )
    monkeypatch.setattr(fix.kis_wrapper_module, "_price_cache", fake_cache)

    age = fix._evict_stale_canonical_rest_cache("078340")

    assert age is not None and age >= 9.0
    assert key not in fake_cache.cache


def test_pretrade_keeps_rest_cache_inside_five_second_freshness(monkeypatch):
    monkeypatch.setenv("KR_PB1_PRETRADE_QUOTE_MAX_AGE_SEC", "5")
    key = ("J", "078340")
    row = SimpleNamespace(ts=time.time() - 1.0, data={"prpr": 10000.0})
    fake_cache = SimpleNamespace(cache={key: row}, lock=threading.Lock())
    monkeypatch.setattr(fix.kis_wrapper_module, "_price_cache", fake_cache)

    age = fix._evict_stale_canonical_rest_cache("078340")

    assert age is not None and age < 5.0
    assert fake_cache.cache[key] is row


def test_missing_canonical_price_fails_closed_and_writes_retryable_skip(monkeypatch):
    monkeypatch.setenv("KR_PB1_PRETRADE_QUOTE_MAX_AGE_SEC", "5")
    # A source-time-proven REST response with no positive price must still be
    # classified as price_unavailable and fail closed before broker submit.
    kis = FakeKis({"rt_cd": "0"})
    engine = FakeEngine(kis)
    guard = fix._build_pb1_pretrade_canonical_quote_guard(_never_original)

    assert _call_pretrade(guard, engine) is False
    assert kis.snapshot_calls == 0
    assert kis.rest_calls == 1
    assert kis.safe_calls == 0
    assert engine.ledger
    assert engine.ledger[-1]["reasons"] == ["pretrade:price_unavailable"]


def test_pretrade_never_reprices_or_resizes_durable_buy_intent(monkeypatch):
    monkeypatch.setenv("KR_PB1_PRETRADE_QUOTE_MAX_AGE_SEC", "5")
    kis = FakeKis({"prpr": 9000.0, "ask": 9010.0})
    engine = FakeEngine(kis)
    guard = fix._build_pb1_pretrade_canonical_quote_guard(_never_original)

    assert _call_pretrade(guard, engine, price=10000.0) is True
    assert engine.calc_calls == 0
    assert kis.snapshot_calls == 0
    assert kis.rest_calls == 1
    assert kis.safe_calls == 0


def test_legacy_adapter_without_snapshot_method_delegates_existing_validator():
    class LegacyAdapter:
        pass

    called = []

    def original(self, **kwargs):
        called.append(kwargs)
        return True

    guard = fix._build_pb1_pretrade_canonical_quote_guard(original)
    engine = FakeEngine(LegacyAdapter())

    assert _call_pretrade(guard, engine) is True
    assert len(called) == 1
    assert called[0]["code"] == "078340"
    assert called[0]["side"] == "BUY"


def test_non_buy_and_kr_infinite_keep_existing_owner_paths():
    called = []

    def original(self, **kwargs):
        called.append(kwargs)
        return True

    guard = fix._build_pb1_pretrade_canonical_quote_guard(original)
    engine = FakeEngine(FakeKis())
    assert _call_pretrade(guard, engine, side="SELL") is True
    assert _call_pretrade(guard, engine, code="122630", side="BUY") is True
    assert len(called) == 2


def test_am_pm_budget_contract_upgrades_legacy_values_and_preserves_pr148_reserves(monkeypatch):
    monkeypatch.setenv("PB1_SESSION", "afternoon")
    monkeypatch.setenv("PB1_TICK_HARD_TIMEOUT_SEC", "90")
    monkeypatch.setenv("PB1_MIN_TICK_BUDGET_SEC", "105")
    monkeypatch.delenv("KR_POST_ENGINE_RESERVE_SEC", raising=False)
    monkeypatch.delenv("KR_ORDER_SUBMIT_MIN_REMAINING_SEC", raising=False)

    fix._apply_live_session_budget_contract()

    assert os.environ["PB1_TICK_HARD_TIMEOUT_SEC"] == "180"
    assert os.environ["PB1_MIN_TICK_BUDGET_SEC"] == "200"
    assert os.environ["KR_POST_ENGINE_RESERVE_SEC"] == "20"
    assert os.environ["KR_ORDER_SUBMIT_MIN_REMAINING_SEC"] == "20"


def test_explicit_operator_budget_override_is_preserved(monkeypatch):
    monkeypatch.setenv("PB1_SESSION", "am")
    monkeypatch.setenv("PB1_TICK_HARD_TIMEOUT_SEC", "210")
    monkeypatch.setenv("PB1_MIN_TICK_BUDGET_SEC", "230")
    monkeypatch.setenv("KR_POST_ENGINE_RESERVE_SEC", "25")
    monkeypatch.setenv("KR_ORDER_SUBMIT_MIN_REMAINING_SEC", "22")

    fix._apply_live_session_budget_contract()

    assert os.environ["PB1_TICK_HARD_TIMEOUT_SEC"] == "210"
    assert os.environ["PB1_MIN_TICK_BUDGET_SEC"] == "230"
    assert os.environ["KR_POST_ENGINE_RESERVE_SEC"] == "25"
    assert os.environ["KR_ORDER_SUBMIT_MIN_REMAINING_SEC"] == "22"


def test_close_budget_is_not_rewritten(monkeypatch):
    monkeypatch.setenv("PB1_SESSION", "close")
    monkeypatch.setenv("PB1_TICK_HARD_TIMEOUT_SEC", "45")
    monkeypatch.setenv("PB1_MIN_TICK_BUDGET_SEC", "30")

    fix._apply_live_session_budget_contract()

    assert os.environ["PB1_TICK_HARD_TIMEOUT_SEC"] == "45"
    assert os.environ["PB1_MIN_TICK_BUDGET_SEC"] == "30"
