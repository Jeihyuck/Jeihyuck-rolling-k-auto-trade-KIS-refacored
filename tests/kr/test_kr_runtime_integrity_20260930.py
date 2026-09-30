from __future__ import annotations

import os
import time

from trader.kr import runtime_integrity_20260930 as fix
from trader.universe.validation import validate_tradeable_quote


class FakeKis:
    def __init__(self, quote=None):
        self.quote = quote if quote is not None else {"prpr": 10000.0, "ask": 10010.0}
        self.snapshot_calls = 0
        self.safe_calls = 0

    def get_price_snapshot(self, code, market="J"):
        self.snapshot_calls += 1
        return dict(self.quote)

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


def test_fresh_shared_snapshot_reused_without_second_quote_request(monkeypatch):
    monkeypatch.setenv("KR_PB1_PRETRADE_QUOTE_MAX_AGE_SEC", "5")
    kis = FakeKis()
    fix._remember_quote(kis, "078340", {"prpr": 10000.0, "ask": 10010.0}, source="get_price_snapshot")
    engine = FakeEngine(kis)
    guard = fix._build_pb1_pretrade_shared_quote_guard(_never_original)

    assert _call_pretrade(guard, engine) is True
    assert kis.snapshot_calls == 0
    assert kis.safe_calls == 0
    assert engine.calc_calls == 0


def test_stale_shared_snapshot_refreshes_once_via_canonical_snapshot_not_get_quote_safe(monkeypatch):
    monkeypatch.setenv("KR_PB1_PRETRADE_QUOTE_MAX_AGE_SEC", "0.1")
    kis = FakeKis({"prpr": 9900.0, "ask": 9910.0})
    fix._quote_cache(kis)["078340"] = {
        "quote": {"prpr": 10000.0, "ask": 10010.0},
        "captured_monotonic": time.monotonic() - 1.0,
        "source": "get_price_snapshot",
    }
    engine = FakeEngine(kis)
    guard = fix._build_pb1_pretrade_shared_quote_guard(_never_original)

    assert _call_pretrade(guard, engine) is True
    assert kis.snapshot_calls == 1
    assert kis.safe_calls == 0
    assert engine.calc_calls == 0


def test_missing_refreshed_price_fails_closed_and_writes_retryable_skip(monkeypatch):
    monkeypatch.setenv("KR_PB1_PRETRADE_QUOTE_MAX_AGE_SEC", "0.1")
    kis = FakeKis({})
    engine = FakeEngine(kis)
    guard = fix._build_pb1_pretrade_shared_quote_guard(_never_original)

    assert _call_pretrade(guard, engine) is False
    assert kis.snapshot_calls == 1
    assert kis.safe_calls == 0
    assert engine.ledger
    assert engine.ledger[-1]["reasons"] == ["pretrade:price_unavailable"]


def test_pretrade_never_reprices_or_resizes_durable_buy_intent(monkeypatch):
    monkeypatch.setenv("KR_PB1_PRETRADE_QUOTE_MAX_AGE_SEC", "5")
    kis = FakeKis({"prpr": 9000.0, "ask": 9010.0})
    fix._remember_quote(kis, "078340", kis.quote, source="get_price_snapshot")
    engine = FakeEngine(kis)
    guard = fix._build_pb1_pretrade_shared_quote_guard(_never_original)

    assert _call_pretrade(guard, engine, price=10000.0) is True
    assert engine.calc_calls == 0
    assert kis.safe_calls == 0


def test_non_buy_and_kr_infinite_keep_existing_owner_paths():
    called = []

    def original(self, **kwargs):
        called.append(kwargs)
        return True

    guard = fix._build_pb1_pretrade_shared_quote_guard(original)
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
