from __future__ import annotations

import os
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from trader.kr.market_scope import is_kr_market


def _set_us(monkeypatch):
    for key in ("MARKET", "REGION", "TRADING_REGION", "PB1_MARKET_SCOPE", "WSL_RUN_MARKET"):
        monkeypatch.setenv(key, "US")
    monkeypatch.setenv("EXCHANGE", "NYSE")
    monkeypatch.delenv("KR_TRADE_DATE", raising=False)
    monkeypatch.delenv("KR_EXPECTED_AS_OF", raising=False)


def test_us_market_scope_is_not_kr(monkeypatch):
    _set_us(monkeypatch)
    assert is_kr_market() is False


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("MARKET", "US"),
        ("REGION", "US"),
        ("TRADING_REGION", "US"),
        ("PB1_MARKET_SCOPE", "US"),
        ("EXCHANGE", "NYSE"),
        ("EXCHANGE", "NASDAQ"),
    ],
)
def test_us_scope_individual_values_are_not_kr(monkeypatch, key, value):
    for env_key in ("MARKET", "REGION", "TRADING_REGION", "PB1_MARKET_SCOPE", "WSL_RUN_MARKET", "EXCHANGE"):
        monkeypatch.delenv(env_key, raising=False)
    monkeypatch.setenv(key, value)
    assert is_kr_market() is False


@pytest.mark.parametrize(
    ("key", "value"),
    [("MARKET", "KR"), ("EXCHANGE", "KRX"), ("PB1_MARKET_SCOPE", "KRX")],
)
def test_kr_scope_values_are_kr(monkeypatch, key, value):
    for env_key in ("MARKET", "REGION", "TRADING_REGION", "PB1_MARKET_SCOPE", "WSL_RUN_MARKET", "EXCHANGE"):
        monkeypatch.delenv(env_key, raising=False)
    monkeypatch.setenv(key, value)
    assert is_kr_market() is True


def test_us_prep_date_context_does_not_call_kr_calendar_or_artifacts(monkeypatch):
    import trader.prep_runner as prep_runner

    _set_us(monkeypatch)
    called = {"kr_calendar": False, "kr_quarantine": False, "kr_publish": False}

    def fake_resolve_kr_trade_date(*args, **kwargs):
        called["kr_calendar"] = True
        raise AssertionError("KR calendar must not be called for US prep")

    def fake_quarantine(*args, **kwargs):
        called["kr_quarantine"] = True
        raise AssertionError("KR quarantine must not be called for US prep")

    def fake_publish(*args, **kwargs):
        called["kr_publish"] = True
        raise AssertionError("KR publish must not be called for US prep")

    monkeypatch.setattr(prep_runner, "resolve_kr_trade_date", fake_resolve_kr_trade_date)
    monkeypatch.setattr(prep_runner, "quarantine_stale_kr_artifacts", fake_quarantine)
    monkeypatch.setattr(prep_runner, "publish_kr_prep_artifacts_atomic", fake_publish)
    monkeypatch.setattr(prep_runner, "_pick_as_of_date_always_prev", lambda: datetime(2026, 6, 16, tzinfo=ZoneInfo("Asia/Seoul")).date())

    ctx = prep_runner.resolve_prep_date_context(datetime(2026, 6, 17, tzinfo=ZoneInfo("Asia/Seoul")))
    assert ctx["kr_market"] is False
    assert called == {"kr_calendar": False, "kr_quarantine": False, "kr_publish": False}
    assert "KR_TRADE_DATE" not in os.environ
    assert "KR_EXPECTED_AS_OF" not in os.environ


def test_kr_prep_date_context_calls_kr_calendar(monkeypatch):
    import trader.prep_runner as prep_runner

    monkeypatch.setenv("MARKET", "KR")
    monkeypatch.setenv("PB1_MARKET_SCOPE", "KRX")
    called = {"trade": 0, "asof": 0}
    run_date = datetime(2026, 6, 17, tzinfo=ZoneInfo("Asia/Seoul")).date()
    expected = datetime(2026, 6, 16, tzinfo=ZoneInfo("Asia/Seoul")).date()

    monkeypatch.setattr(prep_runner, "resolve_kr_trade_date", lambda _ts: called.__setitem__("trade", called["trade"] + 1) or run_date)
    monkeypatch.setattr(prep_runner, "resolve_kr_expected_as_of", lambda _td: called.__setitem__("asof", called["asof"] + 1) or expected)

    ctx = prep_runner.resolve_prep_date_context(datetime(2026, 6, 17, tzinfo=ZoneInfo("Asia/Seoul")))
    assert ctx["kr_market"] is True
    assert called == {"trade": 1, "asof": 1}
    assert os.environ["KR_TRADE_DATE"] == run_date.isoformat()
    assert os.environ["KR_EXPECTED_AS_OF"] == expected.isoformat()
