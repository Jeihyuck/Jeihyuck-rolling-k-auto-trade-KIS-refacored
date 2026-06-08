# -*- coding: utf-8 -*-
"""Offline QA guard: KIS HTTP must never be called when offline flags are set."""
from __future__ import annotations

from pathlib import Path

import pytest


def _set_offline_http_env(monkeypatch, audit_file: Path) -> None:
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("OFFLINE_MODE", "1")
    monkeypatch.setenv("US_OFFLINE_MODE", "true")
    monkeypatch.setenv("ALLOW_REAL_ORDER", "0")
    monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1")
    monkeypatch.setenv("KIS_HTTP_BLOCK", "1")
    monkeypatch.setenv("KIS_HTTP_AUDIT_FILE", str(audit_file))


def test_kis_client_blocks_token_http_under_offline_env(monkeypatch, tmp_path):
    audit_file = tmp_path / "kis_http_calls.log"
    _set_offline_http_env(monkeypatch, audit_file)

    import requests
    from trader.us.execution.kis_us_client import KisUSClient

    def _fail_post(*args, **kwargs):  # pragma: no cover - should never execute
        raise AssertionError("requests.post must not be called in offline QA")

    monkeypatch.setattr(requests, "post", _fail_post)

    client = KisUSClient(env="practice", offline=False)
    with pytest.raises(RuntimeError, match="OFFLINE_BLOCK"):
        client.get_access_token()

    assert not audit_file.exists(), "blocked offline calls must not reach HTTP audit marker"


def test_kis_client_blocks_quote_balance_order_http_under_offline_env(monkeypatch, tmp_path):
    audit_file = tmp_path / "kis_http_calls.log"
    _set_offline_http_env(monkeypatch, audit_file)

    import requests
    from trader.us.execution.kis_us_client import KisUSClient

    def _fail_get(*args, **kwargs):  # pragma: no cover - should never execute
        raise AssertionError("requests.get must not be called in offline QA")

    def _fail_post(*args, **kwargs):  # pragma: no cover - should never execute
        raise AssertionError("requests.post must not be called in offline QA")

    monkeypatch.setattr(requests, "get", _fail_get)
    monkeypatch.setattr(requests, "post", _fail_post)

    client = KisUSClient(env="practice", offline=False)
    blocked_calls = [
        lambda: client.get_us_price("AAPL", "NASDAQ"),
        lambda: client.get_us_daily_price("AAPL", "NASDAQ"),
        lambda: client.get_us_balance(),
        lambda: client.place_us_buy_order("AAPL", "NASDAQ", 1, 100.0),
        lambda: client.place_us_sell_order("AAPL", "NASDAQ", 1, 100.0),
    ]

    for call in blocked_calls:
        with pytest.raises(RuntimeError, match="OFFLINE_BLOCK"):
            call()

    assert not audit_file.exists(), "no KIS HTTP call marker should be written"


def test_us_data_provider_treats_offline_env_as_offline(monkeypatch, tmp_path):
    audit_file = tmp_path / "kis_http_calls.log"
    _set_offline_http_env(monkeypatch, audit_file)

    import requests
    from trader.us.data_provider import USDataProvider

    def _fail_get(*args, **kwargs):  # pragma: no cover - should never execute
        raise AssertionError("requests.get must not be called through data provider in offline QA")

    monkeypatch.setattr(requests, "get", _fail_get)

    provider = USDataProvider(offline=False, cache_enabled=True)
    price = provider.get_current_price("NVDA", "NASDAQ")
    daily = provider.get_daily_prices("NVDA", "NASDAQ", count=3)
    balance = provider.get_balance()

    assert price["symbol"] == "NVDA"
    assert len(daily) == 3
    assert balance["positions"] == []
    assert not audit_file.exists()
