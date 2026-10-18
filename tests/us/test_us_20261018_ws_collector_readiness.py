"""Offline acceptance for isolated US WS collector and REST-safe fallback."""
import os
import time

import pytest

from trader.marketdata import us_ws_collector as collector
from trader.marketdata.kis_ws_price import KisWebSocketPriceService


def _ws_env(monkeypatch):
    monkeypatch.setenv("KIS_WS_PRICE_TEST_ENABLE", "1")
    monkeypatch.setenv("KIS_WS_PRICE_ENABLED", "1")
    monkeypatch.setenv("MINERVINI_ONLY", "0")
    monkeypatch.setenv("US_WS_COLLECTOR_ENABLED", "1")
    monkeypatch.setenv("US_KIS_HTTP_ENABLED", "AUTO")
    monkeypatch.delenv("STRATEGY_MODE", raising=False)
    monkeypatch.delenv("KIS_EXPLICIT_OFFLINE", raising=False)
    monkeypatch.delenv("DIAG_KIS_CALLS_ENABLED", raising=False)
    monkeypatch.delenv("KIS_WS_PRICE_FORCE_ENABLE", raising=False)
    monkeypatch.delenv("FORCE_HTTP", raising=False)
    monkeypatch.setenv("US_WS_COLLECTOR_ROLE", "READ_ONLY")


def test_readonly_collector_can_subscribe_in_auto_even_without_live_strategy(monkeypatch):
    _ws_env(monkeypatch)
    service = KisWebSocketPriceService()
    monkeypatch.setattr(service, "_ensure_started", lambda: None)
    assert service.network_allowed("US")
    assert service.subscribe_us("META", "NASDAQ") is True
    assert service.stats()["subscriptions"] == 1
    assert service.stats()["quotes_received"] is False
    assert not service.stats()["connected"]


@pytest.mark.parametrize("stop", [
    {"KIS_EXPLICIT_OFFLINE": "1"},
    {"US_KIS_HTTP_ENABLED": "OFF"},
    {"DIAG_KIS_CALLS_ENABLED": "0"},
    {"US_WS_COLLECTOR_ENABLED": "0"},
])
def test_explicit_network_stops_remain_fail_closed(monkeypatch, stop):
    _ws_env(monkeypatch)
    for name, value in stop.items():
        monkeypatch.setenv(name, value)
    service = KisWebSocketPriceService()
    assert not service.network_allowed("US")
    assert service.subscribe_us("META", "NASDAQ") is False


def test_role_not_in_parent_does_not_enable_strategy_network(monkeypatch):
    _ws_env(monkeypatch)
    monkeypatch.delenv("US_WS_COLLECTOR_ROLE", raising=False)
    service = KisWebSocketPriceService()
    assert not service.network_allowed("US")


class Fake:
    def __init__(self, *, allowed=True, connected=False, quote=None):
        self.allowed, self.connected, self.quote = allowed, connected, quote
        self.pairs = []

    def subscribe_us(self, symbol, exchange):
        if not self.allowed:
            return False
        self.pairs.append((symbol, exchange))
        return True

    def get_fresh_quote(self, market, symbol, *, max_age_sec):
        q = self.quote
        if q and time.time() - q["received_at"] <= max_age_sec:
            return q
        return None

    def stats(self):
        return {
            "connected": self.connected, "subscriptions": len(self.pairs),
            "quotes": 1 if self.quote else 0,
            "quotes_received": self.quote is not None,
            "last_received_age_sec": (
                time.time() - self.quote["received_at"] if self.quote else None
            ),
        }


def _req(cmd, **kwargs):
    return {"cmd": cmd, "identity": "test-scope", "env": "practice", **kwargs}


def test_health_stages_do_not_claim_ws_price_on_process_start(monkeypatch):
    monkeypatch.setattr(collector, "_account_identity", lambda env: "test-scope")
    fake = Fake()
    process = collector._reply(_req("HEALTH"), env="practice", generation="g", service=fake)
    assert process["status"] == "OK"
    assert process["stage"] == "PROCESS_READY_WS_CONNECTING"
    fake.connected = True
    connected = collector._reply(_req("HEALTH"), env="practice", generation="g", service=fake)
    assert connected["stage"] == "CONNECTED_AWAITING_QUOTE"
    fake.quote = {"last": 101.0, "received_at": time.time()}
    receiving = collector._reply(_req("HEALTH"), env="practice", generation="g", service=fake)
    assert receiving["stage"] == "PRICE_RECEIVED"
    assert receiving["received_symbols"] == 1


def test_denied_subscription_not_reported_success(monkeypatch):
    monkeypatch.setattr(collector, "_account_identity", lambda env: "test-scope")
    fake = Fake(allowed=False)
    response = collector._reply(_req("SUBSCRIBE_MANY",
        pairs=[{"symbol":"META", "exchange":"NASDAQ"}]),
        env="practice", generation="g", service=fake)
    assert response["status"] == "SUBSCRIPTION_DISABLED"
    assert response["subscribed"] == 0
    assert fake.pairs == []
    quote = collector._reply(_req("QUOTE", symbol="META", exchange="NASDAQ"),
                             env="practice", generation="g", service=fake)
    assert quote["status"] == "MISS"
    assert quote["reason"] == "SUBSCRIPTION_DISABLED"


def test_no_fresh_quote_after_disconnect(monkeypatch):
    _ws_env(monkeypatch)
    service = KisWebSocketPriceService()
    monkeypatch.setattr(service, "_ensure_started", lambda: None)
    service.put_quote(market="US",symbol="META",last=100.0,received_at=time.time()-50)
    assert service.get_fresh_quote("US","META",max_age_sec=10) is None


def test_launcher_sets_read_only_role_in_child_not_parent(monkeypatch):
    _ws_env(monkeypatch)
    monkeypatch.delenv("US_WS_COLLECTOR_ROLE", raising=False)
    monkeypatch.setattr(collector, "_send_request", lambda *a, **k: None)
    monkeypatch.setattr(collector, "_LAST_START_REQUEST", None)
    parent_role = os.getenv("US_WS_COLLECTOR_ROLE")
    calls = []

    class P:
        pid = 44
        def poll(self):
            return None

    def fake_spawn(*args, **kwargs):
        calls.append(kwargs)
        return P()

    monkeypatch.setattr(collector.subprocess, "Popen", fake_spawn)
    collector.ensure_started(env="practice")
    assert calls[0]["env"]["US_WS_COLLECTOR_ROLE"] == "READ_ONLY"
    assert os.getenv("US_WS_COLLECTOR_ROLE") == parent_role
