import time
from unittest.mock import Mock

from trader.us.data_provider import USDataProvider
from trader.marketdata import us_ws_collector


def test_tp_strict_ws_age_falls_through_to_rest(monkeypatch):
    monkeypatch.setenv("US_WS_COLLECTOR_ENABLED", "1")
    monkeypatch.setenv("KIS_WS_FRESH_MAX_AGE_SEC_US", "15")
    collector = Mock(return_value={"last": 100, "received_at": time.time() - 10})
    monkeypatch.setattr(us_ws_collector, "get_quote", collector)
    provider = USDataProvider(offline=False, cache_enabled=False, env="practice")
    client = Mock()
    client.get_us_price.return_value = {
        "output": {"last": "101"}, "_quote_quality": "FRESH",
        "_quote_source": "KIS_LIVE", "_quote_asof_epoch": time.time(),
    }
    monkeypatch.setattr(provider, "_get_client", lambda: client)
    result = provider.get_current_price("PG", "NYSE", max_quote_age_sec=5)
    assert collector.call_args.kwargs["max_age_sec"] == 5
    client.get_us_price.assert_called_once_with("PG", "NYSE")
    assert result["last"] == "101"
    assert result["source"] == "KIS_LIVE"


def test_tp_fresh_collector_does_not_call_rest(monkeypatch):
    monkeypatch.setenv("US_WS_COLLECTOR_ENABLED", "1")
    monkeypatch.setattr(us_ws_collector, "get_quote", lambda *a, **k: {
        "last": 100, "received_at": time.time() - 1,
    })
    provider = USDataProvider(offline=False, cache_enabled=False, env="practice")
    client = Mock()
    monkeypatch.setattr(provider, "_get_client", lambda: client)
    result = provider.get_current_price("PG", "NYSE", max_quote_age_sec=5)
    assert result["source"] == "KIS_WEBSOCKET_COLLECTOR"
    client.get_us_price.assert_not_called()


def test_strict_quote_refresh_bypasses_old_tick_and_provider_cache(monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setenv("US_WS_COLLECTOR_ENABLED", "1")
    monkeypatch.setattr(us_ws_collector, "get_quote", lambda *a, **k: None)
    provider = USDataProvider(offline=False, cache_enabled=True, env="practice")
    provider._offline = False
    old = {"last": "98", "source": "KIS_CACHE", "quality": "fresh_cache",
           "asof_epoch": time.time() - 12}
    key = ("PG", "NYSE")
    provider._price_cache[key] = dict(old)
    ctx = SimpleNamespace(price_cache={key: dict(old)}, count=Mock(), metrics={})
    provider.bind_tick_context(ctx)
    client = Mock()
    client.get_us_price.return_value = {
        "output": {"last": "101"}, "_quote_quality": "FRESH",
        "_quote_source": "KIS_LIVE", "_quote_asof_epoch": time.time(),
    }
    monkeypatch.setattr(provider, "_get_client", lambda: client)
    result = provider.get_current_price("PG", "NYSE", max_quote_age_sec=5)
    assert result["last"] == "101"
    client.get_us_price.assert_called_once_with("PG", "NYSE")
    assert ctx.price_cache[key]["last"] == "101"


def test_native_rest_cache_does_not_override_protective_age(monkeypatch):
    from trader.us.execution.kis_us_client import KisUSClient
    monkeypatch.setenv("US_KIS_PRICE_CACHE_TTL_SEC", "20")
    client = object.__new__(KisUSClient)
    client._assert_not_offline = Mock()
    client._build_headers = Mock(return_value={})
    client._resolve_quote_excd = Mock(return_value="NYS")
    client._get = Mock(return_value={"rt_cd": "0", "output": {"last": "101"}})
    key = ("price", "PG", "NYS")
    client._response_cache = {key: (time.time()-12, {"output": {"last": "98"}})}
    client._temporary_error_streak = {}
    result = client.get_us_price("PG", "NYSE", max_quote_age_sec=5)
    assert result["output"]["last"] == "101"
    assert result["_quote_source"] == "KIS_LIVE"
    client._get.assert_called_once()
