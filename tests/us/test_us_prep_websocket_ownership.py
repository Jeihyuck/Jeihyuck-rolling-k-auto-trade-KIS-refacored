import asyncio
import sys
import time
from pathlib import Path
from types import SimpleNamespace


def test_prep_wrapper_disables_realtime_websocket_after_env_load():
    src = Path("scripts/wsl/run-us-prep.sh").read_text(encoding="utf-8")
    env_load = src.index("source .env")
    ws_disable = src.index('export KIS_WS_PRICE_ENABLED="0"')
    dispatcher = src.index("-m trader.us.runner.dispatcher --mode prep")
    assert env_load < ws_disable < dispatcher
    assert "[US_PREP][WS_OWNERSHIP] websocket_enabled=0 owner=TRADE_ONLY" in src


def test_trade_wrappers_do_not_disable_websocket():
    for path in (
        "scripts/wsl/run-us-am.sh",
        "scripts/wsl/run-us-afternoon.sh",
        "scripts/wsl/run-us-close.sh",
    ):
        src = Path(path).read_text(encoding="utf-8")
        assert 'export KIS_WS_PRICE_ENABLED="0"' not in src


def test_disabled_websocket_cannot_be_force_enabled(monkeypatch):
    from trader.marketdata.kis_ws_price import KisWebSocketPriceService

    monkeypatch.setenv("KIS_WS_PRICE_ENABLED", "0")
    monkeypatch.setenv("KIS_WS_PRICE_FORCE_ENABLE", "1")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    svc = KisWebSocketPriceService()
    assert svc.enabled() is False
    assert svc.network_allowed("US") is False


def test_websocket_adapter_reconnects_and_only_serves_fresh_quotes(monkeypatch):
    from trader.marketdata import kis_ws_price

    monkeypatch.setenv("KIS_WS_PRICE_ENABLED", "1")
    monkeypatch.setenv("KIS_WS_PRICE_TEST_ENABLE", "1")
    monkeypatch.setenv("US_KIS_HTTP_ENABLED", "1")
    service = kis_ws_price.KisWebSocketPriceService()
    service._approval_key = lambda: "synthesized-approval-key"
    connection_attempts = []

    class WebSocket:
        async def recv(self):
            raise ConnectionError("synthesized disconnect")

    class Connection:
        async def __aenter__(self):
            return WebSocket()

        async def __aexit__(self, *_args):
            return False

    def connect(*_args, **_kwargs):
        connection_attempts.append(True)
        return Connection()

    monkeypatch.setitem(sys.modules, "websockets", SimpleNamespace(connect=connect))
    sleep_calls = []
    real_async_sleep = asyncio.sleep

    async def stop_after_reconnect(_seconds):
        sleep_calls.append(True)
        if len(sleep_calls) == 2:
            service._stop.set()
        await real_async_sleep(0)

    monkeypatch.setattr(kis_ws_price.asyncio, "sleep", stop_after_reconnect)
    asyncio.run(service._run_forever())

    assert len(connection_attempts) == 2
    assert service.stats()["connections"] == 2
    assert service.stats()["reconnects"] == 1
    assert service.stats()["connected"] is False

    service.put_quote(
        market="US", symbol="FIXTURE", exchange="NASDAQ", last=42.0,
        received_at=time.time() - 60,
    )
    assert service.get_fresh_quote("US", "FIXTURE", max_age_sec=5) is None
    from trader.us.runner.trade_tick_runner import _get_tqqq_tick_quote

    class Provider:
        def get_current_price(self, symbol, exchange):
            return service.get_fresh_quote("US", symbol, max_age_sec=5) or {
                "last": 42.0,
                "source": "KIS_WEBSOCKET",
                "stale": True,
            }

    provider = Provider()
    stale_price, stale_source, stale = _get_tqqq_tick_quote(provider)
    assert (stale_price, stale_source, stale) == (42.0, "KIS_WEBSOCKET", True)

    from tests.us.test_us_sep29_tqqq_owner_runtime_gate import (
        _FakeRepository,
        _overlay,
        _sep29_state,
    )
    from trader.us.infinite.integration import run_sleeve
    from datetime import date

    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")
    position = {
        "symbol": "TQQQ",
        "qty": 6,
        "orderable_qty": 6,
        "avg_price": 79.664,
        "strategy_owner": "TQQQ_INFINITE",
    }
    routed = []
    blocked = run_sleeve(
        positions=[position],
        price=stale_price,
        trading_date=date(2026, 9, 29),
        overlay=_overlay(tqqq_quote_stale=stale),
        repository=_FakeRepository(_sep29_state()),
        route=lambda intent: (routed.append(intent) or {"status": "ACK"}),
    )
    assert blocked["status"] == "BLOCK"
    assert blocked["reason"] == "tqqq_quote_invalid"
    assert routed == []

    fields = ["0"] * 25
    fields[0] = "TQQQ"
    fields[10] = "42.5"
    fields[14] = "42.4"
    fields[15] = "42.6"
    asyncio.run(service._handle_message(None, f"0|{service.US_TR_ID}|1|{'^'.join(fields)}"))
    fresh = service.get_fresh_quote("US", "TQQQ", max_age_sec=5)
    assert fresh is not None
    assert fresh["last"] == 42.5
    fresh_price, fresh_source, fresh_stale = _get_tqqq_tick_quote(provider)
    assert (fresh_price, fresh_source, fresh_stale) == (42.5, "KIS_WEBSOCKET", False)
    recovered = run_sleeve(
        positions=[position],
        price=fresh_price,
        trading_date=date(2026, 9, 29),
        overlay=_overlay(tqqq_quote_stale=fresh_stale),
        repository=_FakeRepository(_sep29_state()),
        route=lambda intent: (routed.append(intent) or {"status": "ACK"}),
    )
    assert recovered["status"] == "ACK"
    assert len(routed) == 1
    assert routed[0]["side"] == "BUY"
