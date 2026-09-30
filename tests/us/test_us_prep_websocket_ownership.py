from pathlib import Path


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
