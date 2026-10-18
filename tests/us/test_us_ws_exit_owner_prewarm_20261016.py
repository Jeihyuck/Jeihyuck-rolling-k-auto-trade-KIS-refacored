"""Read-only collector batch subscribe contract; no broker/order routes."""
from trader.marketdata import us_ws_collector as collector


class _Service:
    def __init__(self):
        self.calls = []

    def subscribe_us(self, symbol, exchange):
        self.calls.append((symbol, exchange))

    def get_fresh_quote(self, *args, **kwargs):
        raise AssertionError("subscription prewarm must not request quote/order truth")


def test_subscribe_many_command_is_strictly_read_only(monkeypatch):
    monkeypatch.setattr(collector, "_account_identity", lambda env: "test-scope")
    svc = _Service()
    response = collector._reply({
        "identity": "test-scope", "env": "practice",
        "cmd": "SUBSCRIBE_MANY",
        "pairs": [{"symbol": "AMD", "exchange": "NASDAQ"},
                  {"symbol": "KO", "exchange": "NYSE"}],
    }, env="practice", generation="generation-1", service=svc)
    assert response == {"status": "OK", "generation": "generation-1",
                        "subscribed": 2, "collector": "READ_ONLY"}
    assert svc.calls == [("AMD", "NASDAQ"), ("KO", "NYSE")]
    assert collector._reply({
        "identity": "wrong", "env": "practice", "cmd": "SUBSCRIBE_MANY",
        "pairs": [{"symbol": "AMD", "exchange": "NASDAQ"}],
    }, env="practice", generation="generation-1", service=svc) == {"status": "DENIED"}
    assert len(svc.calls) == 2


def test_client_warmup_is_bounded_and_does_not_need_quote(monkeypatch):
    monkeypatch.setenv("US_WS_COLLECTOR_ENABLED", "1")
    sent = []
    def fake_send(env, payload, *, timeout_sec):
        sent.append((env, payload, timeout_sec))
        return {"status": "OK", "generation": "G",
                "subscribed": len(payload["pairs"])}
    monkeypatch.setattr(collector, "_send_request", fake_send)
    pairs = [("amd", "nasdaq")] * 2 + [
        (f"S{i}", "NYSE") for i in range(30)
    ]
    assert collector.subscribe_many(pairs, env="practice")
    assert len(sent[0][1]["pairs"]) == 16
    assert sent[0][1]["pairs"][0] == {"symbol": "AMD", "exchange": "NASDAQ"}
    assert sent[0][2] <= 0.8
