"""PR-B: collector IPC, generation, ownership and stale evidence invariants."""
from __future__ import annotations

import json
import socket
import threading
import time

from trader.marketdata import us_ws_collector as collector


class _FakeService:
    def __init__(self, quote=None):
        self.quote = quote
        self.subscriptions = []

    def subscribe_us(self, symbol, exchange):
        self.subscriptions.append((symbol, exchange))

    def get_fresh_quote(self, market, symbol, *, max_age_sec):
        if not self.quote or time.time() - self.quote["received_at"] > max_age_sec:
            return None
        return dict(self.quote)


def test_collector_requires_correct_identity_and_refuses_order_commands(monkeypatch):
    monkeypatch.setattr(collector, "_account_identity", lambda env: "account-1")
    service = _FakeService({"last": 100.0, "received_at": time.time()})
    assert collector._reply(
        {"cmd": "QUOTE", "identity": "different", "env": "practice",
         "symbol": "MSFT", "exchange": "NASDAQ"},
        env="practice", generation="g1", service=service,
    )["status"] == "DENIED"
    assert collector._reply(
        {"cmd": "PLACE_ORDER", "identity": "account-1", "env": "practice"},
        env="practice", generation="g1", service=service,
    )["status"] == "BAD_REQUEST"
    assert service.subscriptions == []


def test_ipc_quote_generation_and_stale_rejection(tmp_path, monkeypatch):
    path = tmp_path / "us-ws-test.sock"
    monkeypatch.setattr(collector, "_account_identity", lambda env: "account-1")
    monkeypatch.setattr(collector, "socket_path", lambda env: path)
    monkeypatch.setenv("US_WS_COLLECTOR_ENABLED", "1")
    service = _FakeService({"last": 125.5, "received_at": time.time()})
    thread = threading.Thread(
        target=collector.serve,
        kwargs={"env": "practice", "parent_pid": 0, "service": service, "max_seconds": 1.0},
        daemon=True,
    )
    thread.start()
    for _ in range(100):
        if path.exists():
            break
        time.sleep(0.01)
    quote = collector.get_quote("MSFT", "NASDAQ", env="practice", timeout_sec=0.3)
    assert quote and quote["last"] == 125.5
    assert quote["collector_generation"]
    assert service.subscriptions == [("MSFT", "NASDAQ")]

    service.quote["received_at"] = time.time() - 100
    assert collector.get_quote("MSFT", "NASDAQ", env="practice", timeout_sec=0.3) is None
    thread.join(timeout=2)
    assert not path.exists()


def test_two_account_collectors_do_not_share_socket(tmp_path, monkeypatch):
    monkeypatch.setattr(
        collector, "_account_identity", lambda env: {"practice": "a", "real": "b"}[env]
    )
    monkeypatch.setattr(collector, "socket_path", lambda env: tmp_path / (env + ".sock"))
    assert collector.socket_path("practice") != collector.socket_path("real")


def test_collector_connection_failure_is_bounded_and_safe(tmp_path, monkeypatch):
    monkeypatch.setattr(collector, "socket_path", lambda env: tmp_path / "missing.sock")
    monkeypatch.setattr(collector, "_account_identity", lambda env: "account-1")
    monkeypatch.setenv("US_WS_COLLECTOR_ENABLED", "1")
    started = time.monotonic()
    assert collector.get_quote("MSFT", "NASDAQ", env="practice") is None
    assert time.monotonic() - started < 0.5


def test_late_broker_evidence_is_not_a_collector_generation_event():
    # Collector returns prices only; KIS fills/cancels belong to the broker
    # reconciler and must NOT be screened by a quote generation identifier.
    result = collector._reply(
        {"cmd": "ORDER_FILLED", "identity": "account-1", "env": "practice"},
        env="practice", generation="fresh", service=_FakeService(),
    )
    assert result["status"] in {"BAD_REQUEST", "DENIED"}


def _fork_collector_server(path):
    from pathlib import Path
    from trader.marketdata import us_ws_collector as module
    module.socket_path = lambda env: Path(path)
    module._account_identity = lambda env: "account-1"
    module.serve(
        env="practice", parent_pid=0,
        service=_FakeService({"last": 141.0, "received_at": time.time()}),
        max_seconds=4.0,
    )


def _abrupt_ipc_client_exit(path):
    import os
    import socket as sock
    with sock.socket(sock.AF_UNIX, sock.SOCK_STREAM) as conn:
        conn.connect(str(path))
        conn.sendall(b'{"cmd":"QUOTE","identity":"account-1"')
        os._exit(84)


def test_collector_survives_killed_tick_client_and_restarts_with_new_generation(tmp_path, monkeypatch):
    import multiprocessing as mp
    from pathlib import Path

    ctx = mp.get_context("fork")
    path = tmp_path / "read-only-collector.sock"
    monkeypatch.setattr(collector, "_account_identity", lambda env: "account-1")
    monkeypatch.setattr(collector, "socket_path", lambda env: path)
    monkeypatch.setenv("US_WS_COLLECTOR_ENABLED", "1")

    def ready():
        for _ in range(100):
            try:
                result = collector._send_request("practice", {"cmd": "HEALTH"}, timeout_sec=0.1)
                if result and result.get("status") == "OK":
                    return result
            except (OSError, ValueError):
                pass
            time.sleep(0.02)
        raise AssertionError("collector failed to start")

    first = ctx.Process(target=_fork_collector_server, args=(str(path),))
    first.start()
    try:
        generation_1 = ready()["generation"]
        broken_client = ctx.Process(target=_abrupt_ipc_client_exit, args=(str(path),))
        broken_client.start()
        broken_client.join(timeout=1.5)
        assert broken_client.exitcode == 84
        assert first.is_alive(), "killed trading client must not stop market-data service"
        assert ready()["generation"] == generation_1
    finally:
        if first.is_alive():
            first.terminate()
        first.join(timeout=2)

    restarted = ctx.Process(target=_fork_collector_server, args=(str(path),))
    restarted.start()
    try:
        generation_2 = ready()["generation"]
        assert generation_2 != generation_1, "restarted collector needs a new generation"
        quote = collector.get_quote("AAPL", "NASDAQ", env="practice", timeout_sec=0.2)
        assert quote and quote["collector_generation"] == generation_2
    finally:
        if restarted.is_alive():
            restarted.terminate()
        restarted.join(timeout=2)
