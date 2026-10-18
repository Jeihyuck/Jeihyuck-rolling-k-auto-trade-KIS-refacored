"""Owner-first KR WebSocket priority within the configured subscription cap.

This test never opens a socket or calls KIS.
"""
from trader.marketdata.kis_ws_price import KisWebSocketPriceService


def _offline_service(monkeypatch):
    service = KisWebSocketPriceService()
    monkeypatch.setattr(service, "network_allowed", lambda market: True)
    monkeypatch.setattr(service, "_ensure_started", lambda: None)
    return service


def test_owner_priorities_reorder_earlier_candidate_slots(monkeypatch):
    service = _offline_service(monkeypatch)
    for i in range(40):
        service.subscribe_kr(f"{i+10000:06d}")
    chosen = service.prioritize_kr_symbols(
        holdings=["005930", "000660"],
        unresolved_orders=["005930", "035420"],
        final30=["000660", "207940"],
    )
    keys = list(service._desired)
    assert keys[:4] == [
        ("KR", "005930"), ("KR", "000660"),
        ("KR", "035420"), ("KR", "207940")
    ]
    assert chosen["desired"] == 44
    assert chosen["overflow"] == 4
    assert ("KR", "010000") in service._desired


def test_42_held_positions_are_not_discarded_beyond_ws_cap(monkeypatch):
    service = _offline_service(monkeypatch)
    monkeypatch.setenv("KIS_WS_MAX_SUBSCRIPTIONS", "40")
    holdings = [f"{i+20000:06d}" for i in range(42)]
    selection = service.prioritize_kr_symbols(
        holdings=holdings, unresolved_orders=[], final30=["005930"]
    )
    assert selection == {"priority": 43, "desired": 43, "overflow": 3}
    assert len(service._desired) == 43
    # The selected 40 WS slots never mean that the other three holdings
    # are dropped from the ordinary WS-first -> REST price evaluation path.
    assert ("KR", holdings[41]) in service._desired
    assert service._next_pending_subscription(
        service._desired, set(), max_subscriptions=40, now_mono=0
    )[0] == ("KR", holdings[0])


def test_us_desired_keys_survive_kr_priority_change(monkeypatch):
    service = _offline_service(monkeypatch)
    service.subscribe_us("AAPL", "NASDAQ")
    service.subscribe_kr("005930")
    service.prioritize_kr_symbols(
        holdings=["000660"], unresolved_orders=[], final30=[]
    )
    assert ("US", "AAPL") in service._desired
    assert ("KR", "000660") in service._desired


def test_owner_priority_rebalance_has_unsubscribe_and_subscribe_paths():
    import inspect
    body = inspect.getsource(KisWebSocketPriceService._run_forever)
    assert "REBALANCE_UNSUBSCRIBE" in body
    assert '"tr_type": "2"' in body
    assert "REST_FALLBACK_FOR_EXCESS" in body
