from trader.kr.pb1_stability import same_day_semantic_sell_exists


def row(status="ACK", family="TRAIL_STOP_HIT"):
    return {"code": "035720", "side": "SELL", "status": status, "stage": family,
            "request_json": {"strategy_owner": "KR_STANDARD", "position_lifecycle_id": "sid:1:mode:1"}}


def test_ack_same_semantic_sell_is_blocked(monkeypatch):
    monkeypatch.delenv("KR_ALLOW_REPEAT_SEMANTIC_SELL", raising=False)
    assert same_day_semantic_sell_exists(rows=[row()], symbol="035720", strategy_owner="KR_STANDARD",
                                         reason_family="TRAIL_STOP_HIT", lifecycle_id="sid:1:mode:1")


def test_partial_and_canceled_are_economic_attempts(monkeypatch):
    monkeypatch.delenv("KR_ALLOW_REPEAT_SEMANTIC_SELL", raising=False)
    for status in ("PARTIALLY_FILLED", "CANCELLED", "MANUAL_CANCELED", "REJECTED"):
        assert same_day_semantic_sell_exists(rows=[row(status)], symbol="035720", strategy_owner="KR_STANDARD",
                                             reason_family="TRAIL_STOP_HIT", lifecycle_id="sid:1:mode:1")


def test_semantic_repeat_override(monkeypatch):
    monkeypatch.setenv("KR_ALLOW_REPEAT_SEMANTIC_SELL", "1")
    assert not same_day_semantic_sell_exists(rows=[row()], symbol="035720", strategy_owner="KR_STANDARD",
                                             reason_family="TRAIL_STOP_HIT", lifecycle_id="sid:1:mode:1")
