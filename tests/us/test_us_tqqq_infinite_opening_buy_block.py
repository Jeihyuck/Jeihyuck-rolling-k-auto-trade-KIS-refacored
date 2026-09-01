from datetime import date

from trader.us.infinite.integration import run_sleeve
from trader.us.infinite.models import Action, Decision, InfiniteState
from tests.us.test_us_tqqq_infinite_integration import FakeRepository, market


DAY = date(2026, 8, 24)


def _enable_real_orders(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")


def _overlay(*, blocked: bool, now_et: str) -> dict:
    return market(
        "DEFENSE_RISK_OFF",
        market_regime="RISK_OFF",
        opening_buy_blocked=blocked,
        opening_buy_start_et="10:00:00",
        entry_can_proceed=not blocked,
        exit_can_proceed=True,
        now_et=now_et,
        tqqq_quote_source="test",
        tqqq_quote_stale=False,
    )


def test_tqqq_infinite_fast_dip_buy_blocked_before_1000_et(monkeypatch, caplog):
    _enable_real_orders(monkeypatch)
    caplog.set_level("INFO")
    decision = Decision(Action.BUY, "FAST_DIP_ADD_BUY", qty=3, notional=207.96)
    monkeypatch.setattr("trader.us.infinite.integration.evaluate", lambda **_kwargs: decision)
    routed: list[dict] = []

    result = run_sleeve(
        positions=[{"symbol": "TQQQ", "qty": 15, "avg_price_usd": 70.0}],
        price=69.32,
        trading_date=DAY,
        overlay=_overlay(blocked=True, now_et="2026-08-24T09:45:00-04:00"),
        repository=FakeRepository(InfiniteState(cycle_id="owned", core_filled_notional=1050)),
        route=lambda intent: routed.append(intent) or {"status": "ACK"},
    )

    assert routed == []
    assert result["orders"] == []
    assert result["status"] == "WAIT"
    assert result["reason"] == "OPENING_30MIN_BUY_BLOCK"
    assert "[OPENING_BUY_BLOCK][US_TQQQ_INF]" in caplog.text
    assert "original_reason=FAST_DIP_ADD_BUY" in caplog.text


def test_tqqq_infinite_sell_allowed_during_opening_buy_block(monkeypatch, caplog):
    _enable_real_orders(monkeypatch)
    caplog.set_level("INFO")
    decision = Decision(Action.SELL, "TAKE_PROFIT_TP1", qty=3, notional=210.0,
                        metadata={"profit_stage": "TP1", "desired_profit_stage": "TP1"})
    monkeypatch.setattr("trader.us.infinite.integration.evaluate", lambda **_kwargs: decision)
    routed: list[dict] = []

    result = run_sleeve(
        positions=[{"symbol": "TQQQ", "qty": 15, "orderable_qty": 15, "avg_price_usd": 60.0}],
        price=70.0,
        trading_date=DAY,
        overlay=_overlay(blocked=True, now_et="2026-08-24T09:45:00-04:00"),
        repository=FakeRepository(InfiniteState(cycle_id="owned", core_filled_notional=900)),
        route=lambda intent: routed.append(intent) or {"status": "ACK", "intent": intent},
    )

    assert len(routed) == 1
    assert routed[0]["side"] == "SELL"
    assert result["orders"]
    assert "[OPENING_BUY_BLOCK][US_TQQQ_INF]" not in caplog.text


def test_tqqq_infinite_buy_allowed_at_1000_et(monkeypatch):
    _enable_real_orders(monkeypatch)
    decision = Decision(Action.BUY, "FAST_DIP_ADD_BUY", qty=3, notional=207.96)
    monkeypatch.setattr("trader.us.infinite.integration.evaluate", lambda **_kwargs: decision)
    routed: list[dict] = []

    result = run_sleeve(
        positions=[{"symbol": "TQQQ", "qty": 15, "avg_price_usd": 70.0}],
        price=69.32,
        trading_date=DAY,
        overlay=_overlay(blocked=False, now_et="2026-08-24T10:00:00-04:00"),
        repository=FakeRepository(InfiniteState(cycle_id="owned", core_filled_notional=1050)),
        route=lambda intent: routed.append(intent) or {"status": "ACK", "intent": intent},
    )

    assert len(routed) == 1
    assert routed[0]["side"] == "BUY"
    assert result["orders"]


def test_tqqq_infinite_fast_dip_buy_blocked_by_safe_degraded_runtime(monkeypatch, caplog):
    _enable_real_orders(monkeypatch)
    caplog.set_level("WARNING")
    decision = Decision(Action.BUY, "FAST_DIP_ADD_BUY", qty=3, notional=207.96)
    monkeypatch.setattr("trader.us.infinite.integration.evaluate", lambda **_kwargs: decision)
    routed: list[dict] = []

    result = run_sleeve(
        positions=[{"symbol": "TQQQ", "qty": 15, "avg_price_usd": 70.0}],
        price=69.32, trading_date=DAY,
        overlay=_overlay(blocked=False, now_et="2026-08-24T10:00:00-04:00") | {"entry_can_proceed": False},
        repository=FakeRepository(InfiniteState(cycle_id="owned", core_filled_notional=1050)),
        route=lambda intent: routed.append(intent) or {"status": "ACK"},
    )

    assert routed == []
    assert result["status"] == "WAIT"
    assert result["reason"] == "SESSION_SAFE_DEGRADED"
    assert result["decision"].reason == "FAST_DIP_ADD_BUY"
    assert "[TQQQ_INF][RUNTIME_BUY_BLOCK]" in caplog.text


def test_tqqq_infinite_sell_remains_routable_in_safe_degraded(monkeypatch):
    _enable_real_orders(monkeypatch)
    decision = Decision(Action.SELL, "TAKE_PROFIT_TP1", qty=3, notional=210.0,
                        metadata={"profit_stage": "TP1", "desired_profit_stage": "TP1"})
    monkeypatch.setattr("trader.us.infinite.integration.evaluate", lambda **_kwargs: decision)
    routed: list[dict] = []

    result = run_sleeve(
        positions=[{"symbol": "TQQQ", "qty": 15, "orderable_qty": 15, "avg_price_usd": 60.0}],
        price=70.0, trading_date=DAY,
        overlay=_overlay(blocked=False, now_et="2026-08-24T10:00:00-04:00") | {"entry_can_proceed": False},
        repository=FakeRepository(InfiniteState(cycle_id="owned", core_filled_notional=900)),
        route=lambda intent: routed.append(intent) or {"status": "ACK", "intent": intent},
    )

    assert result["status"] == "ACK"
    assert len(routed) == 1
    assert routed[0]["side"] == "SELL"
