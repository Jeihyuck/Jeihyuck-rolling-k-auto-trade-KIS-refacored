from trader.us.runner.trade_session_runner import advance_timeout_execution_mode
from datetime import date

from trader.us.infinite.integration import run_sleeve
from trader.us.infinite.models import Action, Decision, InfiniteState
from tests.us.test_us_tqqq_infinite_integration import FakeRepository, market


def test_safe_degraded_recovers_only_with_full_authoritative_health():
    mode, allowed = advance_timeout_execution_mode("NORMAL", 3, threshold=3)
    assert (mode, allowed) == ("SAFE_DEGRADED", False)
    quote_only = {"status": "OK", "balance_fetch_failed": True}
    assert advance_timeout_execution_mode(mode, 3, threshold=3, healthy_tick=quote_only) == ("SAFE_DEGRADED", False)
    healthy = {"balance_fetch_failed": False, "ack_reconcile_after_route_status": "OK",
               "unresolved_ack_count": 0, "fill_source_status": "OK", "durable_fence_status": "ACTIVE"}
    assert advance_timeout_execution_mode(mode, 3, threshold=3, healthy_tick=healthy) == ("NORMAL", True)


def test_session_safe_degraded_mode_reaches_real_tqqq_runtime_boundary(monkeypatch):
    monkeypatch.setenv("US_TQQQ_INFINITE_ENABLED", "1")
    monkeypatch.setenv("US_TQQQ_INFINITE_REAL_ORDER", "1")
    mode, buy_allowed = advance_timeout_execution_mode("NORMAL", 3, threshold=3)
    assert mode == "SAFE_DEGRADED" and buy_allowed is False

    decisions = iter([
        Decision(Action.BUY, "FAST_DIP_ADD_BUY", qty=3, notional=210.0),
        Decision(Action.SELL, "TAKE_PROFIT_TP1", qty=3, notional=210.0,
                 metadata={"profit_stage": "TP1", "desired_profit_stage": "TP1"}),
    ])
    monkeypatch.setattr("trader.us.infinite.integration.evaluate", lambda **_kwargs: next(decisions))
    overlay = market("DEFENSE_RISK_OFF", market_regime="RISK_OFF", entry_can_proceed=buy_allowed,
                     exit_can_proceed=True, opening_buy_blocked=False)
    routed: list[dict] = []
    repo = FakeRepository(InfiniteState(cycle_id="owned", core_filled_notional=900))

    buy = run_sleeve(
        positions=[{"symbol": "TQQQ", "qty": 15, "orderable_qty": 15, "avg_price_usd": 60.0}],
        price=70.0, trading_date=date(2026, 8, 31), overlay=overlay,
        repository=repo, route=lambda intent: routed.append(intent) or {"status": "ACK", "intent": intent},
    )
    sell = run_sleeve(
        positions=[{"symbol": "TQQQ", "qty": 15, "orderable_qty": 15, "avg_price_usd": 60.0}],
        price=70.0, trading_date=date(2026, 8, 31), overlay=overlay,
        repository=repo, route=lambda intent: routed.append(intent) or {"status": "ACK", "intent": intent},
    )

    assert buy["reason"] == "SESSION_SAFE_DEGRADED" and buy["orders"] == []
    assert len(routed) == 1 and routed[0]["side"] == "SELL"
    assert sell["status"] == "ACK"
