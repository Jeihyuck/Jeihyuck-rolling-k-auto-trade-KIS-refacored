from datetime import date

from trader.us.execution.order_router import route_order
from trader.us.infinite.integration import exclude_owned
from trader.us.infinite.risk_adapter import effective_regime
from trader.us.market_state_overlay import build_profit_capture_intents
from trader.us.pb1.us_exit_router import route_exit_by_book_horizon


def test_standard_profit_and_swing_exit_never_create_tqqq_intent():
    position = {"symbol": "TQQQ", "qty": 10, "orderable_qty": 10,
                "avg_price": 100, "current_price": 110, "exchange": "NASDAQ"}
    assert build_profit_capture_intents([position], {"profit_capture_enabled": True},
                                        profit_capture_state={}) == []
    assert route_exit_by_book_horizon(position, 80) is None
    assert exclude_owned([position]) == []


def test_router_rejects_non_owner_before_any_broker_or_db_work():
    result = route_order({"symbol": "TQQQ", "side": "BUY"})
    assert result["status"] == "BLOCKED"
    assert result["reason"] == "tqqq_ownership_rejected"
    assert result["broker_submit"] is False


def test_every_documented_tqqq_regime_has_one_effective_policy():
    states = ["STRONG_RISK_ON", "RISK_ON", "NEUTRAL", "DEFENSIVE", "RISK_OFF",
              "CRASH", "DEFENSE_CRASH", "CHOP_HIGH_VOL", "CAPITAL_PRESERVATION"]
    for state in states:
        effective, multiplier, _reserve, allowed, reason = effective_regime(
            {"market_state": state, "market_regime": "DEFENSIVE" if state == "STRONG_RISK_ON" else state}
        )
        assert effective
        assert multiplier >= 0
        assert isinstance(allowed, bool)
        assert reason
