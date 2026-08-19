from datetime import date


def test_kr_infinite_entry_bypasses_general_overlay():
    from trader.kr.market_state_overlay import filter_kr_entry_intent

    out = filter_kr_entry_intent({"code": "122630", "side": "BUY", "qty": 1, "order_value": 100000, "owner_strategy": "KR_INFINITE"}, {
        "market_state": "KR_DEFENSE_RISK_OFF", "force_entry_block": True,
    })

    assert out["side"] == "BUY"
    assert out["meta"]["overlay_bypass"] is True


def test_kr_infinite_first_buy_opening_delay_and_stable_buy():
    from trader.kr.infinite.config import InfiniteConfig
    from trader.kr.infinite.models import Action, BrokerPosition, State
    from trader.kr.infinite.strategy import evaluate

    kwargs = dict(config=InfiniteConfig(), state=State(allocated_capital_krw=1000000, unit_krw=100000), position=BrokerPosition(0, 0, 0, 100),
                  trade_date=date(2026, 8, 19), market_state="KR_DEFENSE_RISK_OFF",
                  orderable_cash=1000000, minutes_since_open=1, best_ask=100)
    assert evaluate(**kwargs).reason == "KR_INF_FIRST_BUY_OPENING_STABILIZATION_WAIT"
    ready = evaluate(**{**kwargs, "minutes_since_open": 11})
    assert ready.action == Action.BUY


def test_existing_kr_infinite_cycle_skips_opening_delay():
    from trader.kr.infinite.config import InfiniteConfig
    from trader.kr.infinite.models import Action, BrokerPosition, State, Status
    from trader.kr.infinite.strategy import evaluate

    state = State(cycle_id="cycle", status=Status.ACTIVE, units_used=1, core_units_used=1,
                  allocated_capital_krw=1000000, unit_krw=100000, last_buy_price=110)
    decision = evaluate(config=InfiniteConfig(), state=state, position=BrokerPosition(1, 1, 110, 100),
                        trade_date=date(2026, 8, 19), market_state="KR_DEFENSE_RISK_OFF",
                        orderable_cash=1000000, minutes_since_open=1, best_ask=100)
    assert decision.action in {Action.BUY, Action.WAIT}
    assert decision.reason != "KR_INF_FIRST_BUY_OPENING_STABILIZATION_WAIT"
