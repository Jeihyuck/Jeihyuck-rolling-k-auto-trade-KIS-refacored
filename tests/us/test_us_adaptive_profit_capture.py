from datetime import date


def _state(**metadata):
    from trader.us.infinite.models import InfiniteState
    return InfiniteState(cycle_id="cycle", metadata=metadata)


def _overlay():
    return {"market_state": "NORMAL", "market_regime": "NORMAL", "base_market_state": "NORMAL",
            "tqqq_context_quality": "ok", "qqq_completed_close": 100, "qqq_ma50": 90,
            "qqq_ma200": 80, "qqq_ma200_slope": 1, "qqq_20d_return": .1,
            "qqq_drawdown_252": -.05, "qqq_realized_vol_20d": .1,
            "qqq_trend_efficiency_20d": .8}


def test_us_normal_tp1_then_tp2():
    from trader.us.infinite.config import InfiniteConfig
    from trader.us.infinite.models import Action, PositionSnapshot
    from trader.us.infinite.strategy import evaluate

    first = evaluate(config=InfiniteConfig(), state=_state(), position=PositionSnapshot(qty=10, orderable_qty=10, average_price=100, price=107),
                     trading_date=date(2026, 8, 20), overlay=_overlay())
    assert first.action == Action.SELL
    assert first.reason == "TAKE_PROFIT_TP1"
    assert first.qty == 5
    second = evaluate(config=InfiniteConfig(), state=_state(profit_stage="TP1"), position=PositionSnapshot(qty=5, orderable_qty=5, average_price=100, price=110),
                      trading_date=date(2026, 8, 20), overlay=_overlay())
    assert second.reason == "TAKE_PROFIT_TP2"
    assert second.qty == 5


def test_us_fast_dip_add_buy_ignores_overlay():
    from trader.us.infinite.config import InfiniteConfig
    from trader.us.infinite.models import Action, PositionSnapshot
    from trader.us.infinite.strategy import evaluate

    state = _state(last_buy_fill_price=100, long_trend="BEAR")
    decision = evaluate(config=InfiniteConfig(), state=state,
                        position=PositionSnapshot(qty=10, orderable_qty=10, average_price=100, price=99),
                        trading_date=date(2026, 8, 20), overlay={**_overlay(), "market_state": "RISK_OFF", "force_entry_block": True, "allow_new_buy": False},
                        daily_filled_buy_notional=0)
    assert decision.action == Action.BUY
    assert decision.reason == "FAST_DIP_ADD_BUY"
    assert decision.qty == 2
