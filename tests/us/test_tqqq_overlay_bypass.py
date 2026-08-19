from datetime import date


def _overlay():
    return {
        "market_state": "NORMAL",
        "market_regime": "NORMAL",
        "base_market_state": "NORMAL",
        "intraday_market_overlay": "INTRADAY_RISK_OFF",
        "intraday_rotation_overlay": "INTRADAY_SEMI_CRASH",
        "allow_new_buy": False,
        "force_entry_block": True,
        "tqqq_context_quality": "ok",
        "qqq_completed_close": 100.0,
        "qqq_ma50": 90.0,
        "qqq_ma200": 80.0,
        "qqq_ma200_slope": 1.0,
        "qqq_20d_return": 0.1,
        "qqq_drawdown_252": -0.05,
        "qqq_realized_vol_20d": 0.1,
        "qqq_trend_efficiency_20d": 0.8,
    }


def test_tqqq_entry_intent_bypasses_overlay_without_qty_change():
    from trader.us.market_state_overlay import filter_entry_intents_for_market_state

    intent = {"symbol": "TQQQ", "side": "BUY", "qty": 3, "strategy_owner": "TQQQ_INFINITE", "meta": {"strategy_owner": "TQQQ_INFINITE"}}
    kept, blocked = filter_entry_intents_for_market_state([intent], _overlay())

    assert not blocked
    assert kept[0]["qty"] == 3
    assert kept[0]["meta"]["overlay_bypass"] is True


def test_tqqq_strategy_ignores_intraday_overlay_and_buy_multiplier():
    from trader.us.infinite.config import InfiniteConfig
    from trader.us.infinite.models import InfiniteState, PositionSnapshot, Action
    from trader.us.infinite.strategy import evaluate

    decision = evaluate(
        config=InfiniteConfig(), state=InfiniteState(),
        position=PositionSnapshot(price=80.0), trading_date=date(2026, 8, 19),
        overlay=_overlay(), buy_multiplier=0.5, effective_regime_name="INTRADAY_RISK_OFF",
    )

    assert decision.action == Action.BUY
    assert decision.qty == 3
    assert decision.notional == 240.0
