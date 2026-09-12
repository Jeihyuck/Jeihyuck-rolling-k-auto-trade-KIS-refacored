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


def test_tqqq_entry_intent_bypasses_pb1_intraday_overlay_without_qty_change():
    from trader.us.market_state_overlay import filter_entry_intents_for_market_state

    intent = {"symbol": "TQQQ", "side": "BUY", "qty": 3, "strategy_owner": "TQQQ_INFINITE", "meta": {"strategy_owner": "TQQQ_INFINITE"}}
    kept, blocked = filter_entry_intents_for_market_state([intent], _overlay())

    assert not blocked
    assert kept[0]["qty"] == 3
    assert kept[0]["meta"]["overlay_bypass"] is True


def test_tqqq_strategy_ignores_intraday_labels_but_preserves_structural_multiplier():
    from trader.us.infinite.config import InfiniteConfig
    from trader.us.infinite.models import InfiniteState, PositionSnapshot, Action
    from trader.us.infinite.strategy import evaluate

    strategy_overlay = _overlay()
    # General production gates are a separate contract. Isolate only the PB1
    # intraday label presence/absence in this structural-sizing test.
    strategy_overlay["allow_new_buy"] = True
    strategy_overlay["force_entry_block"] = False
    decision = evaluate(
        config=InfiniteConfig(), state=InfiniteState(),
        position=PositionSnapshot(price=80.0), trading_date=date(2026, 8, 19),
        overlay=strategy_overlay, entry_allowed=True, buy_multiplier=0.5,
        effective_regime_name="NEUTRAL",
    )

    assert decision.action == Action.BUY
    assert decision.qty == 1
    assert decision.notional == 80.0


def test_tqqq_structural_crash_blocks_buy_even_when_intraday_overlay_is_normal():
    from trader.us.infinite.config import InfiniteConfig
    from trader.us.infinite.models import InfiniteState, PositionSnapshot, Action
    from trader.us.infinite.strategy import evaluate

    overlay = _overlay()
    overlay["intraday_market_overlay"] = "NORMAL"
    overlay["intraday_rotation_overlay"] = "NORMAL"
    overlay["allow_new_buy"] = True
    overlay["force_entry_block"] = False

    decision = evaluate(
        config=InfiniteConfig(), state=InfiniteState(),
        position=PositionSnapshot(price=80.0), trading_date=date(2026, 8, 19),
        overlay=overlay, entry_allowed=False, buy_multiplier=0.0,
        effective_regime_name="DEFENSE_CRASH_CONFIRMED",
    )

    assert decision.action == Action.BLOCK
    assert decision.reason == "tqqq_effective_regime_entry_block"


def test_tqqq_production_intraday_keys_do_not_change_structural_sizing():
    from trader.us.infinite.config import InfiniteConfig
    from trader.us.infinite.models import InfiniteState, PositionSnapshot
    from trader.us.infinite.strategy import evaluate

    with_keys = _overlay()
    with_keys["intraday_market_overlay"] = "NORMAL"
    with_keys["intraday_rotation_overlay"] = "NORMAL"
    with_keys["allow_new_buy"] = True
    with_keys["force_entry_block"] = False
    without_keys = dict(with_keys)
    without_keys.pop("intraday_market_overlay")
    without_keys.pop("intraday_rotation_overlay")

    kwargs = dict(
        config=InfiniteConfig(), state=InfiniteState(),
        position=PositionSnapshot(price=80.0), trading_date=date(2026, 8, 19),
        entry_allowed=True, buy_multiplier=0.75, effective_regime_name="NEUTRAL",
    )
    production = evaluate(overlay=with_keys, **kwargs)
    replay = evaluate(overlay=without_keys, **kwargs)

    assert production.action == replay.action
    assert production.qty == replay.qty
    assert production.notional == replay.notional
