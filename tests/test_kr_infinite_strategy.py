from datetime import date, timedelta

from trader.strategies.kr_infinite import (
    CampaignState,
    KrInfiniteConfig,
    MarketState,
    evaluate_trade,
    next_add_trigger,
)


def _state(**overrides):
    base = dict(
        code="123456",
        cycle_id="cycle-1",
        started_on=date(2026, 8, 1),
        qty=10,
        avg_price=10000.0,
        invested_krw=100000.0,
        deployed_tranches=3,
        last_fill_price=9800.0,
        buys_today=0,
    )
    base.update(overrides)
    return CampaignState(**base)


def test_new_cycle_starts_with_two_equal_tranches():
    cfg = KrInfiniteConfig()
    state = _state(qty=0, avg_price=0, invested_krw=0, deployed_tranches=0, last_fill_price=0)
    plan = evaluate_trade(
        state=state,
        market=MarketState(price=10000, regime_score=2.0),
        campaign_budget_krw=24_000_000,
        available_cash_krw=24_000_000,
        today=date(2026, 8, 14),
        config=cfg,
    )
    assert plan.action == "BUY"
    assert plan.tranche_count == 2
    assert plan.qty == 200
    assert plan.reason == "cycle_start"


def test_ladder_add_uses_last_fill_not_average_price():
    cfg = KrInfiniteConfig(add_step_pct=0.025)
    state = _state(avg_price=10000, last_fill_price=9800)
    trigger = next_add_trigger(state, cfg)
    assert trigger == 9555.0
    hold = evaluate_trade(
        state=state,
        market=MarketState(price=9600, regime_score=1.0),
        campaign_budget_krw=24_000_000,
        available_cash_krw=10_000_000,
        today=date(2026, 8, 14),
        config=cfg,
    )
    buy = evaluate_trade(
        state=state,
        market=MarketState(price=9555, regime_score=1.0),
        campaign_budget_krw=24_000_000,
        available_cash_krw=10_000_000,
        today=date(2026, 8, 14),
        config=cfg,
    )
    assert hold.action == "HOLD"
    assert buy.action == "BUY"
    assert buy.tranche_count == 1


def test_normal_regime_harvests_at_four_and_half_percent():
    plan = evaluate_trade(
        state=_state(avg_price=10000),
        market=MarketState(price=10450, regime_score=1.0),
        campaign_budget_krw=24_000_000,
        available_cash_krw=5_000_000,
        today=date(2026, 8, 14),
        config=KrInfiniteConfig(),
    )
    assert plan.action == "SELL"
    assert plan.qty == 10
    assert plan.reason == "cycle_take_profit"


def test_weak_regime_harvests_earlier_at_three_percent():
    plan = evaluate_trade(
        state=_state(avg_price=10000),
        market=MarketState(price=10300, regime_score=-0.5),
        campaign_budget_krw=24_000_000,
        available_cash_krw=5_000_000,
        today=date(2026, 8, 14),
        config=KrInfiniteConfig(),
    )
    assert plan.action == "SELL"
    assert plan.reason == "cycle_take_profit"


def test_defense_regime_stops_averaging_down():
    plan = evaluate_trade(
        state=_state(last_fill_price=10000),
        market=MarketState(price=9500, regime_score=-1.5),
        campaign_budget_krw=24_000_000,
        available_cash_krw=10_000_000,
        today=date(2026, 8, 14),
        config=KrInfiniteConfig(),
    )
    assert plan.action == "HOLD"
    assert plan.reason == "defense_regime"


def test_deployment_cap_preserves_twenty_five_percent_cash_reserve():
    cfg = KrInfiniteConfig(tranches=24, max_deployed_tranches=18)
    plan = evaluate_trade(
        state=_state(deployed_tranches=18, last_fill_price=10000),
        market=MarketState(price=9000, regime_score=1.0),
        campaign_budget_krw=24_000_000,
        available_cash_krw=6_000_000,
        today=date(2026, 8, 14),
        config=cfg,
    )
    assert plan.action == "HOLD"
    assert plan.reason == "deployment_cap"


def test_premium_guard_and_hard_drawdown_precede_new_buys():
    premium_plan = evaluate_trade(
        state=_state(),
        market=MarketState(price=9500, regime_score=1.0, nav_premium_pct=0.02),
        campaign_budget_krw=24_000_000,
        available_cash_krw=10_000_000,
        today=date(2026, 8, 14),
        config=KrInfiniteConfig(max_market_premium_pct=0.015),
    )
    assert premium_plan.action == "HOLD"
    assert premium_plan.reason == "nav_premium_guard"

    stop_plan = evaluate_trade(
        state=_state(avg_price=10000, last_fill_price=9000),
        market=MarketState(price=8200, regime_score=1.0),
        campaign_budget_krw=24_000_000,
        available_cash_krw=10_000_000,
        today=date(2026, 8, 14),
        config=KrInfiniteConfig(max_drawdown_pct=0.18),
    )
    assert stop_plan.action == "SELL"
    assert stop_plan.reason == "campaign_drawdown_stop"
