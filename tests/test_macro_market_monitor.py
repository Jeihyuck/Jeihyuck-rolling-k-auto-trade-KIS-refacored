from macro_monitor.rules import MarketSnapshot, evaluate


def snap(**overrides):
    base = dict(
        us10y=4.80,
        us10y_prev=4.75,
        brent=98.0,
        brent_change_pct=0.5,
        sp500=7500.0,
        sp500_change_pct=-0.2,
        sp500_drawdown_pct=-4.0,
        nasdaq=25000.0,
        vix=18.0,
        us10y_below_470_days=0,
    )
    base.update(overrides)
    return MarketSnapshot(**base)


def test_red_above_515_has_priority_over_drawdown():
    decision = evaluate(snap(us10y=5.16, sp500_drawdown_pct=-15.0))
    assert decision.regime == "RED"
    assert "US10Y_GT_515" in decision.signals
    assert "신규매수 중단" in decision.action


def test_risk_off_above_500_even_when_sp500_is_down_10():
    decision = evaluate(snap(us10y=5.03, sp500_drawdown_pct=-11.0, brent=104.0))
    assert decision.regime == "RISK_OFF"
    assert "US10Y_GT_500" in decision.signals


def test_oil_110_blocks_buy_signal():
    decision = evaluate(snap(us10y=4.65, brent=111.0, sp500_drawdown_pct=-12.0))
    assert decision.regime == "RISK_OFF"
    assert "BRENT_GT_110" in decision.signals


def test_strong_buy_requires_drawdown_and_low_yield():
    decision = evaluate(snap(us10y=4.68, brent=99.0, sp500_drawdown_pct=-10.5))
    assert decision.regime == "BUY_STRONG"
    assert "379800" in decision.action


def test_buy_1_at_10pct_drawdown_with_yield_below_490():
    decision = evaluate(snap(us10y=4.86, brent=102.0, sp500_drawdown_pct=-10.2))
    assert decision.regime == "BUY_1"


def test_buy_ready_requires_two_days_below_470_and_brent_below_100():
    decision = evaluate(snap(us10y=4.66, brent=97.0, us10y_below_470_days=2))
    assert decision.regime == "BUY_READY"


def test_caution_near_5_percent():
    decision = evaluate(snap(us10y=4.93, brent=99.0))
    assert decision.regime == "CAUTION"


def test_rate_shock_signal_at_15bp():
    decision = evaluate(snap(us10y=4.96, us10y_prev=4.80, brent=99.0))
    assert "RATE_SHOCK" in decision.signals
