from datetime import date

import pytest

from trader.kr.infinite.models import Action, BrokerPosition
from trader.kr.infinite.strategy import evaluate
from .conftest import active

DAY = date(2026, 8, 14)


@pytest.mark.parametrize("regime,price,gap,expected", [
    ("KR_STRONG_RISK_ON", 100, 1, Action.BUY),
    ("KR_RISK_ON", 100, 1, Action.BUY),
    ("KR_NORMAL", 99, 2, Action.BUY),
    ("KR_DEFENSE_CAUTION", 97, 3, Action.BUY),
    ("KR_DEFENSE_RISK_OFF", 95, 7, Action.BUY),
    ("KR_DEFENSE_CRASH", 90, 10, Action.WAIT),
    ("KR_SHOCK_REBOUND_PENDING", 90, 10, Action.WAIT),
    ("KR_SHOCK_REBOUND_CONFIRMED", 90, 10, Action.RECOVERY),
])
def test_all_canonical_states_are_supported(default_config, regime, price, gap, expected):
    state = active(crash_seen=regime == "KR_SHOCK_REBOUND_CONFIRMED")
    position = BrokerPosition(100, 100, 100, price)
    decision = evaluate(config=default_config, state=state, position=position, trade_date=DAY,
                        market_state=regime, trading_days_since_last_buy=gap, orderable_cash=1_000_000)
    assert decision.action == expected and decision.next_status != "FROZEN"


@pytest.mark.parametrize("qty,price,expected", [(0, 100, Action.WAIT), (100, 105, Action.WAIT), (100, 110, Action.SELL_ALL)])
def test_unknown_future_state_pauses_buy_but_preserves_exit(default_config, qty, price, expected):
    state = None if qty == 0 else active()
    position = BrokerPosition(qty, qty, 100 if qty else 0, price)
    decision = evaluate(config=default_config, state=state, position=position, trade_date=DAY,
                        market_state="KR_FUTURE_NEW_STATE", orderable_cash=1_000_000)
    assert decision.action == expected
    if expected == Action.WAIT:
        assert decision.reason == "KR_INF_UNKNOWN_REGIME_BUY_PAUSED"


@pytest.mark.parametrize("qty,price,expected", [(0, 100, Action.WAIT), (100, 105, Action.WAIT), (100, 110, Action.SELL_ALL)])
def test_missing_blocked_regime_pauses_buy_but_preserves_exit(default_config, qty, price, expected):
    state = None if qty == 0 else active()
    position = BrokerPosition(qty, qty, 100 if qty else 0, price)
    decision = evaluate(config=default_config, state=state, position=position, trade_date=DAY,
                        market_state=None, regime_data_quality="BLOCKED", orderable_cash=1_000_000)
    assert decision.action == expected
    if expected == Action.WAIT:
        assert decision.reason == "MISSING_KR_MARKET_REGIME_BUY_PAUSED"


def test_pending_sell_reconciles_without_regime(default_config):
    decision = evaluate(config=default_config, state=active(), position=BrokerPosition(100, 100, 100, 105),
                        trade_date=DAY, market_state=None, regime_data_quality="BLOCKED", pending_sell=True)
    assert decision.action == Action.WAIT and decision.next_status.value == "EXIT_PENDING"
