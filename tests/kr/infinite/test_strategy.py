from dataclasses import replace
from datetime import datetime, timezone

from trader.kr.infinite.config import InfiniteConfig
from trader.kr.infinite.models import Action, BrokerPosition, InfiniteState, Quote
from trader.kr.infinite.risk_adapter import RegimeAction
from trader.kr.infinite.strategy import evaluate


TODAY = datetime.now(timezone.utc).date()
C = InfiniteConfig(deployable_buy_policy=True)
ALLOW = RegimeAction(True, "ALLOW_ROUTINE_BUY")


def call(state=None, broker=None, price=10_000, daily=0, pending=False, regime=ALLOW):
    return evaluate(config=C, state=state or InfiniteState(), broker=broker or BrokerPosition(0, 0, 10_000_000),
        quote=Quote(price, datetime.now(timezone.utc)), trade_date=TODAY, regime=regime,
        pending=pending, daily_filled_buy_notional=daily)


def test_fee_aware_integer_quantity_and_daily_unit():
    d = call()
    assert d.action == Action.BUY and d.quantity == 37
    assert d.estimated_cost <= C.unit_krw


def test_hard_cap_and_40_units():
    state = replace(InfiniteState(), authoritative_buy_notional=15_000_000, used_unit_fraction=40)
    assert call(state).reason == "BLOCK_BY_CAP"


def test_partial_fill_only_uses_daily_remainder():
    d = call(daily=300_000)
    assert d.quantity == 7 and d.estimated_cost <= 75_000


def test_pending_restart_and_same_day_are_blocked():
    assert call(pending=True).reason == "PENDING_ORDER"
    assert call(replace(InfiniteState(), pending_order_key="existing")).reason == "PENDING_ORDER"
    assert call(replace(InfiniteState(), last_buy_trade_date=TODAY)).reason == "DAILY_UNIT_LIMIT"


def test_reconcile_and_ownership_conflict_block():
    broker = BrokerPosition(2, 10_000, 1_000_000)
    assert call(InfiniteState(filled_quantity=1), broker).reason == "RECONCILE_REQUIRED"
    result = evaluate(config=C, state=None, broker=broker, quote=Quote(10_000, datetime.now(timezone.utc)),
        trade_date=TODAY, regime=ALLOW, pending=False, daily_filled_buy_notional=0)
    assert result.reason == "OWNERSHIP_CONFLICT"


def test_sell_has_priority_over_buy():
    state = InfiniteState(filled_quantity=3, authoritative_average_price=10_000)
    d = call(state, BrokerPosition(3, 10_000, 1_000_000), price=10_800)
    assert d.action == Action.SELL


def test_global_regime_block_means_no_buy():
    d = call(regime=RegimeAction(False, "BLOCK_BUY_CRASH"))
    assert d.action == Action.BLOCK


def test_no_deployable_policy_fails_closed_while_enabled():
    d = evaluate(config=InfiniteConfig(), state=InfiniteState(), broker=BrokerPosition(0, 0, 1_000_000),
        quote=Quote(10_000, datetime.now(timezone.utc)), trade_date=TODAY, regime=ALLOW,
        pending=False, daily_filled_buy_notional=0)
    assert d.reason == "NO_DEPLOYABLE_POLICY"


def test_client_order_key_is_idempotent():
    assert call().client_order_key == call().client_order_key
