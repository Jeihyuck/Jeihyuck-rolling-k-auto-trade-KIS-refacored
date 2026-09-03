from datetime import date, datetime, timedelta, timezone

from trader.us.infinite.lifecycle import buy_ttl_expired, rollover_partial_fill
from trader.us.infinite.models import InfiniteState, PositionSnapshot, Status


def test_tqqq_partial_fill_rollover_accounts_for_seed_capital():
    rolled = rollover_partial_fill(InfiniteState(cycle_id="A", status=Status.ACTIVE,
        metadata={"profit_stage": "TP1_FILLED"}), order_status="FILLED", filled_qty=5,
        requested_qty=5, filled_notional=500, order_key="key-1",
        position=PositionSnapshot(qty=20, average_price=100, price=101), trading_date=date(2026, 9, 2),
        broker_position_authoritative=True)
    assert rolled.status is Status.ACTIVE and rolled.cycle_id == "A"
    assert rolled.metadata["buy_round_units_used"] == 0
    assert rolled.metadata["remaining_deployable_capital_usd"] == 8000
    assert rolled.total_filled_notional == 2000


def test_open_order_scope_and_ttl_are_not_global():
    order = {"strategy_owner": "TQQQ_INFINITE", "symbol": "TQQQ", "side": "BUY",
             "cycle_id": "A", "status": "OPEN", "created_at": datetime.now(timezone.utc) - timedelta(hours=1)}
    assert buy_ttl_expired(order, ttl_seconds=1800)


def test_ack_only_and_zero_fill_terminal_never_reset_cycle():
    state = InfiniteState(cycle_id="A", status=Status.ACTIVE)
    position = PositionSnapshot(qty=20, average_price=100)
    assert rollover_partial_fill(state, order_status="ACK", filled_qty=0, requested_qty=5,
                                 filled_notional=0, order_key="key", position=position,
                                 trading_date=date.today(), broker_position_authoritative=True) == state
    unchanged = rollover_partial_fill(state, order_status="REJECTED", filled_qty=0,
                                      requested_qty=5, filled_notional=0, order_key="key",
                                      position=position,                                       trading_date=date.today(), broker_position_authoritative=True)
    assert unchanged == state
