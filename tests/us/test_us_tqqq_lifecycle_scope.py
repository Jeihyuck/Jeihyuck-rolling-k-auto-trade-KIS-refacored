from datetime import date, datetime, timedelta, timezone

from trader.us.infinite.lifecycle import buy_ttl_expired, order_blocks, rollover_partial_fill
from trader.us.infinite.models import InfiniteState, PositionSnapshot, Status


def test_tqqq_partial_fill_rollover_accounts_for_seed_capital():
    old, child = rollover_partial_fill(InfiniteState(cycle_id="A", status=Status.ACTIVE),
        order_status="CANCELLED", filled_qty=5, filled_notional=500,
        position=PositionSnapshot(qty=20, average_price=100, price=101), trading_date=date(2026, 9, 2))
    assert old.status is Status.COMPLETE
    assert child.metadata["units_used"] == 0 and child.metadata["total_units"] == 40
    assert child.metadata["remaining_deployable_capital_usd"] == 8000
    assert child.total_filled_notional == 2000


def test_open_order_scope_and_ttl_are_not_global():
    order = {"strategy_owner": "TQQQ_INFINITE", "symbol": "TQQQ", "side": "BUY",
             "cycle_id": "A", "status": "OPEN", "created_at": datetime.now(timezone.utc) - timedelta(hours=1)}
    assert order_blocks(order, strategy_owner="TQQQ_INFINITE", symbol="TQQQ", side="BUY", cycle_id="A")
    assert not order_blocks(order, strategy_owner="US_STANDARD", symbol="MSFT", side="SELL")
    assert buy_ttl_expired(order, ttl_seconds=1800)


def test_ack_only_and_zero_fill_terminal_never_reset_cycle():
    state = InfiniteState(cycle_id="A", status=Status.ACTIVE)
    position = PositionSnapshot(qty=20, average_price=100)
    assert rollover_partial_fill(state, order_status="ACK", filled_qty=0, filled_notional=0,
                                 position=position, trading_date=date.today())[1] is None
    old, child = rollover_partial_fill(state, order_status="REJECTED", filled_qty=0, filled_notional=0,
                                       position=position, trading_date=date.today())
    assert child is None and old.status is Status.ACTIVE
