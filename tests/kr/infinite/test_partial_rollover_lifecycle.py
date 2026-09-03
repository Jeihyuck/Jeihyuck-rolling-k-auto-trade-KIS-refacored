from datetime import date

from trader.kr.infinite.lifecycle import settle_partial_exit
from trader.kr.infinite.models import BrokerOrderState, BrokerPosition, OrderIntent, State, Status


def test_terminal_partial_fill_rolls_to_seeded_zero_of_40_cycle():
    state = State(cycle_id="A", allocated_capital_krw=4_000_000, unit_krw=100_000,
                  units_used=5, core_units_used=5, status=Status.ACTIVE,
                  metadata={"pending_profit_stage": "TP1_SUBMITTED"})
    intent = OrderIntent(1, "A", date(2026, 9, 2), "SELL_PARTIAL", "k", 1)
    old, child = settle_partial_exit(state, intent, BrokerOrderState("FILLED", 1, 120_000),
                                     BrokerPosition(2, 2, 100_000, 110_000), date(2026, 9, 2))
    assert old.status is Status.COMPLETE
    assert old.metadata["completion_reason"] == "PROFIT_PARTIAL_ROLLOVER"
    assert child.status is Status.ACTIVE and child.units_used == 0
    assert child.metadata["rollover_seed_qty"] == 2
    assert child.unit_krw == 95_000


def test_ack_or_zero_fill_cancel_does_not_rollover():
    state = State(cycle_id="A", status=Status.ACTIVE)
    intent = OrderIntent(1, "A", date(2026, 9, 2), "SELL_PARTIAL", "k", 1)
    assert settle_partial_exit(state, intent, BrokerOrderState("ACK"), BrokerPosition(3, 3, 1, 1), date.today())[1] is None
    old, child = settle_partial_exit(state, intent, BrokerOrderState("CANCELLED"), BrokerPosition(3, 3, 1, 1), date.today())
    assert child is None and old.status is Status.ACTIVE
