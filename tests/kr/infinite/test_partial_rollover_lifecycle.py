from datetime import date

from trader.kr.infinite.lifecycle import settle_partial_exit
from trader.kr.infinite.accounting import apply_confirmed_fill
from trader.kr.infinite.runner import _reconcile_pending
from trader.kr.infinite.models import BrokerOrderState, BrokerPosition, OrderIntent, State, Status


def test_terminal_partial_fill_rolls_to_seeded_zero_of_40_cycle():
    state = State(cycle_id="A", allocated_capital_krw=4_000_000, unit_krw=100_000,
                  units_used=5, core_units_used=5, status=Status.ACTIVE,
                  metadata={"pending_profit_stage": "TP1_SUBMITTED"})
    intent = OrderIntent(1, "A", date(2026, 9, 2), "SELL_PARTIAL", "k", 1)
    rolled = settle_partial_exit(state, intent, BrokerOrderState("FILLED", 1, 120_000),
                                  BrokerPosition(2, 2, 100_000, 110_000), date(2026, 9, 2),
                                  allocated_capital_krw=4_000_000)
    assert rolled.status is Status.ACTIVE and rolled.cycle_id == "A" and rolled.units_used == 0
    assert rolled.metadata["buy_round_units_used"] == 0
    assert rolled.metadata["rollover_seed_qty"] == 2
    assert rolled.metadata["profit_stage"] == "TP1_FILLED"
    assert rolled.unit_krw == 95_000


def test_ack_or_zero_fill_cancel_does_not_rollover():
    state = State(cycle_id="A", status=Status.ACTIVE)
    intent = OrderIntent(1, "A", date(2026, 9, 2), "SELL_PARTIAL", "k", 1)
    assert settle_partial_exit(state, intent, BrokerOrderState("ACK"), BrokerPosition(3, 3, 1, 1), date.today(), allocated_capital_krw=100) == state
    unchanged = settle_partial_exit(state, intent, BrokerOrderState("CANCELLED"), BrokerPosition(3, 3, 1, 1), date.today(), allocated_capital_krw=100)
    assert unchanged.status is Status.ACTIVE and unchanged.units_used == state.units_used


def test_old_cycle_fill_never_mutates_active_cycle():
    active = State(cycle_id="B", allocated_capital_krw=4_000_000, unit_krw=100_000,
                   units_used=2, core_units_used=2, core_filled_notional=200_000,
                   status=Status.ACTIVE, metadata={"profit_stage": "TP1_FILLED"})
    old = OrderIntent(9, "A", date(2026, 9, 1), "BUY", "old", 1, unit_sequence=3)
    evidence = BrokerOrderState("FILLED", 1, 90_000)

    class Repo:
        persisted = []
        def pending_intents(self): return [old]
        def persist_intent_reconciliation(self, updates): self.persisted.extend(updates)
    class Executor:
        def order_state(self, intent, trade_date):
            assert trade_date == old.trade_date
            return evidence

    repo = Repo()
    reconciled, updates = _reconcile_pending(repo, Executor(), active, date(2026, 9, 2))
    assert reconciled == active
    assert repo.persisted == updates
    try:
        apply_confirmed_fill(active, old, evidence, date(2026, 9, 2))
    except ValueError as exc:
        assert str(exc) == "KR_INF_CYCLE_INTENT_MISMATCH"
    else:
        raise AssertionError("cycle mismatch must fail closed")


def test_partial_cancel_keeps_partial_stage_without_buy_round_reset():
    state = State(cycle_id="A", units_used=5, core_units_used=5, status=Status.ACTIVE,
                  metadata={"pending_profit_stage": "TP1_SUBMITTED", "partial_exit_pending": True})
    intent = OrderIntent(1, "A", date(2026, 9, 2), "SELL_PARTIAL", "k", 10)
    result = settle_partial_exit(state, intent, BrokerOrderState("CANCELLED", 4, 400_000),
                                 BrokerPosition(16, 16, 100_000, 110_000), date(2026, 9, 2),
                                 allocated_capital_krw=4_000_000)
    assert result.units_used == 5
    assert result.metadata.get("profit_stage") != "TP1_FILLED"
    assert result.metadata["partial_profit_stage"] == "TP1_PARTIAL"
    assert result.metadata["partial_profit_filled_qty"] == 4
