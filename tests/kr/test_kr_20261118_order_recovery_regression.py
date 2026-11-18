"""2026-11-18 replay: one KR Infinite fill and PB1 staged SELL are separate owners.

Pure replay only. Never calls a broker or changes a runtime database.
"""
import json
from dataclasses import replace
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from trader.kr.infinite.accounting import apply_confirmed_fill
from trader.kr.infinite.models import BrokerOrderState, OrderIntent, State, Status
from trader.execution_state import BalanceFreshness
from trader.pb1_engine import PB1Engine


DAY = date(2026, 11, 18)


def _infinite_state():
    return State(
        cycle_id="KRINF-20261112-7c22b1ed",
        cycle_start_date=date(2026, 11, 12),
        status=Status.ACTIVE,
        allocated_capital_krw=Decimal("15000000"),
        core_filled_notional=Decimal("690052.0000"),
        reserve_filled_notional=Decimal("0"),
        units_used=1, core_units_used=1,
    )


def test_kr_infinite_decimal_confirmed_13_share_buy_and_idempotent_replay():
    state = _infinite_state()
    intent = OrderIntent(
        id=13, cycle_id=state.cycle_id, trade_date=DAY, side="BUY",
        idempotency_key="nov18-kodex", requested_qty=13, unit_sequence=2,
        filled_qty=0, filled_notional_krw=Decimal("0"),
        broker_order_id="0000010702", status="SUBMITTED",
    )
    broker = BrokerOrderState("FILLED", 13, Decimal("718900"), Decimal("55300"))
    new_state, qty, notional = apply_confirmed_fill(state, intent, broker, DAY)
    assert qty == 13
    assert notional == 718900.0
    assert new_state.units_used == 2
    assert new_state.core_units_used == 2
    assert new_state.core_filled_notional == Decimal("1408952.0000")
    assert new_state.filled_notional == Decimal("1408952.0000")

    reconciled_intent = replace(
        intent, status="FILLED", filled_qty=13,
        filled_notional_krw=Decimal("718900"),
    )
    for _ in range(100):
        again, dq, dn = apply_confirmed_fill(new_state, reconciled_intent, broker, DAY)
        assert again == new_state
        assert (dq, dn) == (0, 0.0)


def test_kr_infinite_partial_then_final_fill_counts_unit_once():
    state = _infinite_state()
    intent = OrderIntent(
        id=13, cycle_id=state.cycle_id, trade_date=DAY, side="BUY",
        idempotency_key="nov18-kodex-partial", requested_qty=13,
        unit_sequence=2,
    )
    mid, qty, _ = apply_confirmed_fill(
        state, intent, BrokerOrderState("PARTIALLY_FILLED", 5, Decimal("276500")),
        DAY,
    )
    assert qty == 5
    assert mid.units_used == 2
    persisted = replace(intent, filled_qty=5, filled_notional_krw=Decimal("276500"))
    final, qty2, _ = apply_confirmed_fill(
        mid, persisted, BrokerOrderState("FILLED", 13, Decimal("718900")), DAY,
    )
    assert qty2 == 8
    assert final.units_used == 2
    assert final.core_filled_notional == Decimal("1408952")


class FakeOrders:
    def __init__(self, rows):
        self.rows = rows

    def list_today_orders(self, *args, **kwargs):
        return self.rows


class FreshBalance:
    freshness = BalanceFreshness.FRESH

    def holding_qty(self, code):
        return 8


def _pb1_durable_context(*, status="FILLED", request=None, fresh=True):
    request = request if request is not None else {
        "trade_session": "morning", "pre_order_holding_qty": "16",
        "submitted_qty": "8", "exit_stage": "KR_US_PROFIT_DEFENSE_PARTIAL",
    }
    row = {
        "strategy": "pb1_pullback_close", "code": "028050",
        "position_cycle_id": "samsung-cycle", "status": status,
        "stage": "KR_US_PROFIT_DEFENSE_PARTIAL", "qty": 8,
        "request_json": json.dumps(request) if isinstance(request, dict) else request,
    }
    balance = FreshBalance() if fresh else SimpleNamespace(
        freshness=BalanceFreshness.STALE, holding_qty=lambda code: 8,
    )
    return SimpleNamespace(
        orders_repo=FakeOrders([row]), env="practice",
        STRATEGY_NAME="pb1_pullback_close", window_internal="morning",
        _authoritative_balance=balance,
        _session_sell_blocked_codes={"028050": {"reason": "SELL_ACCEPTED"}},
        _session_sell_accepted_codes={"028050": {"reason": "SELL_ACCEPTED"}},
    )


def test_pb1_samsung_ea_partial_to_full_exit_proven_by_fresh_balance(monkeypatch):
    monkeypatch.setenv("PB1_SESSION_KIND", "am")
    engine = _pb1_durable_context()
    blocked, _ = PB1Engine._durable_sell_block(
        engine, code="028050", position_cycle_id="samsung-cycle",
        exit_stage="FULL_EXIT",
    )
    assert not blocked
    assert "028050" not in engine._session_sell_blocked_codes
    assert "028050" not in engine._session_sell_accepted_codes


def test_pb1_samsung_ea_unresolved_or_stale_prior_sell_stays_blocked(monkeypatch):
    monkeypatch.setenv("PB1_SESSION_KIND", "am")
    for status, fresh in (("ACKED", True), ("PARTIAL_FILLED", True),
                          ("FILLED", False)):
        engine = _pb1_durable_context(status=status, fresh=fresh)
        blocked, _ = PB1Engine._durable_sell_block(
            engine, code="028050", position_cycle_id="samsung-cycle",
            exit_stage="FULL_EXIT",
        )
        assert blocked
        assert "028050" in engine._session_sell_blocked_codes


def test_pb1_samsung_ea_corrupt_request_json_fails_closed(monkeypatch):
    monkeypatch.setenv("PB1_SESSION_KIND", "am")
    engine = _pb1_durable_context(request="{invalid-json")
    blocked, _ = PB1Engine._durable_sell_block(
        engine, code="028050", position_cycle_id="samsung-cycle",
        exit_stage="FULL_EXIT",
    )
    assert blocked
