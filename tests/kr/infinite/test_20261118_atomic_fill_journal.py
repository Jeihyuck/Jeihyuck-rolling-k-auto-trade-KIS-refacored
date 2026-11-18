"""KR Infinite durable reconciliation: offline PostgreSQL lock/CAS contract.

No exchange calls, no real database writes. The fake transaction follows
the production SELECT FOR UPDATE -> UPDATE -> state journal boundaries.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest

from trader.kr.infinite.models import BrokerOrderState, OrderIntent, State, Status
from trader.kr.infinite.repository import InfiniteRepository


DAY = date(2026, 11, 18)


class _Result:
    def __init__(self, row=None):
        self._row = deepcopy(row) if row is not None else None

    def mappings(self):
        return self

    def first(self):
        return deepcopy(self._row)


class _Tx:
    def __init__(self, engine):
        self.engine = engine
        self.dialect = SimpleNamespace(name="postgresql")

    def __enter__(self):
        self.original = deepcopy(self.engine.data)
        self.pending = deepcopy(self.engine.data)
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.engine.data = self.pending
        return False

    def execute(self, query, values=None):
        sql = str(query).upper()
        if "SELECT VERSION,CYCLE_ID" in sql and "FROM KR_INFINITE_STATE" in sql:
            return _Result(self.pending["state"])
        if "FROM KR_INFINITE_ORDER_INTENTS" in sql:
            return _Result(self.pending["intent"])
        if "UPDATE KR_INFINITE_ORDER_INTENTS" in sql:
            self.pending["intent"].update({
                "status": values["status"],
                "filled_qty": values["qty"],
                "filled_notional_krw": values["notional"],
                "filled_avg_price": values["average"],
            })
            return _Result()
        raise AssertionError("unexpected SQL: " + sql[:160])


class _Engine:
    def __init__(self):
        self.data = {
            "state": {
                "version": 8, "cycle_id": "KRINF-TEST-CYCLE",
                "units_used": 1,
                "core_filled_notional": Decimal("690052"),
                "reserve_filled_notional": Decimal("0"),
                "status": "ACTIVE",
            },
            "intent": {
                "trading_epoch_id": "epoch-1",
                "strategy_id": "KR_INFINITE_V1",
                "symbol": "122630",
                "cycle_id": "KRINF-TEST-CYCLE",
                "side": "BUY", "idempotency_key": "key-10702",
                "broker_order_id": "0000010702", "trade_date": DAY,
                "requested_qty": 13, "filled_qty": 0,
                "filled_notional_krw": Decimal("0"), "status": "SUBMITTED",
            },
        }

    def begin(self):
        return _Tx(self)


def _fixture(monkeypatch):
    engine = _Engine()
    repository = InfiniteRepository(engine)
    monkeypatch.setattr(repository, "_epoch_id",
                        lambda _conn, required=True: "epoch-1")
    journal = []

    def save_state(conn, state, *, epoch_id):
        assert epoch_id == "epoch-1"
        conn.pending["state"].update({
            "version": conn.pending["state"]["version"] + 1,
            "units_used": state.units_used,
            "core_filled_notional": state.core_filled_notional,
            "reserve_filled_notional": state.reserve_filled_notional,
            "status": state.status.value,
        })
        journal.append(state)

    monkeypatch.setattr(repository, "_save_state", save_state)
    monkeypatch.setattr(repository, "record_execution_claim_for_order",
                        lambda *a, **kw: None)
    intent = OrderIntent(
        id=13, cycle_id="KRINF-TEST-CYCLE", trade_date=DAY, side="BUY",
        idempotency_key="key-10702", requested_qty=13,
        broker_order_id="0000010702", unit_sequence=2,
        status="SUBMITTED", filled_qty=0, filled_notional_krw=Decimal("0"),
    )
    state = State(
        cycle_id="KRINF-TEST-CYCLE", version=8, status=Status.ACTIVE,
        units_used=2, core_units_used=2,
        core_filled_notional=Decimal("1408952"),
        reserve_filled_notional=Decimal("0"),
    )
    broker = BrokerOrderState("FILLED", 13, Decimal("718900"), Decimal("55300"))
    return engine, repository, state, intent, broker, journal


def test_13share_kodex_fill_updates_intent_and_economic_state_together(monkeypatch):
    engine, repo, state, intent, broker, journal = _fixture(monkeypatch)
    repo.persist_reconciliation(state, [(intent, broker)])
    assert len(journal) == 1
    assert engine.data["intent"]["filled_qty"] == 13
    assert engine.data["intent"]["filled_notional_krw"] == Decimal("718900")
    assert engine.data["state"]["core_filled_notional"] == Decimal("1408952")
    assert engine.data["state"]["units_used"] == 2


def test_second_writer_with_stale_intent_cannot_double_apply_fill(monkeypatch):
    engine, repo, state, intent, broker, _ = _fixture(monkeypatch)
    repo.persist_reconciliation(state, [(intent, broker)])
    snapshot = deepcopy(engine.data)
    with pytest.raises(RuntimeError, match="INTENT_RACE_RELOAD"):
        repo.persist_reconciliation(state, [(intent, broker)])
    assert engine.data == snapshot


def test_100_replays_with_reloaded_intent_do_not_increase_notional(monkeypatch):
    engine, repo, state, intent, broker, _ = _fixture(monkeypatch)
    repo.persist_reconciliation(state, [(intent, broker)])
    from dataclasses import replace
    loaded = replace(intent, status="FILLED", filled_qty=13,
                     filled_notional_krw=Decimal("718900"))
    for _ in range(100):
        repo.persist_reconciliation(state, [(loaded, broker)])
    assert engine.data["intent"]["filled_qty"] == 13
    assert engine.data["state"]["core_filled_notional"] == Decimal("1408952")


@pytest.mark.parametrize("bad", [
    BrokerOrderState("FILLED", 13, Decimal("0")),
    BrokerOrderState("FILLED", 14, Decimal("718900")),
    BrokerOrderState("FILLED", -1, Decimal("718900")),
])
def test_invalid_broker_cumulative_values_roll_back_atomically(monkeypatch, bad):
    engine, repo, state, intent, _, journal = _fixture(monkeypatch)
    original = deepcopy(engine.data)
    with pytest.raises(RuntimeError, match="RECONCILE"):
        repo.persist_reconciliation(state, [(intent, bad)])
    assert engine.data == original
    assert journal == []


def test_cross_owner_or_epoch_provenance_never_changes_state(monkeypatch):
    engine, repo, state, intent, broker, _ = _fixture(monkeypatch)
    engine.data["intent"]["strategy_id"] = "PB1"
    prior = deepcopy(engine.data)
    with pytest.raises(RuntimeError, match="INTENT_RACE_RELOAD"):
        repo.persist_reconciliation(state, [(intent, broker)])
    assert engine.data == prior
    engine.data["intent"]["strategy_id"] = "KR_INFINITE_V1"
    engine.data["intent"]["trading_epoch_id"] = "stale-epoch"
    # The fake SELECT intentionally tests that the query alone is not used
    # as sole authorization: an epoch-scoped result must be checked too.
    prior = deepcopy(engine.data)
    with pytest.raises(RuntimeError, match="INTENT_RACE_RELOAD"):
        repo.persist_reconciliation(state, [(intent, broker)])
    assert engine.data == prior


def test_prior_filled_notional_must_not_regress(monkeypatch):
    engine, repo, state, intent, broker, _ = _fixture(monkeypatch)
    engine.data["state"]["core_filled_notional"] = Decimal("1500000")
    before = deepcopy(engine.data)
    with pytest.raises(RuntimeError, match="ECONOMIC_STATE_REGRESSION"):
        repo.persist_reconciliation(state, [(intent, broker)])
    assert engine.data == before


def test_kodex_55215_decision_price_reaches_kis_as_55300_limit(monkeypatch):
    """Submitted limit is exchange-tick-normalized, not a fabricated fill."""
    import trader.kr.infinite.executor as executor_module
    from trader.kr.infinite.models import Decision, Action
    monkeypatch.setattr(executor_module, "is_order_accepted",
                        lambda response, **kwargs: response["rt_cd"] == "0")
    monkeypatch.setattr(executor_module, "extract_order_no",
                        lambda response: response["output"]["ODNO"])
    received = []

    class FakeKIS:
        def buy_stock_limit(self, symbol, qty, price):
            received.append((symbol, qty, price))
            return {"rt_cd": "0", "output": {"ODNO": "0000010702"}}

        def sell_stock(self, *_args, **_kwargs):
            raise AssertionError("must never place a SELL")

    executor = executor_module.KISExecutor(FakeKIS(), "practice")
    intent_price = 55215
    decision = Decision(Action.BUY, reason="OWNER_BUY",
                        qty=13, notional=13 * intent_price)
    odno, response = executor.submit(decision, "122630")
    assert received == [("122630", 13, 55300)]
    assert odno == "0000010702"
    assert response["rt_cd"] == "0"
    # KIS ACK confirms only acceptance, never a 13-share fill.
