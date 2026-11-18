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
    BrokerOrderState("FILLED", None, Decimal("0")),
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


def test_kodex_55215_decision_price_remains_native_etf_55215_at_kis_submit(monkeypatch):
    """ETF's legitimate 5-KRW tick must not be rounded to the stock 100-KRW tick."""
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
    assert received == [("122630", 13, 55215)]
    assert odno == "0000010702"
    assert response["rt_cd"] == "0"
    # 55,300 in 11/18 historical evidence is preserved as the ACTUAL
    # historical request. New code does not rewrite or guess prior fills.
    # KIS ACK confirms only acceptance, never a 13-share fill.


def test_too_large_economic_state_cannot_silently_consume_other_fill(monkeypatch):
    """Quantity-CAS alone is insufficient if a different order raced us."""
    from dataclasses import replace
    engine, repo, state, intent, broker, _ = _fixture(monkeypatch)
    before = deepcopy(engine.data)
    bogus = replace(state, core_filled_notional=Decimal("1408953"))
    with pytest.raises(RuntimeError, match="ECONOMIC_DELTA_MISMATCH"):
        repo.persist_reconciliation(bogus, [(intent, broker)])
    assert engine.data == before


def test_unit_sequence_consistent_with_exactly_one_confirmed_buy(monkeypatch):
    from dataclasses import replace
    engine, repo, state, intent, broker, _ = _fixture(monkeypatch)
    before = deepcopy(engine.data)
    bogus = replace(state, units_used=3)
    with pytest.raises(RuntimeError, match="ECONOMIC_DELTA_MISMATCH"):
        repo.persist_reconciliation(bogus, [(intent, broker)])
    assert engine.data == before


def test_price_unresolved_sell_quantity_can_settle_without_fabricating_proceeds(monkeypatch):
    """SELL balance delta safety is not the same as BUY cost-basis proof."""
    from dataclasses import replace
    engine, repo, state, intent, _, _ = _fixture(monkeypatch)
    engine.data["intent"]["side"] = "SELL_PARTIAL"
    engine.data["intent"]["requested_qty"] = 3
    intent = replace(intent, side="SELL_PARTIAL", requested_qty=3)
    state = replace(state, units_used=1, core_units_used=1,
                    core_filled_notional=Decimal("690052"))
    broker = BrokerOrderState("FILLED", 3, Decimal("0"), None)
    repo.persist_reconciliation(state, [(intent, broker)])
    assert engine.data["intent"]["filled_qty"] == 3
    assert engine.data["intent"]["filled_notional_krw"] == Decimal("0")
    assert engine.data["state"]["core_filled_notional"] == Decimal("690052")


def test_fractional_etf_owner_limit_never_truncates_before_tick(monkeypatch):
    """An owner 55215.1 limit must be rounded UP to 55220, not DOWN to 55215."""
    import trader.kr.infinite.executor as executor_module
    from trader.kr.infinite.models import Decision, Action
    monkeypatch.setattr(executor_module, "is_order_accepted",
                        lambda response, **kwargs: response.get("rt_cd") == "0")
    monkeypatch.setattr(executor_module, "extract_order_no",
                        lambda response: response["output"]["ODNO"])
    seen = []

    class FakeKIS:
        def buy_stock_limit(self, symbol, qty, price):
            seen.append((symbol, qty, price))
            return {"rt_cd": "0", "output": {"ODNO": "SYNTHETIC"}}

    decision = Decision(
        Action.BUY, reason="SYNTHETIC", qty=2, notional=2 * 55215.1,
    )
    executor_module.KISExecutor(FakeKIS(), "practice").submit(decision, "122630")
    assert seen == [("122630", 2, 55220)]


def test_kis_original_order_day_native_total_cash_proves_thirteen_shares():
    """Use exact native KIS order-day total, not rounded AVG or order LIMIT."""
    from trader.kr.infinite.executor import KISExecutor
    from dataclasses import replace
    state_engine, _, state, intent, _, _ = _fixture_native_intent()
    dates = []

    class NativeKIS:
        def inquire_daily_ccld(self, *, start_date, end_date):
            dates.append((start_date, end_date))
            return {"rt_cd": "0", "output1": [{
                "odno": "0000010702", "pdno": "122630",
                "sll_buy_dvsn_cd": "02", "tot_ccld_qty": "13",
                "tot_ccld_amt": "718900", "avg_prvs": "",
            }]}

    broker = KISExecutor(NativeKIS(), "practice").order_state(
        intent, date(2026, 11, 19),
    )
    assert dates == [("20261118", "20261118")]
    assert broker.status == "FILLED"
    assert broker.filled_qty == 13
    assert broker.filled_notional_krw == Decimal("718900")
    assert broker.filled_avg_price == 55300.0


def _fixture_native_intent():
    """A broker receipt never comes from or modifies our simulated state."""
    engine = _Engine()
    state = State(cycle_id="KRINF-TEST-CYCLE", status=Status.ACTIVE)
    intent = OrderIntent(
        id=13, cycle_id="KRINF-TEST-CYCLE", trade_date=DAY,
        side="BUY", idempotency_key="key-10702", requested_qty=13,
        unit_sequence=2, broker_order_id="0000010702",
        status="SUBMITTED", filled_qty=0,
        filled_notional_krw=Decimal("0"),
    )
    return engine, None, state, intent, None, None


@pytest.mark.parametrize(
    ("row_override", "expected_status"),
    [
        ({"pdno": "028050"}, "RECONCILE_PENDING"),
        ({"sll_buy_dvsn_cd": "01"}, "RECONCILE_PENDING"),
        ({"tot_ccld_amt": "0"}, "RECONCILE_PENDING"),
        ({"tot_ccld_amt": "-718900"}, "RECONCILE_PENDING"),
        ({"tot_ccld_amt": "NaN"}, "RECONCILE_PENDING"),
        ({"tot_ccld_amt": "", "avg_prvs": ""}, "RECONCILE_PENDING"),
        ({"tot_ccld_qty": "14"}, "RECONCILE_PENDING"),
    ],
)
def test_kis_order_row_never_authorizes_unproven_or_wrong_owner_fill(
    row_override, expected_status,
):
    """Wrong symbol/side, qty, and missing native cash all preserve BUY fence."""
    from trader.kr.infinite.executor import KISExecutor

    row = {
        "odno": "0000010702", "pdno": "122630", "sll_buy_dvsn_cd": "02",
        "tot_ccld_qty": "13", "tot_ccld_amt": "718900",
        "avg_prvs": "55300",
    }
    row.update(row_override)

    class NativeKIS:
        def inquire_daily_ccld(self, **kwargs):
            return {"rt_cd": "0", "output1": [row]}

        def get_balance(self):
            raise AssertionError("Incorrect fill may not use holdings as a substitute")

    intent = _fixture_native_intent()[3]
    broker = KISExecutor(NativeKIS(), "practice").order_state(intent, DAY)
    assert broker.status == expected_status
    assert broker.filled_qty is None


def test_kis_native_average_only_and_partial_cash_receipts_are_reconciled():
    """Existing native avg-only responses remain compatible, not synthetic."""
    from trader.kr.infinite.executor import KISExecutor
    intent = _fixture_native_intent()[3]
    rows = [
        {
            "odno": "0000010702", "pdno": "122630", "sll_buy_dvsn_cd": "02",
            "tot_ccld_qty": "5", "tot_ccld_amt": "276500",
            "avg_prvs": "55300",
        },
        {
            "odno": "0000010702", "pdno": "122630", "sll_buy_dvsn_cd": "02",
            "tot_ccld_qty": "13", "tot_ccld_amt": "",
            "avg_prvs": "55300",
        },
    ]
    class NativeKIS:
        def __init__(self, row):
            self.row = row
        def inquire_daily_ccld(self, **kwargs):
            return {"rt_cd": "0", "output1": [self.row]}

    first = KISExecutor(NativeKIS(rows[0]), "practice").order_state(intent, DAY)
    assert first.status == "PARTIALLY_FILLED"
    assert first.filled_qty == 5
    assert first.filled_notional_krw == Decimal("276500")
    final = KISExecutor(NativeKIS(rows[1]), "practice").order_state(intent, DAY)
    assert final.status == "FILLED"
    assert final.filled_qty == 13
    assert final.filled_notional_krw == Decimal("718900")
