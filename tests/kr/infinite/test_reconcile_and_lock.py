from datetime import date

import pytest

from trader.kr.infinite.models import InfiniteState
from trader.kr.infinite.repository import InfiniteRepository


def test_reconcile_counts_only_owned_actual_partial_fills_and_clears_terminal_pending():
    state = InfiniteState(cycle_id="cycle-1", cycle_status="ACTIVE")
    owned = {"strategy_id": state.strategy_id, "book": state.book, "cycle_id": "cycle-1"}
    fills = [
        {**owned, "side": "BUY", "qty": 10, "price": 10_000, "trade_date": date(2026, 8, 13)},
        {**owned, "side": "BUY", "qty": 2, "price": 9_000, "trade_date": date(2026, 8, 13)},
        {**owned, "side": "SELL", "qty": 3, "price": 11_000, "trade_date": date(2026, 8, 13)},
        {**owned, "cycle_id": "other", "side": "BUY", "qty": 999, "price": 99_000},
    ]
    orders = [{**owned, "status": "FILLED", "client_order_key": "done"}]
    got = InfiniteRepository.reconcile_evidence(state, broker_quantity=9,
        broker_average_price=9_800, fills=fills, orders=orders)
    assert got.filled_quantity == 9
    assert got.authoritative_buy_notional == 118_000
    assert got.authoritative_sell_notional == 33_000
    assert got.used_unit_fraction == 118_000 / 375_000
    assert got.pending_order_key is None and got.cycle_status == "ACTIVE"


def test_partial_order_remains_pending_and_cancel_reject_clear_it():
    state = InfiniteState(cycle_id="c")
    owner = {"strategy_id": state.strategy_id, "book": state.book, "cycle_id": "c"}
    partial = InfiniteRepository.reconcile_evidence(state, broker_quantity=1, broker_average_price=10,
        fills=[{**owner, "side": "BUY", "qty": 1, "price": 10}],
        orders=[{**owner, "status": "PARTIALLY_FILLED", "client_order_key": "p"}])
    assert partial.pending_order_key == "p"
    for terminal in ("CANCELLED", "REJECTED", "FILLED"):
        got = InfiniteRepository.reconcile_evidence(state, broker_quantity=1, broker_average_price=10,
            fills=[{**owner, "side": "BUY", "qty": 1, "price": 10}],
            orders=[{**owner, "status": terminal, "client_order_key": "x"}])
        assert got.pending_order_key is None


class Scalar:
    def __init__(self, value): self.value = value
    def scalar(self): return self.value


class Connection:
    def __init__(self, engine): self.engine = engine; self.closed = False
    def execute(self, statement):
        sql = str(statement)
        if "pg_try_advisory_lock" in sql:
            if self.engine.owner is not None: return Scalar(False)
            self.engine.owner = self; return Scalar(True)
        if "pg_advisory_unlock" in sql:
            assert self.engine.owner is self
            self.engine.owner = None; return Scalar(True)
        raise AssertionError(sql)
    def close(self): self.closed = True


class Engine:
    def __init__(self): self.owner = None; self.connections = []
    def connect(self):
        c = Connection(self); self.connections.append(c); return c


def test_lock_same_connection_blocks_then_releases_and_has_no_stale_pool_lock():
    engine = Engine(); first = InfiniteRepository(engine); second = InfiniteRepository(engine)
    with first.critical_section() as acquired:
        assert acquired
        with second.critical_section() as blocked: assert not blocked
    with second.critical_section() as acquired_again: assert acquired_again
    assert engine.owner is None and all(c.closed for c in engine.connections)


def test_lock_released_after_exception():
    engine = Engine(); repo = InfiniteRepository(engine)
    with pytest.raises(RuntimeError):
        with repo.critical_section() as acquired:
            assert acquired
            raise RuntimeError("boom")
    assert engine.owner is None
