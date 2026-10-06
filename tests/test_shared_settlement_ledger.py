"""Database-backed common KR/US settlement invariant regressions."""
from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest
import sqlalchemy as sa

from trader.settlement.core import (
    SettlementConflict, SettlementObservation, preview_decision, settle_atomic,
)
from trader.settlement.schema import metadata


def _obs(market="KR", **values):
    defaults = dict(
        env="practice", market=market, account_scope="account-hash",
        trading_epoch_id="epoch-1",
        strategy_owner="PB1" if market == "KR" else "US_STANDARD",
        position_cycle_id="cycle-1", client_order_key="order-1",
        broker_trade_date=date(2026, 10, 6),
        exchange="KRX" if market == "KR" else "NASD",
        broker_order_no="0000010012", side="BUY", requested_qty=5,
        cumulative_qty=3, evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        evidence_digest="sha256-observation",
        currency="KRW" if market == "KR" else "USD",
        execution_price=Decimal("110"),
    )
    defaults.update(values)
    return SettlementObservation(**defaults)


@pytest.fixture
def ledger():
    engine = sa.create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(sa.text(
            "CREATE TABLE test_positions (settlement_key TEXT PRIMARY KEY, qty INTEGER NOT NULL DEFAULT 0, notional NUMERIC NOT NULL DEFAULT 0)"
        ))
    return engine


def projection(conn, obs, decision):
    conn.execute(sa.text(
        "INSERT INTO test_positions (settlement_key,qty,notional) "
        "VALUES (:key,:qty,:cash) ON CONFLICT(settlement_key) "
        "DO UPDATE SET qty=test_positions.qty+:qty,notional=test_positions.notional+:cash"
    ), {
        "key": decision.settlement_key, "qty": decision.qty_delta,
        "cash": float(decision.notional_delta),
    })


@pytest.mark.parametrize("market", ["KR", "US"])
def test_repeat_filled_observation_100_times_never_double_applies(ledger, market):
    obs = _obs(market)
    decisions = [settle_atomic(ledger, obs, projection) for _ in range(100)]
    assert decisions[0].qty_delta == 3
    assert all(x.qty_delta == 0 and x.notional_delta == 0 for x in decisions[1:])
    with ledger.connect() as conn:
        row = conn.execute(sa.text("SELECT qty,notional FROM test_positions")).one()
    assert row[0] == 3 and row[1] == 330


def test_quantity_known_price_arrives_later_without_qty_reapply(ledger):
    base = _obs(
        evidence_type="HOLDINGS_DELTA_EXCLUSIVE",
        execution_price=None, pre_holding_qty=0, post_holding_qty=3,
        exclusive_order_proof=True, evidence_digest="balance-1",
    )
    pending = settle_atomic(ledger, base, projection)
    priced = settle_atomic(ledger, replace(
        base, evidence_type="KIS_EXECUTION_ACTUAL",
        execution_price=Decimal("100.25"), execution_qty=3,
        evidence_digest="actual-1",
    ), projection)
    assert pending.qty_delta == 3 and pending.price_status == "PRICE_PENDING"
    assert priced.qty_delta == 0 and priced.notional_delta == Decimal("300.75")
    with ledger.connect() as conn:
        row = conn.execute(sa.text("SELECT qty,notional FROM test_positions")).one()
    assert row[0] == 3 and row[1] == pytest.approx(300.75)


def test_partial_then_full_is_monotonic(ledger):
    first = settle_atomic(ledger, _obs(), projection)
    second = settle_atomic(ledger, _obs(
        cumulative_qty=5, execution_price=Decimal("115"),
        evidence_digest="cumulative-5",
    ), projection)
    assert first.qty_delta == 3 and second.qty_delta == 2
    assert second.notional_delta == Decimal("245")


def test_rollback_on_economic_projection_failure(ledger):
    def broken(conn, obs, decision):
        projection(conn, obs, decision)
        raise RuntimeError("simulate-crash-before-commit")
    with pytest.raises(RuntimeError):
        settle_atomic(ledger, _obs(), broken)
    with ledger.connect() as conn:
        assert conn.execute(sa.text("SELECT count(*) FROM settlement_applications")).scalar() == 0
        assert conn.execute(sa.text("SELECT count(*) FROM broker_settlement_evidence")).scalar() == 0
        assert conn.execute(sa.text("SELECT count(*) FROM test_positions")).scalar() == 0
    assert settle_atomic(ledger, _obs(), projection).qty_delta == 3


def test_exclusive_holdings_proof_is_required():
    with pytest.raises(SettlementConflict, match="ATTRIBUTION_UNPROVEN"):
        _obs(evidence_type="HOLDINGS_DELTA_EXCLUSIVE", execution_price=None)
    with pytest.raises(SettlementConflict, match="HOLDINGS_DELTA_MISMATCH"):
        _obs(evidence_type="HOLDINGS_DELTA_EXCLUSIVE", execution_price=None,
             exclusive_order_proof=True, pre_holding_qty=0, post_holding_qty=2)


def test_conflicting_price_and_quantity_regression_are_blocked(ledger):
    settle_atomic(ledger, _obs(), projection)
    with pytest.raises(SettlementConflict, match="CUMULATIVE_QUANTITY_REGRESSION"):
        settle_atomic(ledger, _obs(cumulative_qty=2), projection)
    with pytest.raises(SettlementConflict, match="CONFLICTING_CONFIRMED_EXECUTION_PRICE"):
        settle_atomic(ledger, _obs(execution_price=Decimal("120")), projection)


def test_owner_epoch_and_market_are_separately_scoped(ledger):
    a = settle_atomic(ledger, _obs(), projection)
    b = settle_atomic(ledger, _obs(strategy_owner="KR_INFINITE"), projection)
    c = settle_atomic(ledger, _obs(market="US", currency="USD", exchange="NASD"), projection)
    assert len({a.settlement_key, b.settlement_key, c.settlement_key}) == 3


def test_partial_order_never_reports_economic_settlement_complete(ledger):
    partial = settle_atomic(ledger, _obs(), projection)
    assert partial.cumulative_qty == 3
    assert partial.settlement_status == "PARTIALLY_SETTLED"
    completed = settle_atomic(ledger, _obs(
        cumulative_qty=5, execution_price=Decimal("112"),
        evidence_digest="completed-5",
    ), projection)
    assert completed.settlement_status == "SETTLED"
    assert completed.qty_delta == 2
    with ledger.connect() as conn:
        row = conn.execute(sa.text(
            "SELECT requested_qty,applied_qty,settlement_status FROM settlement_applications"
        )).mappings().one()
    assert row["requested_qty"] == 5
    assert row["applied_qty"] == 5
    assert row["settlement_status"] == "SETTLED"


def test_observation_attempt_cannot_change_immutable_target_qty(ledger):
    settle_atomic(ledger, _obs(), projection)
    with pytest.raises(SettlementConflict, match="SETTLEMENT_SCOPE_CONFLICT"):
        settle_atomic(ledger, _obs(requested_qty=6), projection)
    with ledger.connect() as conn:
        assert conn.execute(sa.text("SELECT applied_qty FROM settlement_applications")).scalar_one() == 3


def test_individual_execution_prices_accumulate_incremental_notional(ledger):
    first = _obs(
        cumulative_qty=1, evidence_type="KIS_EXECUTION_ACTUAL",
        execution_price=Decimal("100"), execution_qty=1,
        evidence_digest="exec-1",
    )
    second = _obs(
        cumulative_qty=2, evidence_type="KIS_EXECUTION_ACTUAL",
        execution_price=Decimal("110"), execution_qty=1,
        evidence_digest="exec-2",
    )
    a = settle_atomic(ledger, first, projection)
    b = settle_atomic(ledger, second, projection)
    assert a.notional_delta == Decimal("100")
    assert b.notional_delta == Decimal("110")
    assert b.priced_qty == 2
    with ledger.connect() as conn:
        row = conn.execute(sa.text(
            "SELECT qty,notional FROM test_positions"
        )).one()
        app = conn.execute(sa.text(
            "SELECT applied_qty,priced_qty,applied_notional,price_status "
            "FROM settlement_applications"
        )).mappings().one()
    assert row[0] == 2 and row[1] == 210
    assert app["applied_qty"] == 2
    assert app["priced_qty"] == 2
    assert Decimal(str(app["applied_notional"])) == Decimal("210")
    assert app["price_status"] == "CONFIRMED"


def test_replaying_same_individual_execution_is_zero_delta(ledger):
    obs = _obs(
        cumulative_qty=1, evidence_type="KIS_EXECUTION_ACTUAL",
        execution_price=Decimal("100"), execution_qty=1,
        evidence_digest="same-exec",
    )
    first = settle_atomic(ledger, obs, projection)
    repeat = settle_atomic(ledger, obs, projection)
    assert first.qty_delta == 1 and first.notional_delta == Decimal("100")
    assert repeat.qty_delta == 0 and repeat.notional_delta == 0
    assert repeat.priced_qty_delta == 0
