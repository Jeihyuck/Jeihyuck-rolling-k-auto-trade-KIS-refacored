"""Phase 4: actual DB integrity reads and fail-closed release decision."""
from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest
import sqlalchemy as sa

from trader.settlement.core import SettlementObservation, settle_atomic
from trader.settlement.schema import metadata
from trader.settlement.release_gate import (
    ReleaseEvidence, SettlementHealth, assess_release, inspect_settlement_ledger,
)


def _engine():
    eng = sa.create_engine("sqlite:///:memory:")
    metadata.create_all(eng)
    with eng.begin() as conn:
        conn.execute(sa.text("CREATE TABLE economic_position(qty INTEGER NOT NULL, cost NUMERIC NOT NULL)"))
        conn.execute(sa.text("INSERT INTO economic_position(qty,cost) VALUES (0,0)"))
    return eng


def _observation():
    return SettlementObservation(
        env="practice", market="US", account_scope="account-hash",
        trading_epoch_id="epoch-20261006", strategy_owner="US_STANDARD",
        position_cycle_id="cycle-a", client_order_key="client-a",
        broker_trade_date=date(2026, 10, 6), exchange="NASD",
        broker_order_no="0000010101", side="BUY", requested_qty=5,
        cumulative_qty=5, evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        evidence_digest="proof-a", currency="USD", execution_price=Decimal("10"),
    )


def _apply(conn, obs, delta):
    conn.execute(sa.text(
        "UPDATE economic_position SET qty=qty+:q,cost=cost+:n"
    ), {"q": delta.qty_delta, "n": float(delta.notional_delta)})


def _perfect_evidence():
    return ReleaseEvidence(
        migrations_verified=True,
        pg_atomicity_and_concurrency_verified=True,
        kr_us_replay_verified=True,
        original_broker_orders_attributed=True,
        original_cumulative_quantity_matches_db=True,
        full_authoritative_kis_balance=True,
        legacy_same_order_writer_disabled=True,
        shadow_market_days=5,
    )


def test_ledger_read_is_scoped_and_nonmutating():
    engine = _engine()
    settle_atomic(engine, _observation(), _apply)
    health = inspect_settlement_ledger(
        engine, market="US", env="practice", trading_epoch_id="epoch-20261006",
    )
    assert health.applications == 1 and health.evidence_count == 1
    assert health.price_pending == 0 and health.findings == ()
    other = inspect_settlement_ledger(
        engine, market="KR", env="practice", trading_epoch_id="epoch-20261006",
    )
    assert other.applications == 0


def test_zero_ledger_is_not_ready():
    health = inspect_settlement_ledger(
        _engine(), market="KR", env="practice", trading_epoch_id="epoch-20261006",
    )
    dec = assess_release(
        health=health, evidence=_perfect_evidence(), revision="abc",
        operator_approved_revision="abc", writer_feature_enabled=True,
    )
    assert not dec.permitted
    assert "NO_PROVEN_BASELINE_SEED" in dec.reasons


@pytest.mark.parametrize("field", [
    "migrations_verified", "pg_atomicity_and_concurrency_verified",
    "kr_us_replay_verified", "original_broker_orders_attributed",
    "original_cumulative_quantity_matches_db", "full_authoritative_kis_balance",
    "legacy_same_order_writer_disabled",
])
def test_each_required_gate_independently_blocks(field):
    engine = _engine()
    settle_atomic(engine, _observation(), _apply)
    health = inspect_settlement_ledger(
        engine, market="US", env="practice", trading_epoch_id="epoch-20261006",
    )
    evidence = replace(_perfect_evidence(), **{field: False})
    dec = assess_release(
        health=health, evidence=evidence, revision="abc",
        operator_approved_revision="abc", writer_feature_enabled=True,
    )
    assert not dec.permitted


def test_operator_revision_and_default_writer_flag_required(monkeypatch):
    monkeypatch.delenv("NULLIM_SETTLEMENT_WRITER_ENABLED", raising=False)
    monkeypatch.delenv("NULLIM_SETTLEMENT_RELEASE_APPROVED_REVISION", raising=False)
    eng = _engine()
    settle_atomic(eng, _observation(), _apply)
    health = inspect_settlement_ledger(
        eng, market="US", env="practice", trading_epoch_id="epoch-20261006",
    )
    result = assess_release(health=health, evidence=_perfect_evidence(), revision="abc")
    assert not result.permitted
    assert "WRITER_FEATURE_DISABLED" in result.reasons
    assert "OPERATOR_REVISION_APPROVAL_MISSING" in result.reasons


def test_pending_price_and_unattributed_fill_block_release():
    eng = _engine()
    obs = replace(_observation(), execution_price=None)
    settle_atomic(eng, obs, _apply)
    health = inspect_settlement_ledger(
        eng, market="US", env="practice", trading_epoch_id="epoch-20261006",
    )
    decision = assess_release(
        health=health,
        evidence=replace(_perfect_evidence(), unattributed_fills=1),
        revision="abc", operator_approved_revision="abc", writer_feature_enabled=True,
    )
    assert "PENDING_SETTLEMENT_REQUIRES_RECONCILE" in decision.reasons
    assert "BROKER_TRUTH_OR_PNL_UNRESOLVED" in decision.reasons


def test_gate_can_only_pass_with_explicit_all_true_evidence_and_signoff():
    eng = _engine()
    settle_atomic(eng, _observation(), _apply)
    health = inspect_settlement_ledger(
        eng, market="US", env="practice", trading_epoch_id="epoch-20261006",
    )
    decision = assess_release(
        health=health, evidence=_perfect_evidence(),
        revision="approved-sha", operator_approved_revision="approved-sha",
        writer_feature_enabled=True,
    )
    assert decision.permitted and not decision.reasons
