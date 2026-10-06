"""Phase 4: fail-closed ledger health and explicit KR/US release gates."""
from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest
import sqlalchemy as sa

from trader.settlement.core import SettlementObservation, settle_atomic
from trader.settlement.health import check_settlement_health
from trader.settlement.release_gate import (
    SettlementReleaseProof, REQUIRED_PROOFS, assess_release,
    assert_no_unguarded_writer_env,
)
from trader.settlement.schema import metadata


@pytest.fixture
def engine():
    db = sa.create_engine("sqlite:///:memory:")
    metadata.create_all(db)
    with db.begin() as conn:
        conn.execute(sa.text("CREATE TABLE economic_projection (settlement_key TEXT PRIMARY KEY, qty INTEGER NOT NULL)"))
    return db


def observation(market="KR", *, price=Decimal("150.25")):
    return SettlementObservation(
        market=market, env="practice", account_scope="a-hashed-account",
        trading_epoch_id="epoch-new", strategy_owner="KR_STANDARD" if market == "KR" else "US_STANDARD",
        position_cycle_id="cycle-1", client_order_key="order-1",
        broker_trade_date=date(2026, 10, 6), exchange="KRX" if market == "KR" else "NASD",
        broker_order_no="0000000123", side="SELL", requested_qty=5, cumulative_qty=5,
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL", evidence_digest="unique-evidence-1",
        currency="KRW" if market == "KR" else "USD", execution_price=price,
    )


def apply_delta(conn, obs, decision):
    conn.execute(sa.text(
        "INSERT INTO economic_projection (settlement_key, qty) VALUES (:key, :qty) "
        "ON CONFLICT(settlement_key) DO UPDATE SET qty=economic_projection.qty+:qty"
    ), {"key": decision.settlement_key, "qty": decision.qty_delta})


def report(engine, market="KR", epoch="epoch-new"):
    return check_settlement_health(
        engine, market=market, env="practice", trading_epoch_id=epoch,
        account_scope="a-hashed-account",
    )


@pytest.mark.parametrize("market", ["KR", "US"])
def test_no_migration_is_not_reported_as_health_ok(engine, market):
    h = report(engine, market)
    assert h.status == "NOT_MIGRATED"
    assert not h.ledger_consistent


@pytest.mark.parametrize("market", ["KR", "US"])
def test_quantity_only_then_actual_price_converges_and_replays(engine, market):
    initial = replace(
        observation(market, price=None), evidence_type="HOLDINGS_DELTA_EXCLUSIVE",
        evidence_digest="balance-1", exclusive_order_proof=True,
        pre_holding_qty=5, post_holding_qty=0,
    )
    settle_atomic(engine, initial, apply_delta)
    h = report(engine, market)
    assert h.status == "PRICE_PENDING"
    actual = replace(
        initial, evidence_type="KIS_EXECUTION_ACTUAL",
        evidence_digest="broker-execution-1", execution_price=Decimal("150.25"),
        execution_qty=5,
    )
    settle_atomic(engine, actual, apply_delta)
    for _ in range(4):
        settle_atomic(engine, actual, apply_delta)
    h = report(engine, market)
    assert h.status == "LEDGER_ONLY_OK"
    assert h.application_count == 1 and h.evidence_count == 2
    with engine.connect() as conn:
        assert conn.execute(sa.text("SELECT qty FROM economic_projection")).scalar_one() == 5


def test_evidence_and_watermark_divergence_fails_health_closed(engine):
    obs = observation()
    result = settle_atomic(engine, obs, apply_delta)
    with engine.begin() as conn:
        conn.execute(sa.text(
            "UPDATE settlement_applications SET applied_qty=9 WHERE settlement_key=:key"
        ), {"key": result.settlement_key})
    h = report(engine)
    assert h.status == "INTEGRITY_DEGRADED"
    assert any("WATERMARK_EXCEEDS_BROKER_EVIDENCE" in reason for reason in h.problems)


def test_market_epoch_and_account_are_isolated(engine):
    settle_atomic(engine, observation("KR"), apply_delta)
    assert report(engine, "KR").status == "LEDGER_ONLY_OK"
    assert report(engine, "US").status == "NOT_MIGRATED"
    assert report(engine, "KR", epoch="different-epoch").status == "NOT_MIGRATED"


def test_unavailable_schema_cannot_pass_a_release_gate():
    empty = sa.create_engine("sqlite:///:memory:")
    result = report(empty)
    assert result.status == "LEDGER_UNAVAILABLE"
    assert not result.ledger_consistent


def test_release_requires_every_independent_proof(engine):
    settle_atomic(engine, observation(), apply_delta)
    healthy = report(engine)
    assert assess_release(
        market="KR", health=healthy, proofs=SettlementReleaseProof(),
        activation_requested=True,
    ).status == "ACTIVATION_BLOCKED"
    flags = {name: True for name in REQUIRED_PROOFS}
    ready = assess_release(
        market="KR", health=healthy, proofs=SettlementReleaseProof(**flags),
        activation_requested=True,
    )
    assert ready.status == "READY_FOR_CONTROLLED_SWITCH"
    assert ready.writer_allowed
    assert not assess_release(
        market="KR", health=healthy, proofs=SettlementReleaseProof(**flags),
        activation_requested=False,
    ).writer_allowed
    wrong_scope = report(engine, "US")
    with pytest.raises(ValueError, match="RELEASE_SCOPE_MISMATCH"):
        assess_release(market="KR", health=wrong_scope, proofs=SettlementReleaseProof(**flags))


def test_unprotected_activation_flag_fails_closed(monkeypatch):
    monkeypatch.delenv("NULLIM_SETTLEMENT_WRITER_ENABLED", raising=False)
    assert_no_unguarded_writer_env()
    monkeypatch.setenv("NULLIM_SETTLEMENT_WRITER_ENABLED", "1")
    with pytest.raises(RuntimeError, match="SETTLEMENT_WRITER_NOT_ACTIVATED"):
        assert_no_unguarded_writer_env()


def test_partially_filled_order_is_not_eligible_for_release(engine):
    partial = replace(observation(), cumulative_qty=3)
    decision = settle_atomic(engine, partial, apply_delta)
    assert decision.settlement_status == "PARTIALLY_SETTLED"
    h = report(engine)
    assert h.status == "PARTIAL_IN_FLIGHT"
    assert not h.ledger_consistent
    flags = {name: True for name in REQUIRED_PROOFS}
    assert not assess_release(
        market="KR", health=h, proofs=SettlementReleaseProof(**flags),
        activation_requested=True,
    ).writer_allowed


def test_read_only_audit_requires_seeded_ledger_and_never_authorizes_writer(engine):
    from trader.settlement.audit import audit_snapshot
    k = dict(market="KR", env="practice", trading_epoch_id="epoch-new",
             account_scope="a-hashed-account")
    exit_status, payload = audit_snapshot(engine, **k)
    assert exit_status == 2 and payload["status"] == "NOT_MIGRATED"
    settle_atomic(engine, observation(), apply_delta)
    exit_status, payload = audit_snapshot(engine, **k)
    assert exit_status == 0
    assert payload["status"] == "LEDGER_ONLY_OK"
    assert payload["writer_activation_allowed"] is False
    assert payload["report_scope"] == "LEDGER_ONLY_NOT_BROKER_PARITY"


def test_positive_but_wrong_notional_cannot_pass_health(engine):
    decision = settle_atomic(engine, observation(), apply_delta)
    with engine.begin() as conn:
        conn.execute(sa.text(
            "UPDATE settlement_applications SET applied_notional=1 "
            "WHERE settlement_key=:key"
        ), {"key": decision.settlement_key})
    h = report(engine)
    assert h.status == "INTEGRITY_DEGRADED"
    assert any("APPLIED_NOTIONAL_MISMATCH" in item for item in h.problems)


def test_evidence_scope_mismatch_cannot_reuse_application_key(engine):
    decision = settle_atomic(engine, observation(), apply_delta)
    with engine.begin() as conn:
        conn.execute(sa.text(
            "UPDATE broker_settlement_evidence SET strategy_owner='KR_INFINITE' "
            "WHERE settlement_key=:key"
        ), {"key": decision.settlement_key})
    h = report(engine)
    assert h.status == "INTEGRITY_DEGRADED"
    assert any("EVIDENCE_PROVENANCE_MISMATCH" in item for item in h.problems)


@pytest.mark.parametrize("bad_status", ["QUANTITY_PENDING", "UNKNOWN", "PRICE_PENDING"])
def test_full_priced_application_requires_exact_settlement_state(engine, bad_status):
    decision = settle_atomic(engine, observation(), apply_delta)
    with engine.begin() as conn:
        conn.execute(sa.text(
            "UPDATE settlement_applications SET settlement_status=:status "
            "WHERE settlement_key=:key"
        ), {"status": bad_status, "key": decision.settlement_key})
    h = report(engine)
    assert h.status == "INTEGRITY_DEGRADED"
    assert any("SETTLEMENT_STATUS_WATERMARK_MISMATCH" in item for item in h.problems)


def test_price_status_must_match_priced_quantity_watermark(engine):
    decision = settle_atomic(engine, observation(), apply_delta)
    with engine.begin() as conn:
        conn.execute(sa.text(
            "UPDATE settlement_applications SET price_status='PRICE_PENDING' "
            "WHERE settlement_key=:key"
        ), {"key": decision.settlement_key})
    h = report(engine)
    assert h.status == "INTEGRITY_DEGRADED"
    assert any("PRICE_STATUS_WATERMARK_MISMATCH" in item for item in h.problems)
