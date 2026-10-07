from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json

import sqlalchemy as sa
import pytest

from trader.settlement.core import SettlementObservation
from trader.settlement.kr_cutover import (
    load_kr_runtime_release,
    route_kr_settlement,
)
from trader.settlement.release_gate import SettlementReleaseDecision
from trader.settlement.schema import metadata


def _engine():
    engine = sa.create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    return engine


def _obs():
    return SettlementObservation(
        env="practice", market="KR", account_scope="acct",
        trading_epoch_id="epoch", strategy_owner="PB1",
        position_cycle_id="cycle", client_order_key="key",
        broker_trade_date=date(2026, 10, 7), exchange="KOSPI",
        broker_order_no="123", side="SELL", requested_qty=3,
        cumulative_qty=3, evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        evidence_digest="digest", currency="KRW",
        execution_price=Decimal("10000"),
    )


def test_blocked_release_uses_legacy_only():
    calls = []
    result = route_kr_settlement(
        engine=_engine(), observation=_obs(),
        release=SettlementReleaseDecision(
            market="KR", status="ACTIVATION_BLOCKED",
            writer_allowed=False, missing=("shadow_parity_verified",),
        ),
        apply_atomic_economic_delta=lambda *_: calls.append("atomic"),
        apply_legacy=lambda: calls.append("legacy") or {"status": "OK"},
    )
    assert result.mode == "LEGACY"
    assert result.legacy_result == {"status": "OK"}
    assert calls == ["legacy"]


def test_ready_release_uses_atomic_only_and_same_connection_transaction():
    engine = _engine()
    calls = []

    def atomic(conn, obs, decision):
        calls.append(("atomic", id(conn), decision.qty_delta))
        # Callback receives the active settle_atomic connection. No nested
        # repository transaction is needed or permitted by this adapter.
        assert conn.in_transaction()
        assert obs.market == "KR"

    result = route_kr_settlement(
        engine=engine, observation=_obs(),
        release=SettlementReleaseDecision(
            market="KR", status="READY_FOR_CONTROLLED_SWITCH",
            writer_allowed=True, missing=(),
        ),
        apply_atomic_economic_delta=atomic,
        apply_legacy=lambda: calls.append(("legacy",)),
    )
    assert result.mode == "ATOMIC_SETTLEMENT"
    assert result.atomic_decision.qty_delta == 3
    assert len(calls) == 1 and calls[0][0] == "atomic"


def test_writer_allowed_without_ready_status_fails_closed():
    with pytest.raises(RuntimeError, match="WITHOUT_READY_STATUS"):
        route_kr_settlement(
            engine=_engine(), observation=_obs(),
            release=SettlementReleaseDecision(
                market="KR", status="SHADOW_ONLY", writer_allowed=True, missing=(),
            ),
            apply_atomic_economic_delta=lambda *_: None,
            apply_legacy=lambda: None,
        )


def test_market_scope_mismatch_fails_closed():
    with pytest.raises(RuntimeError, match="SCOPE_MISMATCH"):
        route_kr_settlement(
            engine=_engine(), observation=_obs(),
            release=SettlementReleaseDecision(
                market="US", status="READY_FOR_CONTROLLED_SWITCH",
                writer_allowed=True, missing=(),
            ),
            apply_atomic_economic_delta=lambda *_: None,
            apply_legacy=lambda: None,
        )


def test_kr_sell_reconcile_can_share_caller_transaction():
    from datetime import datetime, timezone
    from trader.db.repos import PositionsRepo
    from tests.kr.test_kr_20261006_execution_convergence import _db, _open_position

    engine = _db()
    repo = PositionsRepo(engine)
    position = _open_position(engine, code="293490", qty=115, avg=9485.826)

    with engine.begin() as conn:
        result = repo.reconcile_sell_execution(
            env="practice",
            strategy="pb1_pullback_close",
            sid=1,
            mode=1,
            code="293490",
            market="KOSPI",
            confirmed_cumulative_qty=37,
            fill_price=None,
            filled_at=datetime(2026, 10, 6, 0, 48, tzinfo=timezone.utc),
            position_cycle_id=str(position["position_cycle_id"]),
            portfolio_epoch_id=str(position["portfolio_epoch_id"]),
            order_id="atomic-kr-sell-293490",
            pre_order_holding_qty=115,
            broker_holding_qty=78,
            _conn=conn,
        )
        assert conn.in_transaction()
        assert result["qty_applied"] == 37
        assert result["remaining_qty"] == 78

    stored = repo.get_position(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        code="293490", position_cycle_id=str(position["position_cycle_id"]),
        portfolio_epoch_id=str(position["portfolio_epoch_id"]),
    )
    assert stored["qty"] == 78


def test_kr_position_mutators_expose_caller_connection_contract():
    import inspect
    from trader.db.repos import PositionsRepo

    assert "_conn" in inspect.signature(PositionsRepo.apply_fill).parameters
    assert "_conn" in inspect.signature(PositionsRepo.reconcile_sell_execution).parameters


def test_unproven_holdings_evidence_uses_legacy_when_release_blocked():
    calls = []
    result = route_kr_settlement(
        engine=_engine(), observation=None,
        release=SettlementReleaseDecision(
            market="KR", status="SHADOW_ONLY", writer_allowed=False,
            missing=("HOLDINGS_ORDER_ATTRIBUTION_UNPROVEN",),
        ),
        apply_atomic_economic_delta=lambda *_: calls.append("atomic"),
        apply_legacy=lambda: calls.append("legacy") or {"status": "OK"},
    )
    assert result.mode == "LEGACY"
    assert calls == ["legacy"]


def test_unproven_holdings_evidence_cannot_activate_atomic_writer():
    with pytest.raises(RuntimeError, match="AUTHORITATIVE_OBSERVATION_REQUIRED"):
        route_kr_settlement(
            engine=_engine(), observation=None,
            release=SettlementReleaseDecision(
                market="KR", status="READY_FOR_CONTROLLED_SWITCH",
                writer_allowed=True, missing=(),
            ),
            apply_atomic_economic_delta=lambda *_: None,
            apply_legacy=lambda: None,
        )


def test_runtime_release_env_flag_alone_cannot_activate(monkeypatch):
    engine = _engine()
    monkeypatch.setenv("NULLIM_KR_SETTLEMENT_ACTIVATE", "1")
    monkeypatch.delenv("NULLIM_KR_SETTLEMENT_RELEASE_PROOF_FILE", raising=False)
    decision = load_kr_runtime_release(engine, _obs())
    assert decision.writer_allowed is False
    assert decision.status == "ACTIVATION_BLOCKED"
    assert "release_proof_file_missing" in decision.missing


def test_runtime_release_requires_fresh_scoped_proof_and_revision(monkeypatch, tmp_path):
    from trader.settlement.core import settle_atomic
    from trader.settlement.release_gate import REQUIRED_PROOFS

    engine = _engine()
    obs = _obs()
    settle_atomic(engine, obs, lambda _conn, _obs, _decision: None)

    revision = "a" * 40
    proof_path = tmp_path / "kr-release.json"
    proof_path.write_text(json.dumps({
        "market": "KR",
        "env": obs.env,
        "trading_epoch_id": obs.trading_epoch_id,
        "account_scope": obs.account_scope,
        "run_revision": revision,
        "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        "proofs": {name: True for name in REQUIRED_PROOFS},
    }), encoding="utf-8")
    monkeypatch.setenv("NULLIM_KR_SETTLEMENT_ACTIVATE", "1")
    monkeypatch.setenv("NULLIM_KR_SETTLEMENT_RELEASE_PROOF_FILE", str(proof_path))
    monkeypatch.setenv("GITHUB_SHA", revision)

    decision = load_kr_runtime_release(engine, obs)
    assert decision.status == "READY_FOR_CONTROLLED_SWITCH"
    assert decision.writer_allowed is True


def test_production_reconcile_is_connected_to_single_writer_router():
    source = open("trader/reconcile_kis.py", encoding="utf-8").read()
    assert "route_kr_settlement(" in source
    assert "load_kr_runtime_release" in source
    assert "record_execution_claim_for_order(" in source
    assert "_conn=conn" in source



def test_kr_lifecycle_metadata_mutators_share_atomic_connection_contract():
    import inspect
    from trader.db.repos import PositionsRepo

    assert "_conn" in inspect.signature(PositionsRepo.update_position_fields).parameters
    assert "_conn" in inspect.signature(PositionsRepo.mark_pyramid_add_fill).parameters


def test_production_atomic_callback_projects_exit_and_pyramid_state_in_same_transaction():
    source = open("trader/reconcile_kis.py", encoding="utf-8").read()
    assert "claim_snapshot = orders_repo.record_execution_claim_for_order(" in source
    assert "positions_repo.update_position_fields(" in source
    assert "positions_repo.mark_pyramid_add_fill(" in source
    assert "_conn=conn" in source
    assert "atomic_buy_applied = route_result.mode == \"ATOMIC_SETTLEMENT\"" in source
