from __future__ import annotations

from datetime import date
from decimal import Decimal

import sqlalchemy as sa
import pytest

from trader.settlement.core import SettlementObservation
from trader.settlement.kr_cutover import route_kr_settlement
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
