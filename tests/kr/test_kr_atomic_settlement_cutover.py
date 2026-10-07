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
