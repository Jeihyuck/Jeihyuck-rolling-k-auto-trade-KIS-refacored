from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal

import sqlalchemy as sa
import pytest

from trader.settlement.core import SettlementObservation
from trader.settlement.release_gate import SettlementReleaseDecision
from trader.settlement.schema import metadata
from trader.settlement.us_cutover import route_us_settlement


def _engine():
    engine = sa.create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    return engine


def _obs(owner="US_STANDARD", cumulative=2, price=Decimal("100")):
    return SettlementObservation(
        env="practice", market="US", account_scope="acct",
        trading_epoch_id="epoch", strategy_owner=owner,
        position_cycle_id="cycle", client_order_key="key",
        broker_trade_date=date(2026, 10, 6), exchange="NASDAQ",
        broker_order_no="123", side="SELL", requested_qty=5,
        cumulative_qty=cumulative,
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        evidence_digest=f"digest-{cumulative}-{price}", currency="USD",
        execution_price=price,
    )


def _ready():
    return SettlementReleaseDecision(
        market="US", status="READY_FOR_CONTROLLED_SWITCH",
        writer_allowed=True, missing=(),
    )


def test_blocked_release_uses_legacy_only():
    calls = []
    result = route_us_settlement(
        engine=_engine(), observation=_obs(),
        release=SettlementReleaseDecision(
            market="US", status="ACTIVATION_BLOCKED",
            writer_allowed=False, missing=("shadow_parity_verified",),
        ),
        apply_atomic_economic_delta=lambda *_: calls.append("atomic"),
        apply_legacy=lambda: calls.append("legacy") or {"status": "OK"},
    )
    assert result.mode == "LEGACY"
    assert calls == ["legacy"]


def test_ready_release_uses_atomic_only():
    calls = []

    def atomic(conn, obs, decision):
        assert conn.in_transaction()
        calls.append((obs.strategy_owner, decision.qty_delta))

    result = route_us_settlement(
        engine=_engine(), observation=_obs(), release=_ready(),
        apply_atomic_economic_delta=atomic,
        apply_legacy=lambda: calls.append(("legacy", 0)),
    )
    assert result.mode == "ATOMIC_SETTLEMENT"
    assert calls == [("US_STANDARD", 2)]


def test_tqqq_owner_remains_separate_and_allowed():
    calls = []
    result = route_us_settlement(
        engine=_engine(), observation=_obs(owner="TQQQ_INFINITE"),
        release=_ready(),
        apply_atomic_economic_delta=lambda _c, o, d: calls.append((o.strategy_owner, d.qty_delta)),
        apply_legacy=lambda: calls.append(("legacy", 0)),
    )
    assert result.mode == "ATOMIC_SETTLEMENT"
    assert calls == [("TQQQ_INFINITE", 2)]


def test_unknown_owner_fails_closed():
    with pytest.raises(RuntimeError, match="OWNER_SCOPE_INVALID"):
        route_us_settlement(
            engine=_engine(), observation=_obs(owner="PB1"), release=_ready(),
            apply_atomic_economic_delta=lambda *_: None,
            apply_legacy=lambda: None,
        )


def test_partial_then_late_price_does_not_reapply_quantity():
    engine = _engine()
    seen = []

    first = replace(_obs(cumulative=2, price=None), evidence_digest="qty-only")
    route_us_settlement(
        engine=engine, observation=first, release=_ready(),
        apply_atomic_economic_delta=lambda _c, _o, d: seen.append((d.qty_delta, d.priced_qty_delta)),
        apply_legacy=lambda: None,
    )
    later = replace(_obs(cumulative=2, price=Decimal("101")), evidence_digest="late-price")
    route_us_settlement(
        engine=engine, observation=later, release=_ready(),
        apply_atomic_economic_delta=lambda _c, _o, d: seen.append((d.qty_delta, d.priced_qty_delta)),
        apply_legacy=lambda: None,
    )
    assert seen == [(2, 0), (0, 2)]
