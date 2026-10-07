from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import inspect
import json

import sqlalchemy as sa
import pytest

from trader.settlement.core import SettlementObservation
from trader.settlement.release_gate import SettlementReleaseDecision
from trader.settlement.schema import metadata
from trader.settlement.us_cutover import load_us_runtime_release, route_us_settlement


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



def test_us_reconcile_mutators_accept_caller_owned_transaction():
    from trader.execution_claims import DurableExecutionClaimRepo
    from trader.us.db import repos

    assert "_conn" in inspect.signature(repos.mark_order_filled_by_reconcile).parameters
    assert "_conn" in inspect.signature(repos.mark_us_profit_capture_stage).parameters
    assert "_conn" in inspect.signature(DurableExecutionClaimRepo.record_observation).parameters


def test_production_broker_observation_is_connected_to_single_writer_router():
    from trader.us.db import repos

    source = inspect.getsource(repos.apply_broker_order_observation)
    assert "route_us_settlement(" in source
    assert "load_us_runtime_release" in source
    assert "mark_order_filled_by_reconcile(" in source
    assert "_conn=conn" in source
    assert "SETTLEMENT_CUTOVER_BLOCKED" in source


def test_runtime_release_env_flag_alone_cannot_activate(monkeypatch):
    engine = _engine()
    monkeypatch.setenv("NULLIM_US_SETTLEMENT_ACTIVATE", "1")
    monkeypatch.delenv("NULLIM_US_SETTLEMENT_RELEASE_PROOF_FILE", raising=False)
    decision = load_us_runtime_release(engine, _obs())
    assert decision.writer_allowed is False
    assert decision.status == "ACTIVATION_BLOCKED"
    assert "release_proof_file_missing" in decision.missing


def test_runtime_release_requires_fresh_scoped_proof_and_revision(monkeypatch, tmp_path):
    from trader.settlement.core import settle_atomic
    from trader.settlement.release_gate import REQUIRED_PROOFS

    engine = _engine()
    obs = _obs(cumulative=5, price=Decimal("100"))
    settle_atomic(engine, obs, lambda _conn, _obs, _decision: None)

    revision = "b" * 40
    proof_path = tmp_path / "us-release.json"
    proof_path.write_text(json.dumps({
        "market": "US",
        "env": obs.env,
        "trading_epoch_id": obs.trading_epoch_id,
        "account_scope": obs.account_scope,
        "run_revision": revision,
        "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        "proofs": {name: True for name in REQUIRED_PROOFS},
    }), encoding="utf-8")
    monkeypatch.setenv("NULLIM_US_SETTLEMENT_ACTIVATE", "1")
    monkeypatch.setenv("NULLIM_US_SETTLEMENT_RELEASE_PROOF_FILE", str(proof_path))
    monkeypatch.setenv("GITHUB_SHA", revision)

    decision = load_us_runtime_release(engine, obs)
    assert decision.status == "READY_FOR_CONTROLLED_SWITCH"
    assert decision.writer_allowed is True
