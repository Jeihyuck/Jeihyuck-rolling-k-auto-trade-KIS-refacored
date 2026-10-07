"""Guarded US economic settlement cutover adapter.

The shared writer is enabled only by a verified SettlementReleaseDecision.
US_STANDARD and TQQQ_INFINITE remain distinct owners. Exactly one economic
writer is invoked for a broker observation.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Callable

from sqlalchemy.engine import Connection, Engine

from trader.settlement.core import (
    SettlementDecision,
    SettlementObservation,
    settle_atomic,
)
from trader.account_state import get_account_key
from trader.settlement.health import check_settlement_health
from trader.settlement.release_gate import (
    REQUIRED_PROOFS,
    SettlementReleaseDecision,
    SettlementReleaseProof,
    assess_release,
)

_ALLOWED_US_OWNERS = frozenset({"US_STANDARD", "TQQQ_INFINITE"})




def _account_scope(env: str) -> str:
    return hashlib.sha256(get_account_key(env=env).encode("utf-8")).hexdigest()


def build_us_broker_observation(
    *,
    order: dict,
    trade_date: str,
    cumulative_qty: int,
    broker_fill_price: float | Decimal,
    evidence_digest: str,
) -> SettlementObservation:
    """Build an atomic observation only from exact KIS cumulative order evidence."""
    meta = order.get("meta") if isinstance(order.get("meta"), dict) else {}
    owner = str(meta.get("strategy_owner") or order.get("strategy_owner") or "").strip().upper()
    if owner not in _ALLOWED_US_OWNERS:
        raise RuntimeError("US_SETTLEMENT_OWNER_SCOPE_INVALID")
    env = str(order.get("env") or os.getenv("KIS_ENV") or "practice").strip().lower()
    epoch = str(order.get("trading_epoch_id") or meta.get("trading_epoch_id") or "").strip()
    lifecycle = str(
        meta.get("position_lifecycle_id")
        or order.get("position_lifecycle_id")
        or order.get("position_cycle_id")
        or ""
    ).strip()
    client_key = str(order.get("client_order_key") or "").strip()
    requested = int(order.get("qty_requested") or order.get("qty") or 0)
    if not epoch or not lifecycle or not client_key or requested <= 0:
        raise RuntimeError("US_SETTLEMENT_DURABLE_IDENTITY_MISSING")
    price = Decimal(str(broker_fill_price))
    return SettlementObservation(
        env=env,
        market="US",
        account_scope=_account_scope(env),
        trading_epoch_id=epoch,
        strategy_owner=owner,
        position_cycle_id=lifecycle,
        client_order_key=client_key,
        broker_trade_date=date.fromisoformat(str(trade_date)),
        exchange=str(order.get("exchange") or meta.get("exchange") or ""),
        broker_order_no=str(order.get("order_no") or ""),
        side=str(order.get("side") or "").upper(),
        requested_qty=requested,
        cumulative_qty=int(cumulative_qty),
        evidence_type="KIS_ORDER_CUMULATIVE_ACTUAL",
        evidence_digest=str(evidence_digest),
        currency="USD",
        execution_price=price,
    )


def _blocked_release(reason: str) -> SettlementReleaseDecision:
    return SettlementReleaseDecision(
        market="US",
        status="ACTIVATION_BLOCKED",
        writer_allowed=False,
        missing=(reason,),
    )


def load_us_runtime_release(
    engine: Engine,
    observation: SettlementObservation,
) -> SettlementReleaseDecision:
    """Require fresh scoped proof artifact plus explicit activation request."""
    activation_requested = os.getenv(
        "NULLIM_US_SETTLEMENT_ACTIVATE", "0"
    ).strip() == "1"
    if not activation_requested:
        return SettlementReleaseDecision(
            market="US",
            status="SHADOW_ONLY",
            writer_allowed=False,
            missing=("activation_not_requested",),
        )
    proof_path = os.getenv("NULLIM_US_SETTLEMENT_RELEASE_PROOF_FILE", "").strip()
    if not proof_path:
        return _blocked_release("release_proof_file_missing")
    try:
        payload = json.loads(Path(proof_path).read_text(encoding="utf-8"))
    except Exception:
        return _blocked_release("release_proof_file_unreadable")

    for field, expected in (
        ("market", "US"),
        ("env", observation.env),
        ("trading_epoch_id", observation.trading_epoch_id),
        ("account_scope", observation.account_scope),
    ):
        if str(payload.get(field) or "") != str(expected):
            return _blocked_release("release_scope_mismatch:" + field)

    expires_at = str(payload.get("expires_at") or "").strip()
    if not expires_at:
        return _blocked_release("release_proof_expiry_missing")
    try:
        expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        if expiry <= datetime.now(timezone.utc):
            return _blocked_release("release_proof_expired")
    except Exception:
        return _blocked_release("release_proof_expiry_invalid")

    proof_values = payload.get("proofs")
    if not isinstance(proof_values, dict):
        return _blocked_release("release_proofs_missing")
    flags = {name: bool(proof_values.get(name)) for name in REQUIRED_PROOFS}

    current_revision = str(
        os.getenv("US_PINNED_RUN_REVISION")
        or os.getenv("GITHUB_SHA")
        or os.getenv("RUN_REVISION")
        or ""
    ).strip()
    proof_revision = str(payload.get("run_revision") or "").strip()
    flags["run_revision_matches"] = bool(
        flags.get("run_revision_matches")
        and current_revision
        and proof_revision
        and current_revision == proof_revision
    )
    health = check_settlement_health(
        engine,
        market="US",
        env=observation.env,
        trading_epoch_id=observation.trading_epoch_id,
        account_scope=observation.account_scope,
    )
    return assess_release(
        market="US",
        health=health,
        proofs=SettlementReleaseProof(**flags),
        activation_requested=True,
    )


@dataclass(frozen=True)
class UsSettlementRouteResult:
    mode: str
    atomic_decision: SettlementDecision | None = None
    legacy_result: Any = None


def _validate_us_release(
    observation: SettlementObservation,
    release: SettlementReleaseDecision,
) -> None:
    if observation.market != "US" or release.market != "US":
        raise RuntimeError("US_SETTLEMENT_RELEASE_SCOPE_MISMATCH")
    if observation.strategy_owner not in _ALLOWED_US_OWNERS:
        raise RuntimeError("US_SETTLEMENT_OWNER_SCOPE_INVALID")
    if release.writer_allowed and release.status != "READY_FOR_CONTROLLED_SWITCH":
        raise RuntimeError("US_SETTLEMENT_WRITER_ALLOWED_WITHOUT_READY_STATUS")


def route_us_settlement(
    *,
    engine: Engine,
    observation: SettlementObservation,
    release: SettlementReleaseDecision,
    apply_atomic_economic_delta: Callable[
        [Connection, SettlementObservation, SettlementDecision], None
    ],
    apply_legacy: Callable[[], Any],
) -> UsSettlementRouteResult:
    """Route one US broker observation to one writer, never both.

    A blocked/shadow release keeps mark_order_filled_by_reconcile and the
    existing projection authoritative. A verified controlled switch uses
    settle_atomic only. Partial/cumulative quantity and late-price replay
    semantics are delegated to the common settlement decision.
    """
    _validate_us_release(observation, release)
    if release.writer_allowed:
        decision = settle_atomic(engine, observation, apply_atomic_economic_delta)
        return UsSettlementRouteResult(
            mode="ATOMIC_SETTLEMENT",
            atomic_decision=decision,
        )
    return UsSettlementRouteResult(
        mode="LEGACY",
        legacy_result=apply_legacy(),
    )
