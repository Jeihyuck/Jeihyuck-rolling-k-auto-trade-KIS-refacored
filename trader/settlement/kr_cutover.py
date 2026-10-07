"""Guarded KR economic settlement cutover adapter.

This module is deliberately inert unless a verified SettlementReleaseDecision
allows the KR writer.  It never trusts an environment flag as activation proof.
Exactly one economic writer is invoked per route decision.
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




_OWNER_MAP = {
    "PB1": "PB1",
    "KR_STANDARD": "PB1",
    "KR_INFINITE": "KR_INFINITE",
}


def _account_scope(env: str) -> str:
    return hashlib.sha256(get_account_key(env=env).encode("utf-8")).hexdigest()


def build_kr_broker_observation(
    *,
    order: dict,
    request_json: dict,
    broker_trade_date: date,
    requested_qty: int,
    cumulative_qty: int,
    evidence_type: str,
    evidence_digest: str,
    execution_price: float | Decimal | None = None,
    execution_qty: int | None = None,
) -> SettlementObservation:
    """Build an atomic observation only from an exact durable KR order identity."""
    raw_owner = str(
        request_json.get("strategy_owner")
        or order.get("strategy_owner")
        or ""
    ).strip().upper()
    owner = _OWNER_MAP.get(raw_owner, "")
    if not owner:
        raise RuntimeError("KR_SETTLEMENT_STRATEGY_OWNER_MISSING")
    env = str(order.get("env") or os.getenv("KIS_ENV") or "practice").strip().lower()
    cycle = str(order.get("position_cycle_id") or "").strip()
    epoch = str(order.get("trading_epoch_id") or "").strip()
    client_key = str(order.get("client_order_key") or "").strip()
    if not cycle or not epoch or not client_key:
        raise RuntimeError("KR_SETTLEMENT_DURABLE_IDENTITY_MISSING")
    price = (
        Decimal(str(execution_price))
        if execution_price is not None and float(execution_price) > 0
        else None
    )
    return SettlementObservation(
        env=env,
        market="KR",
        account_scope=_account_scope(env),
        trading_epoch_id=epoch,
        strategy_owner=owner,
        position_cycle_id=cycle,
        client_order_key=client_key,
        broker_trade_date=broker_trade_date,
        exchange=str(order.get("market") or "KRX"),
        broker_order_no=str(order.get("kis_odno") or order.get("broker_order_id") or ""),
        side=str(order.get("side") or "").upper(),
        requested_qty=int(requested_qty),
        cumulative_qty=int(cumulative_qty),
        evidence_type=str(evidence_type),
        evidence_digest=str(evidence_digest),
        currency="KRW",
        execution_price=price,
        execution_qty=execution_qty,
    )


def _blocked_release(reason: str) -> SettlementReleaseDecision:
    return SettlementReleaseDecision(
        market="KR",
        status="ACTIVATION_BLOCKED",
        writer_allowed=False,
        missing=(reason,),
    )


def load_kr_runtime_release_for_scope(
    engine: Engine,
    *,
    env: str,
    trading_epoch_id: str,
    account_scope: str | None = None,
) -> SettlementReleaseDecision:
    """Evaluate KR writer release for an account/epoch without applying evidence."""
    env = str(env or "").strip().lower()
    trading_epoch_id = str(trading_epoch_id or "").strip()
    account_scope = str(account_scope or _account_scope(env)).strip()
    if not env or not trading_epoch_id or not account_scope:
        return _blocked_release("release_scope_incomplete")
    activation_requested = os.getenv(
        "NULLIM_KR_SETTLEMENT_ACTIVATE", "0"
    ).strip() == "1"
    if not activation_requested:
        return SettlementReleaseDecision(
            market="KR",
            status="SHADOW_ONLY",
            writer_allowed=False,
            missing=("activation_not_requested",),
        )

    proof_path = os.getenv("NULLIM_KR_SETTLEMENT_RELEASE_PROOF_FILE", "").strip()
    if not proof_path:
        return _blocked_release("release_proof_file_missing")
    try:
        payload = json.loads(Path(proof_path).read_text(encoding="utf-8"))
    except Exception:
        return _blocked_release("release_proof_file_unreadable")

    for field, expected in (
        ("market", "KR"),
        ("env", env),
        ("trading_epoch_id", trading_epoch_id),
        ("account_scope", account_scope),
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
        os.getenv("KR_RUN_REVISION")
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
        market="KR",
        env=env,
        trading_epoch_id=trading_epoch_id,
        account_scope=account_scope,
    )
    proofs = SettlementReleaseProof(**flags)
    decision = assess_release(
        market="KR",
        health=health,
        proofs=proofs,
        activation_requested=True,
    )
    if decision.writer_allowed:
        return decision

    # Once every independent cutover proof remains valid, normal in-flight
    # settlement states must not switch the same order back to the legacy
    # writer. PARTIAL_IN_FLIGHT and PRICE_PENDING are non-corrupt intermediate
    # states when health.problems is empty; integrity degradation still blocks.
    continuation_statuses = {"PARTIAL_IN_FLIGHT", "PRICE_PENDING"}
    all_proofs_valid = all(bool(flags.get(name)) for name in REQUIRED_PROOFS)
    if (
        health.status in continuation_statuses
        and not health.problems
        and all_proofs_valid
    ):
        return SettlementReleaseDecision(
            market="KR",
            status="READY_FOR_CONTROLLED_SWITCH",
            writer_allowed=True,
            missing=(),
        )
    return decision


def load_kr_runtime_release(
    engine: Engine,
    observation: SettlementObservation,
) -> SettlementReleaseDecision:
    return load_kr_runtime_release_for_scope(
        engine,
        env=observation.env,
        trading_epoch_id=observation.trading_epoch_id,
        account_scope=observation.account_scope,
    )


@dataclass(frozen=True)
class KrSettlementRouteResult:
    mode: str
    atomic_decision: SettlementDecision | None = None
    legacy_result: Any = None


def _validate_kr_release(
    observation: SettlementObservation | None,
    release: SettlementReleaseDecision,
) -> None:
    if observation.market != "KR" or release.market != "KR":
        raise RuntimeError("KR_SETTLEMENT_RELEASE_SCOPE_MISMATCH")
    if release.writer_allowed and release.status != "READY_FOR_CONTROLLED_SWITCH":
        raise RuntimeError("KR_SETTLEMENT_WRITER_ALLOWED_WITHOUT_READY_STATUS")


def route_kr_settlement(
    *,
    engine: Engine,
    observation: SettlementObservation | None,
    release: SettlementReleaseDecision,
    apply_atomic_economic_delta: Callable[
        [Connection, SettlementObservation, SettlementDecision], None
    ],
    apply_legacy: Callable[[], Any],
) -> KrSettlementRouteResult:
    """Route one proven KR broker observation to exactly one economic writer.

    SHADOW_ONLY / ACTIVATION_BLOCKED keep the legacy writer authoritative.
    READY_FOR_CONTROLLED_SWITCH invokes settle_atomic and *does not* invoke
    legacy mutation. The atomic callback must use the Connection supplied by
    settle_atomic, so economic mutation and settlement watermark share one DB
    transaction.
    """
    if observation is None:
        if release.writer_allowed:
            raise RuntimeError("KR_SETTLEMENT_AUTHORITATIVE_OBSERVATION_REQUIRED")
    else:
        _validate_kr_release(observation, release)
    if release.writer_allowed:
        assert observation is not None
        decision = settle_atomic(engine, observation, apply_atomic_economic_delta)
        return KrSettlementRouteResult(
            mode="ATOMIC_SETTLEMENT",
            atomic_decision=decision,
        )
    return KrSettlementRouteResult(
        mode="LEGACY",
        legacy_result=apply_legacy(),
    )


def shadow_only_kr_release(*, reason: str = "CONTROLLED_SWITCH_NOT_REQUESTED") -> SettlementReleaseDecision:
    """Merge-safe production default: legacy stays authoritative until proofs are verified."""
    return SettlementReleaseDecision(
        market="KR", status="SHADOW_ONLY", writer_allowed=False, missing=(reason,),
    )
