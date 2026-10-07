"""Guarded KR economic settlement cutover adapter.

This module is deliberately inert unless a verified SettlementReleaseDecision
allows the KR writer.  It never trusts an environment flag as activation proof.
Exactly one economic writer is invoked per route decision.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from sqlalchemy.engine import Connection, Engine

from trader.settlement.core import (
    SettlementDecision,
    SettlementObservation,
    settle_atomic,
)
from trader.settlement.release_gate import SettlementReleaseDecision


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
    observation: SettlementObservation,
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
