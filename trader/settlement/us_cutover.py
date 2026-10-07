"""Guarded US economic settlement cutover adapter.

The shared writer is enabled only by a verified SettlementReleaseDecision.
US_STANDARD and TQQQ_INFINITE remain distinct owners. Exactly one economic
writer is invoked for a broker observation.
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

_ALLOWED_US_OWNERS = frozenset({"US_STANDARD", "TQQQ_INFINITE"})


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
