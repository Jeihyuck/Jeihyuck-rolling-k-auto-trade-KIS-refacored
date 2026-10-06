"""Independent KR/US writer cutover gates; default is observe-only.

This module NEVER routes KIS orders or changes economic positions. The
existence of an environment flag is not proof of safe settlement cutover.
"""
from __future__ import annotations

from dataclasses import dataclass
import os

from .health import SettlementHealth

REQUIRED_PROOFS = (
    "postgresql_migration_verified",
    "postgresql_concurrency_verified",
    "incident_replay_verified",
    "shadow_parity_verified",
    "legacy_watermarks_bootstrapped",
    "new_writer_connected",
    "legacy_writer_disabled",
    "fresh_broker_truth_verified",
    "run_revision_matches",
    "explicit_operator_approval",
)


@dataclass(frozen=True)
class SettlementReleaseProof:
    postgresql_migration_verified: bool = False
    postgresql_concurrency_verified: bool = False
    incident_replay_verified: bool = False
    shadow_parity_verified: bool = False
    legacy_watermarks_bootstrapped: bool = False
    new_writer_connected: bool = False
    legacy_writer_disabled: bool = False
    fresh_broker_truth_verified: bool = False
    run_revision_matches: bool = False
    explicit_operator_approval: bool = False


@dataclass(frozen=True)
class SettlementReleaseDecision:
    market: str
    status: str
    writer_allowed: bool
    missing: tuple[str, ...]


def assess_release(
    *,
    market: str,
    health: SettlementHealth,
    proofs: SettlementReleaseProof,
    activation_requested: bool = False,
) -> SettlementReleaseDecision:
    """Evaluate prerequisites; actual routing stays with the market adapters.

    Callers must build `proofs` from independently checked artifacts, DB and
    broker snapshots, NEVER exclusively from editable env flags.
    """
    if market not in {"KR", "US"} or health.market != market:
        raise ValueError("RELEASE_SCOPE_MISMATCH")
    missing = tuple(name for name in REQUIRED_PROOFS if not getattr(proofs, name))
    if health.status != "LEDGER_ONLY_OK":
        missing += ("LEDGER_STATUS_" + health.status,)
    if not activation_requested:
        return SettlementReleaseDecision(market, "SHADOW_ONLY", False, missing)
    if missing:
        return SettlementReleaseDecision(market, "ACTIVATION_BLOCKED", False, missing)
    return SettlementReleaseDecision(market, "READY_FOR_CONTROLLED_SWITCH", True, ())


def assert_no_unguarded_writer_env() -> None:
    """Fail closed when a half-deployed rollout sets an unsafe env switch.

    Phase 4 deliberately does NOT enable the shared economic writer: KR and
    US still use their existing writer and have not been atomically cut over.
    A later, separately-reviewed switch must pass the verified gate in the
    production adapter before it may call settle_atomic.
    """
    if os.getenv("NULLIM_SETTLEMENT_WRITER_ENABLED", "0").strip() == "1":
        raise RuntimeError(
            "SETTLEMENT_WRITER_NOT_ACTIVATED: market adapter and legacy-writer "
            "cutover proofs required; unset NULLIM_SETTLEMENT_WRITER_ENABLED"
        )
