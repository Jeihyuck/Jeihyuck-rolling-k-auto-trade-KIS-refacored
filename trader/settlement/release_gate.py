"""Fail-closed KR/US settlement integrity and operator activation contract.

No automatic activation is performed. This module is safe to import in either
market and never calls KIS, dispatches orders or mutates existing position rows.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
import os

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from trader.settlement.schema import applications, evidence


@dataclass(frozen=True)
class SettlementHealth:
    market: str
    env: str
    trading_epoch_id: str
    applications: int = 0
    price_pending: int = 0
    quantity_pending: int = 0
    evidence_count: int = 0
    findings: tuple[str, ...] = ()
    readable: bool = True


def inspect_settlement_ledger(
    engine: Engine, *, market: str, env: str, trading_epoch_id: str,
) -> SettlementHealth:
    """Read-only scoped evidence/application consistency snapshot."""
    if market not in {"KR", "US"} or not env or not trading_epoch_id:
        raise ValueError("MARKET_ENV_EPOCH_REQUIRED")
    findings: list[str] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(sa.select(applications).where(sa.and_(
                applications.c.market == market,
                applications.c.env == env,
                applications.c.trading_epoch_id == trading_epoch_id,
            ))).mappings().all()
            broker_evidence = conn.execute(sa.select(evidence).where(sa.and_(
                evidence.c.market == market,
                evidence.c.env == env,
                evidence.c.trading_epoch_id == trading_epoch_id,
            ))).mappings().all()
    except Exception as exc:
        return SettlementHealth(market, env, trading_epoch_id,
                                findings=("SETTLEMENT_DB_UNAVAILABLE:" + type(exc).__name__,),
                                readable=False)
    evidence_ids = {str(e["evidence_id"]) for e in broker_evidence}
    key_to_qty: dict[str, int] = {}
    for row in rows:
        key = str(row["settlement_key"])
        qty = int(row["applied_qty"])
        if qty < 0:
            findings.append("NEGATIVE_APPLIED_QUANTITY:" + key)
        if Decimal(str(row["applied_notional"])) < 0:
            findings.append("NEGATIVE_SETTLEMENT_NOTIONAL:" + key)
        if row["settlement_status"] == "SETTLED" and row["price_status"] != "CONFIRMED":
            findings.append("FALSE_SETTLED_WITHOUT_PRICE:" + key)
        if row["last_evidence_id"] and str(row["last_evidence_id"]) not in evidence_ids:
            findings.append("APPLIED_EVIDENCE_MISSING:" + key)
        key_to_qty[key] = qty
    for row in broker_evidence:
        key = str(row["settlement_key"])
        if key not in key_to_qty:
            findings.append("UNATTRIBUTED_SETTLEMENT_EVIDENCE:" + key)
        elif int(row["confirmed_cumulative_qty"]) > key_to_qty[key]:
            findings.append("OBSERVED_QTY_GT_APPLIED:" + key)
    return SettlementHealth(
        market=market, env=env, trading_epoch_id=trading_epoch_id,
        applications=len(rows),
        price_pending=sum(1 for row in rows if row["price_status"] != "CONFIRMED"),
        quantity_pending=sum(1 for row in rows if row["settlement_status"] == "QUANTITY_PENDING"),
        evidence_count=len(broker_evidence),
        findings=tuple(sorted(set(findings))),
    )


@dataclass(frozen=True)
class ReleaseEvidence:
    """Values must originate from actual replay/PG CI and live shadow audit."""
    migrations_verified: bool = False
    pg_atomicity_and_concurrency_verified: bool = False
    kr_us_replay_verified: bool = False
    original_broker_orders_attributed: bool = False
    original_cumulative_quantity_matches_db: bool = False
    full_authoritative_kis_balance: bool = False
    unresolved_claims: int = 0
    unattributed_fills: int = 0
    unresolved_pnl: int = 0
    legacy_same_order_writer_disabled: bool = False
    shadow_market_days: int = 0
    broker_read_failures: int = 0


@dataclass(frozen=True)
class ReleaseDecision:
    permitted: bool
    reasons: tuple[str, ...]
    market: str
    env: str
    revision: str


def assess_release(
    *, health: SettlementHealth, evidence: ReleaseEvidence,
    revision: str, operator_approved_revision: str | None = None,
    writer_feature_enabled: bool | None = None,
) -> ReleaseDecision:
    """Never infer approval or safety from CI-green or zero ledger rows."""
    reasons: list[str] = []
    if not health.readable:
        reasons.append("LEDGER_DB_UNAVAILABLE")
    if health.findings:
        reasons.extend(health.findings)
    if health.applications <= 0 or health.evidence_count <= 0:
        reasons.append("NO_PROVEN_BASELINE_SEED")
    if health.quantity_pending > 0 or health.price_pending > 0:
        reasons.append("PENDING_SETTLEMENT_REQUIRES_RECONCILE")
    required = {
        "MIGRATION_UNVERIFIED": evidence.migrations_verified,
        "PG_ATOMICITY_UNVERIFIED": evidence.pg_atomicity_and_concurrency_verified,
        "BOTH_MARKETS_REPLAY_UNVERIFIED": evidence.kr_us_replay_verified,
        "BROKER_ORDER_ATTRIBUTION_UNVERIFIED": evidence.original_broker_orders_attributed,
        "CUMULATIVE_QTY_NOT_PROVEN": evidence.original_cumulative_quantity_matches_db,
        "KIS_BALANCE_NOT_AUTHORITATIVE": evidence.full_authoritative_kis_balance,
        "LEGACY_WRITER_STILL_ACTIVE": evidence.legacy_same_order_writer_disabled,
    }
    reasons.extend(label for label, flag in required.items() if not flag)
    if evidence.shadow_market_days < 5:
        reasons.append("SHADOW_OBSERVATION_DAYS_BELOW_FIVE")
    if evidence.unresolved_claims or evidence.unattributed_fills or evidence.unresolved_pnl:
        reasons.append("BROKER_TRUTH_OR_PNL_UNRESOLVED")
    if evidence.broker_read_failures:
        reasons.append("KIS_BROKER_READ_FAILURES")
    enabled = (
        os.getenv("NULLIM_SETTLEMENT_WRITER_ENABLED", "0") == "1"
        if writer_feature_enabled is None else writer_feature_enabled
    )
    if not enabled:
        reasons.append("WRITER_FEATURE_DISABLED")
    approved = (
        os.getenv("NULLIM_SETTLEMENT_RELEASE_APPROVED_REVISION")
        if operator_approved_revision is None else operator_approved_revision
    )
    if not revision or not approved or approved != revision:
        reasons.append("OPERATOR_REVISION_APPROVAL_MISSING")
    return ReleaseDecision(
        permitted=not reasons, reasons=tuple(dict.fromkeys(reasons)),
        market=health.market, env=health.env, revision=revision,
    )
