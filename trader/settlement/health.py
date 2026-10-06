"""Read-only, fail-closed evidence/application integrity checks for KR and US.

This verifies the *new settlement ledger only*. It must never be interpreted
as live-broker parity or permission to activate an economic writer.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine import Engine

from .core import canonical_settlement_key
from .schema import applications, evidence

_EPS = Decimal("0.00000001")
_SCOPE_FIELDS = (
    "env", "market", "account_scope", "trading_epoch_id", "strategy_owner",
    "position_cycle_id", "client_order_key", "broker_trade_date",
    "exchange", "side", "currency",
)


@dataclass(frozen=True)
class SettlementHealth:
    market: str
    env: str
    trading_epoch_id: str
    account_scope: str
    status: str
    application_count: int
    evidence_count: int
    price_pending_count: int
    problems: tuple[str, ...]

    @property
    def ledger_consistent(self) -> bool:
        return self.status == "LEDGER_ONLY_OK"


def _canonical_from_row(row: dict[str, Any]) -> str:
    return canonical_settlement_key(SimpleNamespace(**row))


def _price_proof_candidates(
    proofs: list[dict[str, Any]], *, priced_qty: int,
) -> list[Decimal]:
    """Return independently complete notional proofs for exactly priced_qty."""
    if priced_qty <= 0:
        return [Decimal("0")]
    candidates: list[Decimal] = []

    cumulative = []
    for proof in proofs:
        if (
            proof.get("evidence_type") == "KIS_ORDER_CUMULATIVE_ACTUAL"
            and proof.get("execution_price") is not None
            and int(proof.get("confirmed_cumulative_qty") or 0) == priced_qty
        ):
            cumulative.append(
                Decimal(str(proof["execution_price"])) * Decimal(priced_qty)
            )
    candidates.extend(cumulative)

    individual = [
        proof for proof in proofs
        if proof.get("evidence_type") == "KIS_EXECUTION_ACTUAL"
        and proof.get("execution_price") is not None
        and int(proof.get("execution_qty") or 0) > 0
    ]
    individual_qty = sum(int(proof["execution_qty"]) for proof in individual)
    if individual and individual_qty == priced_qty:
        candidates.append(sum(
            Decimal(str(proof["execution_price"]))
            * Decimal(int(proof["execution_qty"]))
            for proof in individual
        ))
    return candidates


def check_settlement_health(
    engine: Engine, *, market: str, env: str, trading_epoch_id: str,
    account_scope: str,
) -> SettlementHealth:
    """Scope every query to account, epoch and market; never mutate a row."""
    scope = (market, env, trading_epoch_id, account_scope)
    if market not in {"KR", "US"} or not all(scope):
        raise ValueError("explicit market/env/epoch/account scope required")
    where_a = sa.and_(
        applications.c.market == market,
        applications.c.env == env,
        applications.c.trading_epoch_id == trading_epoch_id,
        applications.c.account_scope == account_scope,
    )
    where_e = sa.and_(
        evidence.c.market == market,
        evidence.c.env == env,
        evidence.c.trading_epoch_id == trading_epoch_id,
        evidence.c.account_scope == account_scope,
    )
    try:
        with engine.connect() as conn:
            app_rows = [dict(row) for row in conn.execute(
                sa.select(applications).where(where_a)
            ).mappings()]
            evidence_rows = [dict(row) for row in conn.execute(
                sa.select(evidence).where(where_e)
            ).mappings()]
    except Exception as exc:
        return SettlementHealth(
            market, env, trading_epoch_id, account_scope,
            "LEDGER_UNAVAILABLE", 0, 0, 0,
            ("LEDGER_READ_FAILED:" + type(exc).__name__,),
        )
    if not app_rows and not evidence_rows:
        return SettlementHealth(
            market, env, trading_epoch_id, account_scope,
            "NOT_MIGRATED", 0, 0, 0, ("NO_SETTLEMENT_APPLICATIONS",),
        )

    evidence_by_key: dict[str, list[dict[str, Any]]] = {}
    for item in evidence_rows:
        evidence_by_key.setdefault(str(item["settlement_key"]), []).append(item)
    app_by_key = {str(item["settlement_key"]): item for item in app_rows}
    problems: list[str] = []
    pending = 0
    partial = 0

    for key in evidence_by_key:
        if key not in app_by_key:
            problems.append("ORPHAN_EVIDENCE:" + key)

    for key, row in app_by_key.items():
        proofs = evidence_by_key.get(key, [])
        if not proofs:
            problems.append("MISSING_EVIDENCE:" + key)
            continue

        try:
            if _canonical_from_row(row) != key:
                problems.append("APPLICATION_KEY_PROVENANCE_MISMATCH:" + key)
        except Exception:
            problems.append("APPLICATION_KEY_INVALID:" + key)

        for proof in proofs:
            if any(str(proof.get(field)) != str(row.get(field)) for field in _SCOPE_FIELDS):
                problems.append("EVIDENCE_PROVENANCE_MISMATCH:" + key)
                break

        qty = int(row["applied_qty"])
        priced_qty = int(row.get("priced_qty") or 0)
        requested_qty = int(row["requested_qty"])
        if requested_qty <= 0 or not (0 <= qty <= requested_qty):
            problems.append("INVALID_ORDER_APPLICATION_QUANTITY:" + key)
        elif qty < requested_qty:
            partial += 1
        if not (0 <= priced_qty <= qty):
            problems.append("INVALID_PRICED_QUANTITY:" + key)

        max_qty = max(int(x["confirmed_cumulative_qty"]) for x in proofs)
        if qty > max_qty:
            problems.append("WATERMARK_EXCEEDS_BROKER_EVIDENCE:" + key)
        elif qty < max_qty:
            problems.append("UNAPPLIED_BROKER_EVIDENCE:" + key)

        if any(
            proof["evidence_type"] == "HOLDINGS_DELTA_EXCLUSIVE"
            and (
                proof.get("execution_price") is not None
                or proof.get("execution_qty") is not None
            )
            for proof in proofs
        ):
            problems.append("HOLDINGS_FABRICATED_PRICE:" + key)

        candidates = _price_proof_candidates(proofs, priced_qty=priced_qty)
        if priced_qty > 0 and not candidates:
            problems.append("PRICED_QTY_WITHOUT_COMPLETE_PRICE_EVIDENCE:" + key)
        if len(candidates) > 1 and any(
            abs(candidate - candidates[0]) > _EPS for candidate in candidates[1:]
        ):
            problems.append("CONFLICTING_COMPLETE_PRICE_EVIDENCE:" + key)
        expected_notional = candidates[0] if candidates else Decimal("0")
        applied_notional = Decimal(str(row["applied_notional"]))
        if candidates and abs(applied_notional - expected_notional) > _EPS:
            problems.append("APPLIED_NOTIONAL_MISMATCH:" + key)
        if priced_qty == 0 and abs(applied_notional) > _EPS:
            problems.append("NOTIONAL_WITHOUT_PRICED_QUANTITY:" + key)

        expected_price_status = (
            "CONFIRMED" if qty > 0 and priced_qty == qty else "PRICE_PENDING"
        )
        if str(row["price_status"]) != expected_price_status:
            problems.append("PRICE_STATUS_WATERMARK_MISMATCH:" + key)
        if expected_price_status != "CONFIRMED" and qty > 0:
            pending += 1

        expected_settlement_status = (
            "QUANTITY_PENDING" if qty == 0
            else "PRICE_PENDING" if priced_qty < qty
            else "PARTIALLY_SETTLED" if qty < requested_qty
            else "SETTLED"
        )
        if str(row["settlement_status"]) != expected_settlement_status:
            problems.append("SETTLEMENT_STATUS_WATERMARK_MISMATCH:" + key)

    if problems:
        status = "INTEGRITY_DEGRADED"
    elif pending:
        status = "PRICE_PENDING"
    elif partial:
        status = "PARTIAL_IN_FLIGHT"
    else:
        status = "LEDGER_ONLY_OK"
    return SettlementHealth(
        market, env, trading_epoch_id, account_scope, status,
        len(app_rows), len(evidence_rows), pending, tuple(dict.fromkeys(problems)),
    )
