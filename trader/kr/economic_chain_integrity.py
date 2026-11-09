"""Read-only proof of KR BUY identity across contract, KIS fill and position cycle.

A mirrored entry SHA is locally generated metadata, not a KIS-signed execution.
This module never writes to DB, requests a broker trade, or repairs a position.
"""
from __future__ import annotations

import json
from typing import Any, Mapping, Iterable

_NATIVE = {"KIS_ORDER_CUMULATIVE_EXECUTION", "KIS_INDIVIDUAL_EXECUTION"}


def _obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
            return decoded if isinstance(decoded, dict) else {}
        except (ValueError, TypeError):
            pass
    return {}


def _positive_int(value: Any) -> int:
    try:
        numeric = int(str(value))
        return numeric if numeric > 0 and str(value).strip() == str(numeric) else 0
    except (TypeError, ValueError, OverflowError):
        return 0


def _positive_price(value: Any) -> bool:
    try:
        from decimal import Decimal
        numeric = Decimal(str(value))
        return numeric.is_finite() and numeric > 0
    except (ValueError, TypeError, ArithmeticError):
        return False


def audit_kr_buy_identity(
    *, order: Mapping[str, Any], fills: Iterable[Mapping[str, Any]],
    positions: Iterable[Mapping[str, Any]], claim: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Inspect one BUY's durable chain; never promote an ACK to a fill.

    `claim` must be resolved by an exact action key in the caller, not by
    ticker name. If absent, it contributes no proof either way.
    """
    if str(order.get("side") or "").upper() != "BUY":
        raise ValueError("KR_BUY_IDENTITY_REQUIRES_BUY_ORDER")
    from trader.db.repos import _assert_kr_buy_entry_contract

    code = str(order.get("code") or "").zfill(6)
    cycle = str(order.get("position_cycle_id") or "")
    epoch = str(order.get("trading_epoch_id") or "")
    odno = str(order.get("kis_odno") or "").zfill(10)
    request = _obj(order.get("request_json"))
    response = _obj(order.get("response_json"))
    echo = _obj(response.get("_kr_buy_entry_contract"))
    issues: list[str] = []

    try:
        _assert_kr_buy_entry_contract(request)
        contract_valid = bool(
            request.get("entry_contract_sha256")
            and request.get("entry_contract_sha256") == echo.get("entry_contract_sha256")
        )
    except (RuntimeError, ValueError, TypeError):
        contract_valid = False
    if not contract_valid:
        issues.append("LOCAL_CONTRACT_HASH_UNVERIFIED")

    cumulative = []
    individual_qty = 0
    for fill in fills:
        if str(fill.get("order_id") or "") != str(order.get("order_id") or ""):
            continue
        if (
            str(fill.get("code") or "").zfill(6) != code
            or str(fill.get("side") or "").upper() != "BUY"
            or str(fill.get("position_cycle_id") or "") != cycle
            or str(fill.get("trading_epoch_id") or "") != epoch
            or str(fill.get("kis_odno") or "").zfill(10) != odno
        ):
            issues.append("FILL_IDENTITY_OR_CYCLE_MISMATCH")
            continue
        meta = _obj(fill.get("fill_meta_json"))
        kind = str(meta.get("evidence_kind") or "")
        if kind not in _NATIVE or not _positive_price(fill.get("price")):
            issues.append("FILL_NATIVE_PRICE_PROOF_MISSING")
            continue
        qty = _positive_int(fill.get("qty"))
        if not qty:
            issues.append("FILL_NATIVE_QTY_PROOF_MISSING")
        elif kind == "KIS_ORDER_CUMULATIVE_EXECUTION":
            cumulative.append(qty)
        else:
            individual_qty += qty

    if cumulative and individual_qty:
        issues.append("MIXED_CUMULATIVE_AND_INDIVIDUAL_FILL_PROOF")
    priced_qty = max(cumulative, default=0) if not individual_qty else individual_qty
    if not priced_qty:
        issues.append("ORIGINAL_PRICED_BUY_FILL_MISSING")
    if _positive_int(response.get("tot_ccld_qty")) and not priced_qty:
        issues.append("BROKER_RESPONSE_EXECUTION_NOT_PERSISTED")

    claim_state = str((claim or {}).get("action_state") or "").upper()
    if claim_state == "SATISFIED" and not priced_qty:
        issues.append("SATISFIED_CLAIM_WITHOUT_PRICED_FILL")
    if claim_state == "SATISFIED" and priced_qty and priced_qty < _positive_int(order.get("qty")):
        issues.append("CLAIM_QUANTITY_EXCEEDS_PRICED_FILL")

    symbol_positions = [
        p for p in positions
        if str(p.get("code") or "").zfill(6) == code
        and str(p.get("trading_epoch_id") or "") == epoch
    ]
    exact_cycle = any(str(p.get("position_cycle_id") or "") == cycle for p in symbol_positions)
    if not exact_cycle and symbol_positions:
        issues.append("POSITION_CYCLE_NOT_BOUND_TO_BUY")
        if any(
            str(p.get("position_origin") or "").upper() == "IMPORTED"
            and _positive_int(p.get("qty"))
            for p in symbol_positions
        ):
            issues.append("IMPORTED_HOLDING_WITHOUT_EXACT_BUY_PROOF")

    return {
        "code": code,
        "order_id": str(order.get("order_id") or ""),
        "kis_odno": odno,
        "contract_sha_matches_local_echo": contract_valid,
        # This is evidence stored in our DB, not a fresh KIS verification.
        "stored_native_priced_buy_qty": priced_qty,
        "position_cycle_bound": exact_cycle,
        "claim_state": claim_state or None,
        "status": "CHAIN_EVIDENCE_PRESENT" if not issues else "CHAIN_INCOMPLETE",
        "issues": sorted(set(issues)),
        "note": "Read-only DB audit; independent KIS original-day proof is required for historical repair.",
    }
