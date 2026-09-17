"""2026-09-17 KR execution-integrity repairs.

Implementation-only guards for production failures observed on 2026-09-17.
No PB1/TP/KR-Infinite thresholds are changed here.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import functools
import json
import logging
import sys
from typing import Any

logger = logging.getLogger(__name__)
_INSTALLED = False
_PRICE_QUANT = Decimal("0.000001")


def _json_object(value: Any, *, field: str, strict: bool = False) -> dict[str, Any]:
    """Normalize DB JSON read-back variants without weakening validation."""
    if isinstance(value, dict):
        return dict(value)
    raw = value
    if isinstance(raw, memoryview):
        raw = raw.tobytes()
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = bytes(raw).decode("utf-8")
        except Exception as exc:
            if strict:
                raise RuntimeError(f"{field}_NOT_JSON:{type(value).__name__}") from exc
            return {}
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            if strict:
                raise RuntimeError(f"{field}_NOT_JSON:empty")
            return {}
        try:
            decoded = json.loads(text)
        except Exception as exc:
            if strict:
                raise RuntimeError(f"{field}_NOT_JSON:malformed") from exc
            return {}
        if isinstance(decoded, dict):
            return dict(decoded)
        if strict:
            raise RuntimeError(f"{field}_NOT_JSON_OBJECT:{type(decoded).__name__}")
        return {}
    if strict:
        raise RuntimeError(f"{field}_NOT_JSON:{type(value).__name__}")
    return {}


def _canonical_price(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value)).quantize(_PRICE_QUANT, rounding=ROUND_HALF_UP)
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _install_kr_buy_json_roundtrip_guard() -> None:
    import trader.db.repos as repos

    if getattr(repos, "_pr132_kr_buy_json_guard_installed", False):
        return
    original_payload = repos._kr_buy_entry_contract_payload
    original_hash = repos._kr_buy_entry_contract_hash
    original_assert = repos._assert_kr_buy_entry_contract

    @functools.wraps(original_payload)
    def _payload(value: Any):
        return original_payload(_json_object(value, field="KR_BUY_ENTRY_CONTRACT", strict=True))

    @functools.wraps(original_hash)
    def _hash(value: Any):
        return original_hash(_json_object(value, field="KR_BUY_ENTRY_CONTRACT", strict=True))

    @functools.wraps(original_assert)
    def _assert(value: Any) -> None:
        normalized = _json_object(value, field="KR_BUY_ENTRY_CONTRACT", strict=True)
        original_assert(normalized)

    repos._kr_buy_entry_contract_payload = _payload
    repos._kr_buy_entry_contract_hash = _hash
    repos._assert_kr_buy_entry_contract = _assert
    repos._pr132_kr_buy_json_guard_installed = True

    # reconcile_kis imports the validator by value. Keep both bindings aligned
    # so restart reconciliation uses the same durable contract semantics.
    reconcile = sys.modules.get("trader.reconcile_kis")
    if reconcile is not None:
        reconcile._assert_kr_buy_entry_contract = _assert


def _normalized_adoption_position(position: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    p = dict(position or {})
    decode_errors: list[str] = []
    for field in ("entry_exit_plan_json", "position_meta", "entry_meta_json"):
        raw = p.get(field)
        if isinstance(raw, dict) or raw in (None, ""):
            p[field] = dict(raw or {})
            continue
        parsed = _json_object(raw, field=f"KR_POLICY_ADOPTION_{field}", strict=False)
        if not parsed:
            decode_errors.append(f"{field}_not_json_object")
        p[field] = parsed

    plan = dict(p.get("entry_exit_plan_json") or {})
    for field in ("risk_plan", "profit_plan", "policy_adoption_contract"):
        raw = plan.get(field)
        if isinstance(raw, dict) or raw in (None, ""):
            plan[field] = dict(raw or {})
        else:
            parsed = _json_object(raw, field=f"KR_POLICY_ADOPTION_plan_{field}", strict=False)
            if not parsed:
                decode_errors.append(f"plan.{field}_not_json_object")
            plan[field] = parsed
    contract = dict(plan.get("policy_adoption_contract") or {})
    for field in ("risk_plan", "profit_plan"):
        raw = contract.get(field)
        if isinstance(raw, dict) or raw in (None, ""):
            contract[field] = dict(raw or {})
        else:
            parsed = _json_object(raw, field=f"KR_POLICY_ADOPTION_contract_{field}", strict=False)
            if not parsed:
                decode_errors.append(f"contract.{field}_not_json_object")
            contract[field] = parsed
    plan["policy_adoption_contract"] = contract
    p["entry_exit_plan_json"] = plan
    return p, decode_errors


def _adoption_verification_reasons(position: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
    import trader.kr.market_state_overlay as overlay

    p, reasons = _normalized_adoption_position(position)
    plan = dict(p.get("entry_exit_plan_json") or {})
    meta = dict(p.get("position_meta") or {})
    contract = dict(plan.get("policy_adoption_contract") or {})
    cycle_id = str(p.get("position_cycle_id") or "")
    epoch_id = str(p.get("portfolio_epoch_id") or "")
    digest = str(contract.get("sha256") or "")
    code = str(p.get("code") or p.get("symbol") or "").zfill(6)

    if not cycle_id:
        reasons.append("position_cycle_id_missing")
    if not epoch_id:
        reasons.append("portfolio_epoch_id_missing")
    if str(p.get("policy_version") or "") != overlay.KR_POLICY_MISSING_ADOPTION_VERSION:
        reasons.append("position.policy_version")
    if str(p.get("policy_source") or "") != overlay.KR_POLICY_MISSING_ADOPTION_SOURCE:
        reasons.append("position.policy_source")
    if str(p.get("exit_policy_family") or "") != "SWING_STAGED_EXIT":
        reasons.append("position.exit_policy_family")
    if str(plan.get("policy_version") or "") != overlay.KR_POLICY_MISSING_ADOPTION_VERSION:
        reasons.append("plan.policy_version")
    if str(plan.get("policy_source") or "") != overlay.KR_POLICY_MISSING_ADOPTION_SOURCE:
        reasons.append("plan.policy_source")
    if str(plan.get("exit_policy_family") or "") != "SWING_STAGED_EXIT":
        reasons.append("plan.exit_policy_family")
    if str(contract.get("version") or "") != overlay.KR_POLICY_MISSING_ADOPTION_VERSION:
        reasons.append("contract.version")
    if str(contract.get("code") or "").zfill(6) != code:
        reasons.append("contract.code")
    if str(contract.get("position_cycle_id") or "") != cycle_id:
        reasons.append("contract.position_cycle_id")
    if str(contract.get("portfolio_epoch_id") or "") != epoch_id:
        reasons.append("contract.portfolio_epoch_id")

    contract_price = _canonical_price(contract.get("entry_price"))
    row_price = _canonical_price(
        p.get("avg_buy_price") if p.get("avg_buy_price") not in (None, "")
        else p.get("avg") if p.get("avg") not in (None, "")
        else p.get("entry_price")
    )
    if contract_price is None or row_price is None or contract_price != row_price:
        reasons.append("contract.entry_price")
    if contract.get("risk_plan") != plan.get("risk_plan"):
        reasons.append("contract.risk_plan")
    if contract.get("profit_plan") != plan.get("profit_plan"):
        reasons.append("contract.profit_plan")
    if not digest:
        reasons.append("contract.sha256_missing")
    elif digest != overlay._kr_policy_missing_adoption_sha256(contract):
        reasons.append("contract.sha256_mismatch")
    if meta.get("policy_adopted") is not True:
        reasons.append("position_meta.policy_adopted")
    if str(meta.get("policy_adoption_version") or "") != overlay.KR_POLICY_MISSING_ADOPTION_VERSION:
        reasons.append("position_meta.policy_adoption_version")
    if str(meta.get("policy_adoption_sha256") or "") != digest:
        reasons.append("position_meta.policy_adoption_sha256")
    return sorted(set(reasons)), p


def _install_policy_adoption_roundtrip_guard() -> None:
    import trader.kr.market_state_overlay as overlay
    import trader.pb1_engine as pb1_engine

    if getattr(overlay, "_pr132_adoption_roundtrip_guard_installed", False):
        return
    original_build = overlay.build_kr_policy_missing_adoption
    original_claim = overlay.has_kr_policy_missing_adoption_claim

    @functools.wraps(original_build)
    def _build(position: dict, *, current_price: float | None = None):
        normalized, _ = _normalized_adoption_position(dict(position or {}))
        return original_build(normalized, current_price=current_price)

    @functools.wraps(original_claim)
    def _claim(position: dict) -> bool:
        normalized, _ = _normalized_adoption_position(dict(position or {}))
        return bool(original_claim(normalized))

    def _verified(position: dict) -> bool:
        reasons, normalized = _adoption_verification_reasons(dict(position or {}))
        code = str(normalized.get("code") or normalized.get("symbol") or "").zfill(6)
        if reasons:
            logger.error(
                "[KR_POLICY_ADOPTION][VERIFY_DETAIL] code=%s result=BLOCK reasons=%s plan_type=%s meta_type=%s cycle=%s epoch=%s",
                code,
                reasons,
                type((position or {}).get("entry_exit_plan_json")).__name__,
                type((position or {}).get("position_meta")).__name__,
                normalized.get("position_cycle_id"),
                normalized.get("portfolio_epoch_id"),
            )
            return False
        logger.info("[KR_POLICY_ADOPTION][VERIFY_DETAIL] code=%s result=VERIFIED", code)
        return True

    overlay.build_kr_policy_missing_adoption = _build
    overlay.has_kr_policy_missing_adoption_claim = _claim
    overlay.is_verified_kr_policy_missing_adoption = _verified

    # PB1 imported these symbols by value before PR131 installed its overlay
    # wrapper. Replace the live PB1 bindings as well; otherwise production keeps
    # calling the old verifier even though overlay unit tests pass.
    pb1_engine.build_kr_policy_missing_adoption = _build
    pb1_engine.has_kr_policy_missing_adoption_claim = _claim
    pb1_engine.is_verified_kr_policy_missing_adoption = _verified
    overlay._pr132_adoption_roundtrip_guard_installed = True


def _minimum_preexisting_age(context: Any) -> Any:
    """Unknown entry date is not evidence of a same-day BUY.

    A current-day BUY has durable fill evidence and therefore an entry_date in
    the normal path. Imported/recovered broker holdings with no BUY fill are
    marked as pre-existing with a conservative minimum lifecycle age of one
    trading day. We do not invent holding minutes or an entry timestamp.
    """
    if context is None or getattr(context, "entry_date", None):
        return context
    if int(getattr(context, "holding_qty", 0) or 0) <= 0:
        return context
    meta = getattr(context, "position_meta", None)
    if not isinstance(meta, dict):
        meta = {}
        try:
            context.position_meta = meta
        except Exception:
            return context
    origin = str(meta.get("position_origin") or "").upper()
    preexisting = bool(
        meta.get("holding_age_unknown")
        or meta.get("policy_adopted") is True
        or str(meta.get("policy_adopted_from") or "").upper() == "POLICY_MISSING"
        or origin in {"IMPORTED", "RECOVERY"}
        or str(meta.get("import_source") or "").upper() == "KIS_HOLDING"
    )
    if not preexisting:
        return context
    for attr in ("days_held", "trading_days_held", "calendar_days_held", "holding_bars"):
        try:
            setattr(context, attr, max(1, int(getattr(context, attr, 0) or 0)))
        except Exception:
            pass
    meta["holding_age_unknown"] = True
    meta["preexisting_position_without_entry_fill"] = True
    meta["same_day_entry_provenance"] = "NO_CURRENT_DAY_BUY_FILL"
    logger.info(
        "[KR_POSITION_AGE][PREEXISTING_UNKNOWN] code=%s action=not_same_day source=%s",
        getattr(context, "code", ""), origin or meta.get("import_source") or "legacy",
    )
    return context


def _install_legacy_position_age_guard() -> None:
    import trader.pb1_engine as pb1_engine

    if getattr(pb1_engine.PB1Engine, "_pr132_legacy_age_guard_installed", False):
        return
    for method_name in (
        "_build_holding_contexts_from_balance_rows",
        "_build_holding_contexts_from_position_rows",
        "_build_holding_contexts_from_fill_reconstruction",
        "_build_holding_contexts_from_test_rows",
    ):
        original = getattr(pb1_engine.PB1Engine, method_name, None)
        if original is None:
            continue

        @functools.wraps(original)
        def _wrapped(self, *args, __original=original, **kwargs):
            contexts = __original(self, *args, **kwargs)
            return [_minimum_preexisting_age(ctx) for ctx in (contexts or [])]

        setattr(pb1_engine.PB1Engine, method_name, _wrapped)
    pb1_engine.PB1Engine._pr132_legacy_age_guard_installed = True


def install_kr_20260917_runtime_integrity() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _install_kr_buy_json_roundtrip_guard()
    _install_policy_adoption_roundtrip_guard()
    _install_legacy_position_age_guard()
    _INSTALLED = True
    logger.info("[KR_RUNTIME_INTEGRITY_20260917][INSTALLED]")
