"""2026-09-15 KR production-integrity guards.

These guards are deliberately execution/persistence only.  They do not change
PB1 TP/SL thresholds, KR Infinite unit sizing, or regime policy.

Closed gaps:
* POLICY_MISSING adoption verification survives float/DB round trips.
* PB1 SELL baselines are read-back verified before broker submission and are
  mirrored into broker response evidence for reconciliation.
* Legacy FULL_EXIT SELL rows with a lost baseline may recover it only from one
  exact OPEN lifecycle whose quantity equals the submitted full-exit quantity.
* PB1 session metrics count only orders tagged to the current AM/PM/CLOSE
  session instead of carrying an earlier session's ACK into CLOSE.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import functools
import json
import logging
import os
from typing import Any

import sqlalchemy as sa

from trader.db.json_safe import json_sanitize
from trader.db.repos import OrdersRepo
from trader.db.schema import schema_for_engine

logger = logging.getLogger(__name__)
_INSTALLED = False
_PRICE_QUANT = Decimal("0.000001")
_BASELINE_FIELDS = (
    "pre_order_holding_qty",
    "pre_order_orderable_qty",
    "pre_order_avg_price",
    "requested_qty",
    "submitted_qty",
    "balance_snapshot_id",
    "order_intent_ts",
)


def _json_dict(value: Any) -> dict[str, Any]:
    """Normalize JSON/JSONB read-back without discarding durable evidence."""
    if isinstance(value, dict):
        return dict(value)
    raw = value
    if isinstance(raw, memoryview):
        raw = raw.tobytes()
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = bytes(raw).decode("utf-8")
        except Exception:
            return {}
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        try:
            decoded = json.loads(text)
        except Exception:
            return {}
        return dict(decoded) if isinstance(decoded, dict) else {}
    return {}


def _canonical_price(value: Any) -> Decimal | None:
    """Canonicalize an average price without trusting binary float identity."""
    try:
        result = Decimal(str(value)).quantize(_PRICE_QUANT, rounding=ROUND_HALF_UP)
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _install_policy_adoption_price_guard() -> None:
    import trader.kr.market_state_overlay as overlay

    if getattr(overlay, "_pr131_adoption_price_guard_installed", False):
        return
    original = overlay.is_verified_kr_policy_missing_adoption

    @functools.wraps(original)
    def _verified(position: dict) -> bool:
        p = dict(position or {})
        plan = _json_dict(p.get("entry_exit_plan_json"))
        contract = _json_dict(plan.get("policy_adoption_contract"))
        contract_price = _canonical_price(contract.get("entry_price"))
        row_raw = p.get("avg_buy_price")
        if row_raw in (None, ""):
            row_raw = p.get("avg")
        if row_raw in (None, ""):
            row_raw = p.get("entry_price")
        row_price = _canonical_price(row_raw)
        if contract_price is None or row_price is None or contract_price != row_price:
            return False

        # Preserve every existing policy/hash/lifecycle validation in the
        # canonical verifier; replace only the field used by its brittle float
        # equality so binary DB round-trip noise cannot invalidate the proof.
        normalized = dict(p)
        canonical_float = float(contract_price)
        if "avg_buy_price" in normalized or "avg" not in normalized:
            normalized["avg_buy_price"] = canonical_float
        if "avg" in normalized:
            normalized["avg"] = canonical_float
        if "entry_price" in normalized:
            normalized["entry_price"] = canonical_float
        return bool(original(normalized))

    overlay.is_verified_kr_policy_missing_adoption = _verified
    overlay._pr131_adoption_price_guard_installed = True


def _is_protected_pb1_sell(*, strategy: Any, side: Any, request: dict[str, Any]) -> bool:
    return bool(
        str(strategy or "") == "pb1_pullback_close"
        and str(side or "").upper() == "SELL"
        and str(request.get("strategy_owner") or "").upper() == "KR_STANDARD"
        and str(request.get("exit_stage") or "").strip()
    )


def _baseline_evidence(request: Any) -> dict[str, Any]:
    request_dict = _json_dict(request)
    result = {key: request_dict.get(key) for key in _BASELINE_FIELDS if key in request_dict}
    return result


def _assert_sell_baseline(request: Any, *, key: str) -> None:
    data = _json_dict(request)
    missing: list[str] = []
    pre_qty = data.get("pre_order_holding_qty")
    requested = data.get("requested_qty")
    submitted = data.get("submitted_qty")
    try:
        if pre_qty is None or int(float(pre_qty)) <= 0:
            missing.append("pre_order_holding_qty")
    except Exception:
        missing.append("pre_order_holding_qty")
    try:
        if int(float(requested or 0)) <= 0:
            missing.append("requested_qty")
    except Exception:
        missing.append("requested_qty")
    try:
        if int(float(submitted or 0)) <= 0:
            missing.append("submitted_qty")
    except Exception:
        missing.append("submitted_qty")
    if missing:
        raise RuntimeError(f"KR_SELL_BASELINE_INVALID:{key}:{','.join(sorted(set(missing)))}")


def _merge_baseline_into_response(response: Any, request: Any, *, row: Any = None) -> dict[str, Any]:
    """Mirror immutable order evidence into broker response for restart recovery.

    The response copy is redundancy only: request_json remains authoritative.
    BUY contracts are hashed/validated by OrdersRepo before broker submission.
    """
    out = _json_dict(response)
    request_dict = _json_dict(request)
    evidence = _baseline_evidence(request_dict)
    if evidence and evidence.get("pre_order_holding_qty") is not None:
        for key, value in evidence.items():
            out.setdefault(key, value)
        execution = _json_dict(out.get("_order_execution"))
        for key, value in evidence.items():
            execution.setdefault(key, value)
        execution.setdefault("baseline_contract", "KR_ORDER_BASELINE_V1")
        out["_order_execution"] = execution

    if request_dict.get("enforce_entry_contract") is True:
        row_dict = dict(row or {}) if isinstance(row, dict) else {}
        out["_kr_buy_entry_contract"] = {
            "version": "KR_BUY_ENTRY_CONTRACT_RESPONSE_V1",
            "code": str(row_dict.get("code") or "").zfill(6),
            "side": str(row_dict.get("side") or "BUY").upper(),
            "order_id": str(row_dict.get("order_id") or ""),
            "client_order_key": str(request_dict.get("client_order_key") or row_dict.get("client_order_key") or ""),
            "position_cycle_id": str(request_dict.get("position_cycle_id") or row_dict.get("position_cycle_id") or ""),
            "portfolio_epoch_id": str(request_dict.get("portfolio_epoch_id") or row_dict.get("portfolio_epoch_id") or ""),
            "entry_contract_sha256": str(request_dict.get("entry_contract_sha256") or ""),
            "request": json_sanitize(request_dict),
        }
    return json_sanitize(out)


def _recover_baseline_from_response(row: dict[str, Any]) -> dict[str, Any]:
    request = _json_dict(row.get("request_json"))
    if request.get("pre_order_holding_qty") is not None:
        return request
    response = _json_dict(row.get("response_json"))
    execution = _json_dict(response.get("_order_execution"))
    for source in (execution, response):
        if source.get("pre_order_holding_qty") is None:
            continue
        recovered = dict(request)
        for key in _BASELINE_FIELDS:
            if key in source and recovered.get(key) is None:
                recovered[key] = source.get(key)
        recovered["recovered_baseline_source"] = "BROKER_RESPONSE_REDUNDANCY"
        return recovered
    return request


def _recover_full_exit_baseline_from_exact_lifecycle(
    repo: OrdersRepo, row: dict[str, Any], request: dict[str, Any]
) -> dict[str, Any]:
    """Recover only a lost FULL_EXIT baseline from one exact OPEN lifecycle.

    This is intentionally not available to partial TP/trim orders.  The order
    must already carry broker-submission evidence and its submitted quantity
    must equal the exact lifecycle's current DB quantity.
    """
    if request.get("pre_order_holding_qty") is not None:
        return request
    if str(row.get("side") or "").upper() != "SELL":
        return request
    if str(row.get("strategy") or "") != "pb1_pullback_close":
        return request
    if str(row.get("stage") or "").upper() != "FULL_EXIT":
        return request
    if not str(row.get("kis_odno") or row.get("broker_order_id") or "").strip():
        return request
    cycle = str(row.get("position_cycle_id") or "").strip()
    epoch = str(row.get("portfolio_epoch_id") or "").strip()
    code = str(row.get("code") or "").zfill(6)
    submitted = int(float(request.get("submitted_qty") or row.get("qty") or 0))
    if not cycle or not epoch or not code or submitted <= 0:
        return request

    schema = schema_for_engine(repo.engine)
    with repo.engine.connect() as conn:
        matches = [
            dict(item)
            for item in conn.execute(
                sa.select(schema.positions).where(
                    sa.and_(
                        schema.positions.c.env == row.get("env"),
                        schema.positions.c.strategy == row.get("strategy"),
                        schema.positions.c.sid == int(row.get("sid") or 1),
                        schema.positions.c.mode == int(row.get("mode") or 1),
                        schema.positions.c.code == code,
                        schema.positions.c.position_cycle_id == cycle,
                        schema.positions.c.portfolio_epoch_id == epoch,
                        schema.positions.c.status == "OPEN",
                    )
                )
            ).mappings().all()
        ]
    if len(matches) != 1 or int(matches[0].get("qty") or 0) != submitted:
        return request

    position = matches[0]
    recovered = dict(request)
    recovered.update(
        {
            "pre_order_holding_qty": submitted,
            "pre_order_orderable_qty": submitted,
            "pre_order_avg_price": float(position.get("avg_buy_price") or 0.0),
            "requested_qty": int(request.get("requested_qty") or submitted),
            "submitted_qty": submitted,
            "recovered_baseline_source": "EXACT_OPEN_LIFECYCLE_FULL_EXIT",
        }
    )
    logger.error(
        "[KR_SELL_BASELINE][RECOVERED] code=%s order_id=%s source=EXACT_OPEN_LIFECYCLE_FULL_EXIT qty=%s cycle=%s epoch=%s",
        code,
        row.get("order_id"),
        submitted,
        cycle,
        epoch,
    )
    return recovered


def _persist_recovered_request(repo: OrdersRepo, row: dict[str, Any], request: dict[str, Any]) -> None:
    order_id = row.get("order_id")
    if not order_id:
        return
    schema = schema_for_engine(repo.engine)
    with repo.engine.begin() as conn:
        conn.execute(
            sa.update(schema.orders)
            .where(schema.orders.c.order_id == order_id)
            .values(request_json=json_sanitize(request), updated_at=sa.func.now())
        )


def _install_sell_baseline_guard() -> None:
    if getattr(OrdersRepo, "_pr131_sell_baseline_guard_installed", False):
        return

    original_create = OrdersRepo.create_intent_idempotent
    original_submitted = OrdersRepo.mark_submitted
    original_acked = OrdersRepo.mark_acked
    original_get_open = OrdersRepo.get_open_orders

    @functools.wraps(original_create)
    def _create(self, *args, **kwargs):
        request = _json_dict(kwargs.get("request_json"))
        protected = _is_protected_pb1_sell(
            strategy=kwargs.get("strategy"), side=kwargs.get("side"), request=request
        )
        key = str(kwargs.get("client_order_key") or "")
        if protected:
            _assert_sell_baseline(request, key=key)
        order_id, created = original_create(self, *args, **kwargs)
        if protected and created:
            persisted = self.get_order_by_client_order_key(str(kwargs.get("env") or ""), key) or {}
            persisted_request = _json_dict(persisted.get("request_json"))
            _assert_sell_baseline(persisted_request, key=key)
            expected = _baseline_evidence(request)
            actual = _baseline_evidence(persisted_request)
            for field in ("pre_order_holding_qty", "requested_qty", "submitted_qty"):
                if str(expected.get(field)) != str(actual.get(field)):
                    raise RuntimeError(f"KR_SELL_BASELINE_PERSISTENCE_MISMATCH:{key}:{field}")
            logger.info("[KR_SELL_BASELINE][READBACK_OK] key=%s pre_qty=%s submitted_qty=%s",
                        key, actual.get("pre_order_holding_qty"), actual.get("submitted_qty"))
        return order_id, created

    @functools.wraps(original_submitted)
    def _submitted(self, env, client_order_key, kis_odno, response_json, *, entry_meta_json=None, submitted_qty=None):
        row = self.get_order_by_client_order_key(env, client_order_key) or {}
        request = _json_dict(row.get("request_json"))
        response_json = _merge_baseline_into_response(response_json, request, row=row)
        return original_submitted(
            self, env, client_order_key, kis_odno, response_json,
            entry_meta_json=entry_meta_json, submitted_qty=submitted_qty,
        )

    @functools.wraps(original_acked)
    def _acked(self, env, kis_odno, response_json, *, entry_meta_json=None):
        row = self.get_order_by_kis_odno(env, str(kis_odno or "")) or {}
        request = _json_dict(row.get("request_json"))
        response_json = _merge_baseline_into_response(response_json, request, row=row)
        return original_acked(self, env, kis_odno, response_json, entry_meta_json=entry_meta_json)

    @functools.wraps(original_get_open)
    def _get_open(self, *args, **kwargs):
        rows = [dict(row) for row in (original_get_open(self, *args, **kwargs) or [])]
        for row in rows:
            if str(row.get("side") or "").upper() != "SELL":
                continue
            request = _recover_baseline_from_response(row)
            request = _recover_full_exit_baseline_from_exact_lifecycle(self, row, request)
            if request != _json_dict(row.get("request_json")):
                _persist_recovered_request(self, row, request)
                row["request_json"] = request
        return rows

    OrdersRepo.create_intent_idempotent = _create
    OrdersRepo.mark_submitted = _submitted
    OrdersRepo.mark_acked = _acked
    OrdersRepo.get_open_orders = _get_open
    OrdersRepo._pr131_sell_baseline_guard_installed = True


def _normalize_session(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("_", "-")
    aliases = {
        "morning": "am",
        "trade-am": "am",
        "pm": "afternoon",
        "trade-pm": "afternoon",
        "trade-afternoon": "afternoon",
        "trade-close": "close",
    }
    return aliases.get(raw, raw)


def _install_session_metric_scope_guard() -> None:
    import trader.execution_state as execution_state

    if getattr(execution_state, "_pr131_session_metric_scope_installed", False):
        return
    original = execution_state.durable_order_metrics

    @functools.wraps(original)
    def _scoped(orders, fills):
        order_rows = [dict(row) for row in orders]
        fill_rows = [dict(row) for row in fills]
        current = _normalize_session(os.getenv("PB1_SESSION_KIND"))
        if current not in {"am", "afternoon", "close"}:
            return original(order_rows, fill_rows)

        tagged = [
            row for row in order_rows
            if _normalize_session(_json_dict(row.get("request_json")).get("trade_session"))
            in {"am", "afternoon", "close"}
        ]
        if not tagged:
            return original(order_rows, fill_rows)

        scoped = [
            row for row in order_rows
            if _normalize_session(_json_dict(row.get("request_json")).get("trade_session")) == current
        ]
        order_ids = {str(row.get("order_id")) for row in scoped if row.get("order_id")}
        scoped_fills = [row for row in fill_rows if str(row.get("order_id") or "") in order_ids]
        logger.info("[KR_SESSION_METRICS][SCOPE] session=%s orders_before=%s orders_after=%s fills_before=%s fills_after=%s",
                    current, len(order_rows), len(scoped), len(fill_rows), len(scoped_fills))
        return original(scoped, scoped_fills)

    execution_state.durable_order_metrics = _scoped
    execution_state._pr131_session_metric_scope_installed = True


def install_kr_20260915_runtime_integrity() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _install_policy_adoption_price_guard()
    _install_sell_baseline_guard()
    _install_session_metric_scope_guard()
    _INSTALLED = True
    logger.info("[KR_RUNTIME_INTEGRITY_20260915][INSTALLED]")
