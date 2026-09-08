"""Fail-closed reconstruction of the current KR PB1 position lifecycle.

Only authoritative broker balance plus persisted fills scoped to the PB1
account/strategy may prove provenance.  The replay starts after the most recent
flat point.  No symbol-specific policy exceptions are permitted.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Mapping


class Confidence(str, Enum):
    CONFIRMED = "CONFIRMED"
    PARTIAL = "PARTIAL"
    AMBIGUOUS = "AMBIGUOUS"
    UNRECOVERABLE = "UNRECOVERABLE"


@dataclass(frozen=True)
class ProvenanceAudit:
    code: str
    confidence: Confidence
    reason: str
    broker_qty: int
    broker_avg: float
    reconstructed_qty: int = 0
    reconstructed_avg: float | None = None
    original_buy_id: str | None = None
    updates: dict[str, Any] | None = None


def _side(row: Mapping[str, Any]) -> str:
    return str(row.get("side") or "").strip().upper()


def _qty(row: Mapping[str, Any]) -> int:
    try:
        return max(0, int(float(row.get("qty") or row.get("filled_qty") or 0)))
    except (TypeError, ValueError):
        return 0


def _price(row: Mapping[str, Any]) -> float:
    try:
        return float(row.get("price") or row.get("fill_price") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _meta(row: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in ("order_entry_meta_json", "fill_meta_json", "entry_meta", "meta"):
        value = row.get(key)
        if isinstance(value, Mapping):
            out.update(dict(value))
    request = row.get("request_json")
    if isinstance(request, Mapping):
        if isinstance(request.get("entry_meta"), Mapping):
            out.update(dict(request["entry_meta"]))
        if isinstance(request.get("entry_exit_plan"), Mapping):
            out["entry_exit_plan"] = dict(request["entry_exit_plan"])
    raw = row.get("raw_json")
    if isinstance(raw, Mapping):
        if isinstance(raw.get("entry_meta"), Mapping):
            out.update(dict(raw["entry_meta"]))
        if isinstance(raw.get("entry_exit_plan"), Mapping):
            out["entry_exit_plan"] = dict(raw["entry_exit_plan"])
    for key in (
        "entry_reason", "entry_style_selected", "entry_decision_family",
        "stop_price_at_entry", "pivot_price_at_entry", "entry_rule_version",
    ):
        if row.get(key) is not None:
            out.setdefault(key, row.get(key))
    return out


def audit_position(
    *,
    code: str,
    broker_qty: int,
    broker_avg: float,
    fills: Iterable[Mapping[str, Any]],
    avg_tolerance_pct: float = 1.0,
) -> ProvenanceAudit:
    """Replay fills after the last flat and prove the current lifecycle."""
    code_n = str(code or "").zfill(6)
    qty_now = max(0, int(broker_qty or 0))
    avg_now = float(broker_avg or 0.0)
    if qty_now <= 0 or avg_now <= 0:
        return ProvenanceAudit(
            code_n, Confidence.UNRECOVERABLE, "BROKER_POSITION_NOT_OPEN", qty_now, avg_now
        )

    rows = [
        dict(row) for row in fills
        if str(row.get("code") or "").zfill(6) == code_n
        and _side(row) in {"BUY", "SELL"}
        and _qty(row) > 0
    ]
    rows.sort(key=lambda row: str(
        row.get("filled_at") or row.get("created_at") or row.get("trade_date") or ""
    ))
    if not rows:
        return ProvenanceAudit(
            code_n, Confidence.UNRECOVERABLE, "NO_SCOPED_FILL_HISTORY", qty_now, avg_now
        )

    net = 0
    active_start = 0
    for index, row in enumerate(rows):
        net += _qty(row) if _side(row) == "BUY" else -_qty(row)
        if net < 0:
            return ProvenanceAudit(
                code_n, Confidence.AMBIGUOUS, "NEGATIVE_FILL_CHAIN", qty_now, avg_now
            )
        if net == 0:
            active_start = index + 1

    active = rows[active_start:]
    buys = [row for row in active if _side(row) == "BUY" and _price(row) > 0]
    if not buys:
        return ProvenanceAudit(
            code_n, Confidence.UNRECOVERABLE, "NO_CURRENT_LIFECYCLE_BUY",
            qty_now, avg_now
        )

    reconstructed_qty = 0
    cost_basis = 0.0
    for row in active:
        row_qty = _qty(row)
        if _side(row) == "BUY":
            px = _price(row)
            if px <= 0:
                return ProvenanceAudit(
                    code_n, Confidence.AMBIGUOUS, "BUY_PRICE_MISSING",
                    qty_now, avg_now, reconstructed_qty
                )
            cost_basis += row_qty * px
            reconstructed_qty += row_qty
        else:
            if row_qty > reconstructed_qty:
                return ProvenanceAudit(
                    code_n, Confidence.AMBIGUOUS, "NEGATIVE_ACTIVE_CHAIN",
                    qty_now, avg_now, reconstructed_qty
                )
            running_avg = cost_basis / reconstructed_qty if reconstructed_qty else 0.0
            cost_basis -= row_qty * running_avg
            reconstructed_qty -= row_qty
            if reconstructed_qty == 0:
                cost_basis = 0.0

    reconstructed_avg = (
        cost_basis / reconstructed_qty if reconstructed_qty > 0 else None
    )
    if reconstructed_qty != qty_now:
        return ProvenanceAudit(
            code_n, Confidence.AMBIGUOUS, "QTY_MISMATCH",
            qty_now, avg_now, reconstructed_qty, reconstructed_avg
        )
    avg_diff_pct = (
        abs(float(reconstructed_avg or 0.0) - avg_now) / avg_now * 100.0
        if avg_now > 0 else 100.0
    )
    if reconstructed_avg is None or avg_diff_pct > float(avg_tolerance_pct):
        return ProvenanceAudit(
            code_n, Confidence.AMBIGUOUS, "AVG_MISMATCH",
            qty_now, avg_now, reconstructed_qty, reconstructed_avg
        )

    first_buy = buys[0]
    meta = _meta(first_buy)
    entry_reason = meta.get("entry_reason")
    entry_style = meta.get("entry_style_selected") or entry_reason
    plan = meta.get("entry_exit_plan")
    if not entry_reason or not entry_style:
        return ProvenanceAudit(
            code_n, Confidence.PARTIAL, "ENTRY_METADATA_MISSING",
            qty_now, avg_now, reconstructed_qty, reconstructed_avg
        )
    if not isinstance(plan, Mapping) or not plan.get("exit_policy_family"):
        return ProvenanceAudit(
            code_n, Confidence.PARTIAL, "ORIGINAL_ENTRY_EXIT_PLAN_MISSING",
            qty_now, avg_now, reconstructed_qty, reconstructed_avg
        )

    opened = first_buy.get("filled_at") or first_buy.get("created_at")
    if not opened:
        return ProvenanceAudit(
            code_n, Confidence.PARTIAL, "ENTRY_TIMESTAMP_MISSING",
            qty_now, avg_now, reconstructed_qty, reconstructed_avg
        )

    plan_dict = dict(plan)
    position_meta = {
        "holding_age_unknown": False,
        "provenance_verified": True,
        "provenance_confidence": Confidence.CONFIRMED.value,
        "provenance_reason": "EXACT_LAST_FLAT_FILL_REPLAY",
        "provenance_original_buy_id": str(
            first_buy.get("fill_id") or first_buy.get("order_id") or ""
        ),
        "tp1_done": False,
        "tp2_done": False,
        "trail_eligible": False,
    }
    updates: dict[str, Any] = {
        "entry_ts": str(opened),
        "entry_reason": str(entry_reason),
        "entry_style_selected": str(entry_style),
        "entry_thesis": plan_dict.get("entry_thesis"),
        "trade_horizon": plan_dict.get("trade_horizon"),
        "exit_policy_family": plan_dict.get("exit_policy_family"),
        "eod_action": plan_dict.get("eod_action"),
        "force_eod_close": bool(plan_dict.get("force_eod_close", False)),
        "entry_exit_plan_json": plan_dict,
        "policy_source": "RECOVERED_CONFIRMED_FILL_CHAIN",
        "policy_version": plan_dict.get("policy_version"),
        "entry_meta_json": dict(meta),
        "stop_price_at_entry": meta.get("stop_price_at_entry"),
        "pivot_price_at_entry": meta.get("pivot_price_at_entry"),
        "initial_stop": (
            meta.get("initial_stop")
            or (plan_dict.get("risk_plan") or {}).get("initial_stop")
        ),
        "position_meta": position_meta,
    }
    return ProvenanceAudit(
        code_n,
        Confidence.CONFIRMED,
        "EXACT_LAST_FLAT_FILL_REPLAY",
        qty_now,
        avg_now,
        reconstructed_qty,
        reconstructed_avg,
        str(first_buy.get("fill_id") or first_buy.get("order_id") or ""),
        updates,
    )
