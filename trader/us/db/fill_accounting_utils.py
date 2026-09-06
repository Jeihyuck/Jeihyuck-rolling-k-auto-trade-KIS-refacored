from __future__ import annotations

import json


def _meta_dict(value: object) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def qty_from_accounting_rows(rows: list[dict]) -> tuple[int, int, int]:
    execution_qty = 0
    cumulative_qty = 0
    synthetic_qty = 0
    for fill in rows:
        meta = _meta_dict(fill.get("meta"))
        if meta.get("accounting_active") is False:
            continue
        qty = int(fill.get("qty") or 0)
        evidence = str(meta.get("fill_evidence_type") or fill.get("fill_evidence_type") or "")
        if meta.get("is_synthetic") or meta.get("synthetic") or meta.get("synthetic_fill") or evidence in {"BALANCE_DELTA_SYNTHETIC", "LEGACY_SYNTHETIC"}:
            synthetic_qty = max(synthetic_qty, int(meta.get("cumulative_filled_qty") or qty or 0))
        elif evidence in {"KIS_EXECUTION_ACTUAL", "KIS_ACTUAL"}:
            execution_qty += qty
        elif evidence in {"KIS_ORDER_CUMULATIVE_ACTUAL", "KIS_ORDER_DETAIL_ACTUAL"}:
            cumulative_qty = max(cumulative_qty, int(meta.get("cumulative_filled_qty") or qty or 0))
        else:
            cumulative_qty = max(cumulative_qty, int(meta.get("cumulative_filled_qty") or qty or 0))
    if execution_qty > 0:
        return execution_qty, 0, execution_qty
    if cumulative_qty > 0:
        return cumulative_qty, 0, cumulative_qty
    return 0, synthetic_qty, synthetic_qty
