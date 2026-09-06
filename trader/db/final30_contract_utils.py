from __future__ import annotations

from typing import Any, Dict, Iterable, List

from trader.final30_quality import normalize_final30_contract_row
from trader.indicators import safe_nullable_float


def contract_value_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def field_null_counts(rows: List[Dict[str, Any]], fields: Iterable[str]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for field in fields:
        counts[str(field)] = sum(1 for row in (rows or []) if contract_value_missing((row or {}).get(str(field))))
    return counts


def normalize_final30_score_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    canonical = safe_nullable_float(row.get("score_final"))
    if canonical is None:
        canonical = safe_nullable_float(row.get("final_score"))
    if canonical is None:
        canonical = safe_nullable_float(row.get("score"))
    canonical_value = float(canonical) if canonical is not None else None
    row["score"] = canonical_value
    row["score_final"] = canonical_value
    row["final_score"] = canonical_value
    meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
    meta["score"] = canonical_value
    meta["score_final"] = canonical_value
    meta["final_score"] = canonical_value
    row["meta"] = meta
    return row


def roundtrip_value_matches(lhs: Any, rhs: Any) -> bool:
    lhs_num = safe_nullable_float(lhs)
    rhs_num = safe_nullable_float(rhs)
    if lhs_num is not None or rhs_num is not None:
        if lhs_num is None or rhs_num is None:
            return False
        return abs(float(lhs_num) - float(rhs_num)) < 1e-9
    if contract_value_missing(lhs) and contract_value_missing(rhs):
        return True
    return lhs == rhs


def roundtrip_mismatch_counts(
    source_rows: List[Dict[str, Any]],
    loaded_rows: List[Dict[str, Any]],
    *,
    fields: Iterable[str],
) -> Dict[str, int]:
    source_by_code = {
        str((row or {}).get("code") or "").zfill(6): normalize_final30_contract_row(row)
        for row in (source_rows or [])
        if (row or {}).get("code")
    }
    loaded_by_code = {
        str((row or {}).get("code") or "").zfill(6): normalize_final30_contract_row(row)
        for row in (loaded_rows or [])
        if (row or {}).get("code")
    }
    codes = sorted(set(source_by_code) | set(loaded_by_code))
    mismatch_counts: Dict[str, int] = {}
    for field in fields:
        mismatch_counts[str(field)] = sum(
            1
            for code in codes
            if not roundtrip_value_matches(
                (source_by_code.get(code) or {}).get(str(field)),
                (loaded_by_code.get(code) or {}).get(str(field)),
            )
        )
    return mismatch_counts
