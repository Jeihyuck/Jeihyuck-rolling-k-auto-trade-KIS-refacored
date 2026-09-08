from __future__ import annotations

from typing import Any


def sanitize_balance_snapshot(snapshot: dict | None) -> dict:
    return _sanitize_value(snapshot or {})


def extract_dnca_tot_amt(balance_resp: dict | None) -> int | None:
    if not isinstance(balance_resp, dict):
        return None
    out2 = balance_resp.get("output2")
    if isinstance(out2, list) and out2:
        row = out2[0]
        if isinstance(row, dict) and row:
            value = row.get("dnca_tot_amt")
            if value is not None and str(value).strip() != "":
                return int(float(str(value).replace(",", "")))
        if isinstance(row, dict) and not row:
            return None
    if isinstance(out2, dict) and out2:
        value = out2.get("dnca_tot_amt")
        if value is not None and str(value).strip() != "":
            return int(float(str(value).replace(",", "")))
    return None


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sanitize_value(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    if value is None:
        return None
    return "****"
