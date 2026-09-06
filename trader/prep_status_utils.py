from __future__ import annotations

from typing import Any


def resolve_prep_final_status(*, prep_core: dict[str, Any], aux_failures: list[dict[str, Any]]) -> tuple[str, int]:
    if int((prep_core or {}).get("core_ok") or 0) and not aux_failures:
        return "OK", 0
    if int((prep_core or {}).get("core_ok") or 0) and aux_failures:
        return "OK_CORE_AUX_DEGRADED", 0
    if "quality_not_ok" in list((prep_core or {}).get("reasons") or []):
        return "FAIL_CORE_QUALITY", 2
    return "FAIL_CORE_CONTRACT", 2
