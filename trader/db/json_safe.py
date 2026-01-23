from __future__ import annotations

import json
import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def _is_pandas_timestamp(value: Any) -> bool:
    return value.__class__.__name__ in ("Timestamp", "NaTType")


def _to_iso(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def json_sanitize(obj: Any) -> Any:
    """
    JSON으로 안전하게 저장 가능한 타입만 남기도록 재귀 변환.
    - pandas.Timestamp -> ISO string
    - datetime/date -> ISO string
    - numpy scalar -> python scalar
    - Decimal -> float(또는 str 선택 가능)
    - NaN/Inf -> None
    """
    if obj is None:
        return None

    if _is_pandas_timestamp(obj):
        s = str(obj)
        return None if s == "NaT" else _to_iso(obj)

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    if isinstance(obj, Decimal):
        return float(obj)

    if isinstance(obj, (bool, int, str)):
        return obj

    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj

    if isinstance(obj, dict):
        return {str(k): json_sanitize(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple, set)):
        return [json_sanitize(v) for v in obj]

    if hasattr(obj, "item") and callable(getattr(obj, "item")):
        try:
            return json_sanitize(obj.item())
        except Exception:
            pass

    return str(obj)


def json_dumps_safe(obj: Any) -> str:
    return json.dumps(json_sanitize(obj), ensure_ascii=False, separators=(",", ":"))
