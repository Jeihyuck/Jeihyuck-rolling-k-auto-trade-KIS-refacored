from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import math
from typing import Any

import numpy as np


def is_bad_float(value: Any) -> bool:
    if not isinstance(value, float):
        return False
    return math.isnan(value) or math.isinf(value)


def _is_numpy_scalar(value: Any) -> bool:
    return isinstance(value, np.generic)


def to_jsonable(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, int, bool)):
        return obj
    if isinstance(obj, float):
        return None if is_bad_float(obj) else obj
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if _is_numpy_scalar(obj):
        return to_jsonable(obj.item())
    if isinstance(obj, dict):
        sanitized: dict[Any, Any] = {}
        for key, value in obj.items():
            sanitized_key = to_jsonable(key)
            if not isinstance(sanitized_key, (str, int, float, bool)) and sanitized_key is not None:
                sanitized_key = str(sanitized_key)
            sanitized[sanitized_key] = to_jsonable(value)
        return sanitized
    if isinstance(obj, (list, tuple, set)):
        return [to_jsonable(item) for item in obj]
    return str(obj)


def json_safe(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat") and callable(getattr(value, "isoformat")):
        try:
            return value.isoformat()
        except Exception:
            pass
    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {str(key): json_safe(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    return str(value)
