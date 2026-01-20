from __future__ import annotations

from datetime import date, datetime
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
