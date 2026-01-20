import json

import numpy as np

from trader.utils.json_sanitize import to_jsonable


def test_numpy_scalar_to_float() -> None:
    value = to_jsonable(np.float64(1.23))
    assert isinstance(value, float)
    assert value == 1.23


def test_nan_and_inf_to_none() -> None:
    assert to_jsonable(float("nan")) is None
    assert to_jsonable(float("inf")) is None
    assert to_jsonable(float("-inf")) is None


def test_recursive_conversion() -> None:
    payload = {
        "features": {
            "pullback_pct": np.float64(0.12),
            "vol_contraction": float("nan"),
        },
        "tags": {"a", "b"},
    }
    sanitized = to_jsonable(payload)
    assert sanitized["features"]["pullback_pct"] == 0.12
    assert sanitized["features"]["vol_contraction"] is None
    assert sorted(sanitized["tags"]) == ["a", "b"]
    json.dumps(sanitized)
