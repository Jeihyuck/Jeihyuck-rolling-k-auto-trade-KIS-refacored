from __future__ import annotations

from typing import Any

import pandas as pd


def as_dataframe(value: Any) -> pd.DataFrame:
    if value is None:
        return pd.DataFrame()
    if isinstance(value, pd.DataFrame):
        return value
    if isinstance(value, list):
        return pd.DataFrame(value)
    if isinstance(value, tuple):
        return pd.DataFrame(list(value))
    if isinstance(value, dict):
        return pd.DataFrame([value])
    try:
        return pd.DataFrame(value)
    except Exception:
        return pd.DataFrame()
