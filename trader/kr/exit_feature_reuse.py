"""Bounded pure KR PB1 exit feature reuse; no position/order/risk cache."""
from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from hashlib import sha256
import math
import os
from threading import RLock

import pandas as pd

_CACHE = OrderedDict()
_LOCK = RLock()
_MAX_ENTRIES = 128


def cached_exit_features(*, df, code, trade_date, compute):
    """Exact same original calculator; content or config change invalidates.
    Cache only pure OHLCV-derived features, never an exit verdict or HWM.
    """
    try:
        key = (compute, str(code), str(trade_date),
               os.getenv("MA200_SLOPE_LOOKBACK", "20"),
               tuple(df.columns), tuple(str(x) for x in df.dtypes),
               sha256(pd.util.hash_pandas_object(df, index=True).values.tobytes()).hexdigest())
        with _LOCK:
            if key in _CACHE:
                _CACHE.move_to_end(key)
                return deepcopy(_CACHE[key]), True
    except (TypeError, ValueError, AttributeError, OverflowError):
        return compute(df), False
    result = compute(df)
    with _LOCK:
        _CACHE[key] = deepcopy(result)
        _CACHE.move_to_end(key)
        while len(_CACHE) > _MAX_ENTRIES:
            _CACHE.popitem(last=False)
    return result, False


def high_water_update_fields(*, persisted_max, computed_max, enabled):
    """Elide only a confirmed unchanged positive high-water value.
    Unknown/uninitialized values and new highs retain the original write.
    Does not cache or suppress any actual exit evaluation or stop initialization.
    """
    if enabled:
        try:
            old = float(persisted_max)
            new = float(computed_max)
            if math.isfinite(old) and math.isfinite(new) and old > 0 and old == new:
                return {}
        except (ValueError, TypeError, OverflowError):
            pass
    return {"max_price": computed_max}
