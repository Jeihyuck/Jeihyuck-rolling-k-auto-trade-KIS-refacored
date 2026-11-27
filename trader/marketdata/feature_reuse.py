"""Bounded process-local reuse of exact static native-bar features.

First step of the V1.2 feature engine, not a cross-process data service.
Only pure functions are eligible. Never cache an order decision, price age,
account state, or native proof. Namespaced settings must include formula
version and parameters; callers scope by verified owner/symbol/account.
Changed/corrected/missing rows are distinct keys, regardless of timestamp.
"""
from collections import OrderedDict
from copy import deepcopy
from hashlib import sha256
import json
from threading import RLock

_MAX_ENTRIES = 128
_cache = OrderedDict()
_lock = RLock()
_hits = _misses = 0


def cached_native_features(*, market, scope, trade_date, settings, rows, compute):
    global _hits, _misses
    if market not in {"KR", "US"} or not scope or not all(scope):
        return compute(rows)
    try:
        encoded = json.dumps(rows, sort_keys=True, separators=(",", ":"), allow_nan=False)
        key = (market, tuple(scope), trade_date, tuple(settings), sha256(encoded.encode()).hexdigest())
        hash(key)
    except (ValueError, TypeError):
        return compute(rows)
    with _lock:
        if key in _cache:
            _hits += 1
            _cache.move_to_end(key)
            return deepcopy(_cache[key])
        _misses += 1
    # Compute outside the lock: no broker/DB access or head-of-line blocking.
    value = compute(rows)
    with _lock:
        _cache[key] = deepcopy(value)
        _cache.move_to_end(key)
        while len(_cache) > _MAX_ENTRIES:
            _cache.popitem(last=False)
    return value


def feature_cache_stats():
    with _lock:
        return {"hits": _hits, "misses": _misses, "entries": len(_cache)}


def clear_feature_cache():
    global _hits, _misses
    with _lock:
        _cache.clear()
        _hits = _misses = 0
