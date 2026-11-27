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


def shared_native_features(*, market, scope, trade_date, settings, rows, compute, base, advance=None, slide=None):
    """Bounded same-host, cross-process reuse of pure JSON feature outputs.

    One row per exact scoped formula; corrections/removals change the operand
    hash. Never cache a quote, position, decision, risk or broker authority.
    Unavailable/locked/corrupt cache falls back to the original calculation.
    Caller must use a private runtime directory and return JSON-only features.
    Optional advance(previous, rows, prefix_count) may reuse exact append-only
    history. It returns None to require a full computation. A correction,
    truncation, changed formula or invalid cache must never reuse prefix state.
    """
    import logging
    import os
    import sqlite3
    import stat
    import time
    from datetime import date
    from pathlib import Path
    from contextlib import closing
    log = logging.getLogger(__name__)
    started = time.monotonic()
    def encode(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if market not in {"US", "KR"} or not scope or not all(scope):
        return compute(rows)
    # The payload contracts for exact, append and sliding-window reuse are
    # different. Never decode a state envelope as a plain feature result.
    if advance is not None and slide is not None:
        return compute(rows)
    cache_kind = "append-v1" if advance is not None else "window-v1" if slide is not None else "exact-v1"
    try:
        if date.fromisoformat(trade_date).isoformat() != trade_date:
            return compute(rows)
        identity = encode([market, list(scope), trade_date, list(settings), cache_kind])
        digest = sha256(encode(rows).encode()).hexdigest()
        slot = sha256(identity.encode()).hexdigest()
        # One private account/day DB, not one unbounded file per bar version.
        account = sha256(encode([market, scope[0], trade_date]).encode()).hexdigest()
        root = Path(base)
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            return compute(rows)
        path = root / ("native-features-" + account + ".sqlite3")
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
                return compute(rows)
        finally:
            os.close(fd)
    except (OSError, ValueError, TypeError, AttributeError):
        return compute(rows)
    prior = None
    prior_window = None
    try:
        with closing(sqlite3.connect(path, timeout=.025)) as db:
            db.execute("PRAGMA busy_timeout=25")
            db.execute("""CREATE TABLE IF NOT EXISTS native_features (
                slot TEXT PRIMARY KEY, operand_hash TEXT NOT NULL,
                payload TEXT NOT NULL, checksum TEXT NOT NULL, updated REAL NOT NULL)""")
            row = db.execute("SELECT payload,checksum,operand_hash FROM native_features WHERE slot=?",
                             (slot,)).fetchone()
            if row and len(row[0]) <= 65536 and sha256(row[0].encode()).hexdigest() == row[1]:
                try:
                    decoded = json.loads(row[0])
                    if slide is not None:
                        if isinstance(decoded,dict) and decoded.get("window_schema")==1:
                            previous=decoded.get("rows")
                            if (isinstance(previous,list) and isinstance(rows,list)
                                    and 0<len(previous)<=15 and len(rows)==len(previous)
                                    and "value" in decoded
                                    and sha256(encode(previous).encode()).hexdigest()==row[2]):
                                if row[2]==digest:
                                    return decoded["value"]
                                # Exact suffix/prefix equality, including all
                                # provenance and numerical operands. Corrections
                                # inside the overlap never reuse old state.
                                for shift in range(1,len(rows)):
                                    if encode(previous[shift:])==encode(rows[:-shift]):
                                        prior_window=(decoded["value"],previous,shift)
                                        break
                    elif advance is None:
                        if row[2] == digest:
                            log.debug("[NATIVE_FEATURES][SHARED_HIT] market=%s lookup_ms=%.3f",
                                      market,(time.monotonic()-started)*1000)
                            return decoded
                    elif isinstance(decoded, dict) and decoded.get("append_schema") == 1:
                        count = decoded.get("rows_count")
                        if (type(count) is int and isinstance(rows, list)
                                and 0 < count <= len(rows) and "value" in decoded
                                and sha256(encode(rows[:count]).encode()).hexdigest() == row[2]):
                            if count == len(rows) and row[2] == digest:
                                log.debug("[NATIVE_FEATURES][SHARED_HIT] market=%s lookup_ms=%.3f",
                                          market,(time.monotonic()-started)*1000)
                                return decoded["value"]
                            if count < len(rows):
                                prior = (decoded["value"], count)
                except (ValueError, TypeError):
                    pass
    except (OSError, sqlite3.Error):
        return compute(rows)
    value = None
    if prior is not None:
        try:
            value = advance(prior[0], rows, prior[1])
        except (ValueError, TypeError, KeyError, IndexError, ArithmeticError):
            value = None
        if value is not None:
            log.debug("[NATIVE_FEATURES][APPEND_REUSE] market=%s prior_rows=%d appended_rows=%d lookup_ms=%.3f",
                      market, prior[1], len(rows)-prior[1], (time.monotonic()-started)*1000)
    if prior_window is not None:
        try:
            value=slide(prior_window[0],prior_window[1],rows,prior_window[2])
        except (ValueError,TypeError,KeyError,IndexError,ArithmeticError):
            value=None
        if value is not None:
            log.debug("[NATIVE_FEATURES][WINDOW_REUSE] market=%s shifted_rows=%d lookup_ms=%.3f",
                      market,prior_window[2],(time.monotonic()-started)*1000)
    if value is None:
        value = compute(rows)
    try:
        if slide is not None:
            if not isinstance(rows,list) or not 0<len(rows)<=15:
                return value
            payload=encode({"window_schema":1,"rows":rows,"value":value})
        else:
            payload = encode(value if advance is None else
                             {"append_schema":1,"rows_count":len(rows),"value":value})
        if len(payload) > 65536:
            return value
        with closing(sqlite3.connect(path, timeout=.025)) as db:
            db.execute("PRAGMA busy_timeout=25")
            db.execute("""INSERT INTO native_features VALUES (?,?,?,?,?)
                ON CONFLICT(slot) DO UPDATE SET operand_hash=excluded.operand_hash,
                payload=excluded.payload,checksum=excluded.checksum,updated=excluded.updated""",
                (slot,digest,payload,sha256(payload.encode()).hexdigest(),time.time()))
            db.execute("""DELETE FROM native_features WHERE slot IN (
                SELECT slot FROM native_features ORDER BY updated DESC,slot LIMIT -1 OFFSET ?)""",
                (_MAX_ENTRIES,))
            db.commit()
    except (OSError, ValueError, TypeError, sqlite3.Error):
        pass
    return value
