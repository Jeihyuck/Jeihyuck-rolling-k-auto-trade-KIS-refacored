"""Short-lived, credential-scoped single-flight for canonical KR price reads.

This is a read-only optimization. No order, balance, token, orderbook or
native-minute confirmation request uses this cache. A failed/empty result
is never cached; a follower does not renew the original receipt timestamp.
Critical callers may choose existing fallback after a bounded join.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import stat
import tempfile
import time

from trader.kr.executable_quote import verified_price

log = logging.getLogger(__name__)
_MAX_BYTES = 65536


def enabled(environment):
    return environment == "practice" and os.getenv("KR_WS_SHARED_QUOTE_ENABLED") == "1"


def _paths(appkey, code):
    if not appkey or len(code) != 6 or not code.isascii() or not code.isdigit():
        raise ValueError("KR_REST_SHARED_SCOPE")
    root = Path("/tmp") / f"kr-rest-price-{os.getuid()}"
    root.mkdir(mode=0o700, exist_ok=True)
    info = root.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) & 0o077):
        raise ValueError("KR_REST_SHARED_DIRECTORY")
    key = hashlib.sha256(("practice\0" + appkey + "\0" + code).encode()).hexdigest()
    return root / (key + ".lock"), root / (key + ".json")


def _open_private(path, flags):
    fd = os.open(path, flags | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) & 0o077):
            raise ValueError("KR_REST_SHARED_FILE")
        return fd
    except BaseException:
        os.close(fd)
        raise


def _valid(row, code, now):
    return (isinstance(row, dict)
            and row.get("source") == "KIS_REST_INQUIRE_PRICE"
            and verified_price(row, code, now=now, max_age_sec=5.0))


def _read(path, code):
    try:
        with os.fdopen(_open_private(path, os.O_RDONLY), "r", encoding="utf-8") as file:
            raw = file.read(_MAX_BYTES + 1)
        if len(raw) > _MAX_BYTES:
            return None
        data = json.loads(raw)
        now = time.time()
        if not _valid(data, code, now):
            return None
        # Receipt identity is immutable; only its derived age changes.
        return {**data, "age_sec": now - float(data["received_at"])}
    except (OSError, ValueError, TypeError, OverflowError):
        return None


def _publish(path, row, code):
    if not _valid(row, code, time.time()):
        return
    tmp = None
    try:
        # Only the canonical native stock quote (no HTTP request/header
        # objects) is eligible. JSON is strict; invalid numerics aren't cached.
        data = json.dumps(row, ensure_ascii=True, allow_nan=False, separators=(",", ":"))
        if len(data) > _MAX_BYTES:
            return
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8",
                                         dir=path.parent, prefix=".quote-",
                                         delete=False) as file:
            tmp = Path(file.name)
            file.write(data)
        os.replace(tmp, path)
    except (OSError, ValueError, TypeError, OverflowError):
        log.warning("[KR_REST_SHARED][PUBLISH_FAIL] code=%s", code)
    finally:
        if tmp is not None:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                log.warning("[KR_REST_SHARED][CLEANUP_FAIL] code=%s", code)


def read_cached(*, appkey, code):
    """Nonblocking existing canonical price read; never starts native I/O."""
    try:
        _, path = _paths(appkey, code)
        return _read(path, code)
    except (OSError, ValueError, TypeError):
        return None


def read_or_fetch(*, appkey, code, fetch, max_join_sec=0.15, defer_on_busy=False):
    """Return (status, quote) with at most one leader per symbol.

    No additional broker read is performed by this helper on BUSY or
    UNAVAILABLE. The caller decides whether its original fallback is safe.
    The per-symbol lock is separate from the HTTP quota gate and is never
    used by write endpoints. OS releases it if the leader dies.
    """
    from trader.marketdata.rest_singleflight import read_or_fetch_scoped
    return read_or_fetch_scoped(
        paths=lambda: _paths(appkey, code), open_private=_open_private,
        read=lambda path: _read(path, code), publish=lambda path,row: _publish(path,row,code),
        fetch=fetch, max_join_sec=max_join_sec, defer_on_busy=defer_on_busy,
    )
