"""Shared KR/US single-flight locking extracted from #401. No order APIs."""
import fcntl
import logging
import os
import time

LOG = logging.getLogger(__name__)

def read_or_fetch_scoped(*, paths, open_private, read, publish, fetch,
                         max_join_sec=0.15, defer_on_busy=False):
    started = time.monotonic()
    status = "UNAVAILABLE"
    fetch_ms = None
    try:
        lock_path, cache_path = paths()
        fd = open_private(lock_path, os.O_RDWR | os.O_CREAT)
    except (OSError, ValueError, TypeError):
        LOG.info("[MARKET_DATA_P0][SINGLEFLIGHT] status=UNAVAILABLE elapsed_ms=%.3f", (time.monotonic()-started)*1000)
        return "UNAVAILABLE", {}
    try:
        deadline = time.monotonic() + max(0.0, min(float(max_join_sec), 0.25))
        while True:
            cached = read(cache_path)
            if cached is not None:
                status = "HIT"
                return status, cached
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                remaining = deadline - time.monotonic()
                if defer_on_busy or remaining <= 0:
                    status = "BUSY"
                    return status, {}
                time.sleep(min(0.02, remaining))
                continue
            # Re-read after acquiring the lease. Another process may have
            # published between the first read and acquiring its lock.
            cached = read(cache_path)
            if cached is not None:
                status = "HIT"
                return status, cached
            status = "FETCH_ERROR"
            native_start = time.monotonic()
            try:
                row = fetch()
            finally:
                fetch_ms = (time.monotonic()-native_start)*1000
            publish(cache_path, row)
            status = "FETCH"
            return status, row
    finally:
        os.close(fd)
        LOG.info("[MARKET_DATA_P0][SINGLEFLIGHT] status=%s elapsed_ms=%.3f fetch_ms=%s",
                 status, (time.monotonic()-started)*1000,
                 "UNKNOWN" if fetch_ms is None else round(fetch_ms,3))
