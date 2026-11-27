"""Shared KR/US single-flight locking extracted from #401. No order APIs."""
import fcntl
import os
import time

def read_or_fetch_scoped(*, paths, open_private, read, publish, fetch,
                         max_join_sec=0.15, defer_on_busy=False):
    try:
        lock_path, cache_path = paths()
        fd = open_private(lock_path, os.O_RDWR | os.O_CREAT)
    except (OSError, ValueError, TypeError):
        return "UNAVAILABLE", {}
    try:
        deadline = time.monotonic() + max(0.0, min(float(max_join_sec), 0.25))
        while True:
            cached = read(cache_path)
            if cached is not None:
                return "HIT", cached
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                remaining = deadline - time.monotonic()
                if defer_on_busy or remaining <= 0:
                    return "BUSY", {}
                time.sleep(min(0.02, remaining))
                continue
            # Re-read after acquiring the lease. Another process may have
            # published between the first read and acquiring its lock.
            cached = read(cache_path)
            if cached is not None:
                return "HIT", cached
            row = fetch()
            publish(cache_path, row)
            return "FETCH", row
    finally:
        os.close(fd)
