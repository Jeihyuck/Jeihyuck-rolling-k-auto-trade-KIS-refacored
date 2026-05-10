# -*- coding: utf-8 -*-
"""US timeout guard utility.

Provides timeout wrapper for DB operations that may hang indefinitely.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from typing import Any, Callable, TypedDict


class TimeoutResult(TypedDict):
    """Timeout guard result structure."""
    ok: bool
    value: Any
    error: str | None
    timeout: bool
    elapsed_sec: float


def run_with_timeout(
    fn: Callable[[], Any],
    stage: str,
    timeout_sec: int,
) -> TimeoutResult:
    """
    Execute function with timeout guard.
    
    Args:
        fn: Function to execute (no arguments)
        stage: Stage name for logging (e.g., "prep_status", "watchlist_load")
        timeout_sec: Timeout in seconds
    
    Returns:
        TimeoutResult with ok/value/error/timeout/elapsed_sec
        
    Critical behavior:
    - Uses ThreadPoolExecutor for timeout enforcement
    - Does NOT use 'with' block to avoid hang during shutdown
    - Cancels future and shuts down executor immediately on timeout
    - Guarantees return within timeout_sec + 5 seconds
    """
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fn)
    start = time.monotonic()
    
    print(f"[US_TIMEOUT_GUARD][START] stage={stage} timeout_sec={timeout_sec}", flush=True)
    
    try:
        result = future.result(timeout=timeout_sec)
        elapsed = time.monotonic() - start
        print(f"[US_TIMEOUT_GUARD][DONE] stage={stage} elapsed_sec={elapsed:.2f}", flush=True)
        executor.shutdown(wait=False, cancel_futures=False)
        return {
            "ok": True,
            "value": result,
            "error": None,
            "timeout": False,
            "elapsed_sec": elapsed,
        }
    except FutureTimeoutError:
        elapsed = time.monotonic() - start
        print(
            f"[US_TIMEOUT_GUARD][TIMEOUT] stage={stage} timeout_sec={timeout_sec} elapsed_sec={elapsed:.2f}",
            flush=True,
        )
        future.cancel()
        print(f"[US_TIMEOUT_GUARD][SHUTDOWN] stage={stage} wait=false cancel_futures=true", flush=True)
        executor.shutdown(wait=False, cancel_futures=True)
        return {
            "ok": False,
            "value": None,
            "error": f"timeout after {timeout_sec}s",
            "timeout": True,
            "elapsed_sec": elapsed,
        }
    except Exception as exc:
        elapsed = time.monotonic() - start
        print(
            f"[US_TIMEOUT_GUARD][ERROR] stage={stage} elapsed_sec={elapsed:.2f} error={exc}",
            flush=True,
        )
        executor.shutdown(wait=False, cancel_futures=False)
        return {
            "ok": False,
            "value": None,
            "error": str(exc),
            "timeout": False,
            "elapsed_sec": elapsed,
        }
