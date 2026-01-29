"""
KIS API rate limiter (per-endpoint token bucket).

Why:
- KIS RT endpoints can return:
  - HTTP 500 with msg1 "초당 거래건수를 초과하였습니다."
  - intermittent RemoteDisconnected during bursts
This module provides a simple in-process limiter to smooth burst traffic.

Usage:
    from trader.kis_rate_limiter import get_kis_limiter
    get_kis_limiter().acquire("PRICE")
"""

from __future__ import annotations

import os
import random
import threading
import time
from typing import Dict


class TokenBucket:
    """
    Token Bucket:
    - rate_per_second: token refill rate (RPS)
    - capacity: maximum burst tokens

    acquire() returns wait seconds (0.0 if immediate).
    """

    def __init__(self, rate_per_second: float, capacity: int = 10):
        rate_per_second = float(rate_per_second)
        capacity = int(capacity)

        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be > 0")
        if capacity <= 0:
            raise ValueError("capacity must be > 0")

        self.rate = rate_per_second
        self.capacity = capacity
        self.tokens = float(capacity)
        self.last_update = time.time()
        self.lock = threading.Lock()

    def _refill(self) -> None:
        now = time.time()
        elapsed = now - self.last_update
        if elapsed <= 0:
            return

        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_update = now

    def acquire(self, tokens: int = 1) -> float:
        tokens = int(tokens)
        if tokens <= 0:
            return 0.0

        with self.lock:
            self._refill()

            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0

            deficit = tokens - self.tokens
            wait_time = deficit / self.rate

            # Spend remaining tokens now; move last_update forward to model the "debt".
            self.tokens = 0.0
            self.last_update = time.time() + wait_time
            return wait_time


class KisRateLimiter:
    """
    Endpoint keys:
      - "PRICE"
      - "DAILY_CHART"
      - "HOGA"

    Env overrides (float allowed):
      - KIS_RPS_PRICE (default 2.5)
      - KIS_RPS_DAILY_CHART (default 1)
      - KIS_RPS_HOGA (default 1.5)
      - KIS_RPS_JITTER_MAX (default 0.3 seconds)
    """

    def __init__(self) -> None:
        self.rps_price = float(os.getenv("KIS_RPS_PRICE", "2.5"))
        self.rps_daily_chart = float(os.getenv("KIS_RPS_DAILY_CHART", "1"))
        self.rps_hoga = float(os.getenv("KIS_RPS_HOGA", "1.5"))
        self.jitter_max = float(os.getenv("KIS_RPS_JITTER_MAX", "0.3"))

        def _cap(rps: float) -> int:
            # allow small bursts; ensure >= 1
            return max(1, int(round(rps * 2)))

        self.limiters: Dict[str, TokenBucket] = {
            "PRICE": TokenBucket(self.rps_price, _cap(self.rps_price)),
            "DAILY_CHART": TokenBucket(self.rps_daily_chart, _cap(self.rps_daily_chart)),
            "HOGA": TokenBucket(self.rps_hoga, _cap(self.rps_hoga)),
        }

    def acquire(self, key: str, tokens: int = 1) -> None:
        limiter = self.limiters.get(key)
        if limiter is None:
            return

        wait_time = limiter.acquire(tokens=tokens)
        if wait_time > 0:
            jitter = random.uniform(0.0, max(0.0, self.jitter_max))
            time.sleep(wait_time + jitter)


# Global singleton (simple in-process use)
_kis_limiter = KisRateLimiter()


def get_kis_limiter() -> KisRateLimiter:
    return _kis_limiter
