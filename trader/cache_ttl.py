# -*- coding: utf-8 -*-
# trader/cache_ttl.py — TTL 캐시 (KIS 응답 공유)
import time
from typing import Dict, Any, Tuple

class TTLCache:
    def __init__(self):
        self.cache: Dict[Any, Tuple[Any, float]] = {}

    def get(self, key: Any) -> Any | None:
        if key in self.cache:
            value, expires_at = self.cache[key]
            if time.time() < expires_at:
                return value
            else:
                del self.cache[key]
        return None

    def set(self, key: Any, value: Any, ttl_sec: float):
        expires_at = time.time() + ttl_sec
        self.cache[key] = (value, expires_at)

# 싱글톤들
PRICE_SNAPSHOT_TTL_SEC = 2
DAILY_BAR_TTL_SEC = 1800  # 30분

price_cache = TTLCache()
daily_cache = TTLCache()