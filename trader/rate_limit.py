# -*- coding: utf-8 -*-
# trader/rate_limit.py — KIS 호출 게이트 (레이트리밋 방어)
import time
import threading
from collections import defaultdict
from typing import Dict
import os

MAX_CALLS_PER_SEC = int(os.getenv("KIS_MAX_CALLS_PER_SEC", "3") or "3")  # 보수적 기본값
RATE_LIMIT_COOLDOWN_SEC = float(os.getenv("KIS_RATE_LIMIT_COOLDOWN_SEC", "8.0") or "8.0")
MIN_INTERVAL_SEC = 1.0 / MAX_CALLS_PER_SEC  # 초당 3건이면 0.33초 간격


class _TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int):
        self.rate = max(0.1, float(rate_per_sec))
        self.burst = max(1, int(burst))
        self.tokens = float(self.burst)
        self.last = time.time()

    def consume(self, n: int = 1) -> float:
        now = time.time()
        elapsed = max(0.0, now - self.last)
        self.tokens = min(float(self.burst), self.tokens + elapsed * self.rate)
        self.last = now
        need = float(max(1, n))
        if self.tokens >= need:
            self.tokens -= need
            return 0.0
        deficit = need - self.tokens
        self.tokens = 0.0
        return deficit / self.rate

class KisCallGate:
    def __init__(self):
        self.cooldown_until: Dict[str, float] = defaultdict(float)
        self.call_counts: Dict[str, int] = defaultdict(int)
        self.call_windows: Dict[str, float] = defaultdict(float)
        self.last_call_time: Dict[str, float] = defaultdict(float)  # 마지막 호출 시각
        self.global_cooldown_until: Dict[str, float] = defaultdict(float)
        self.global_buckets: Dict[str, _TokenBucket] = {}
        self.endpoint_buckets: Dict[str, _TokenBucket] = {}
        self.lock = threading.Lock()

    def _env_rate(self, environment: str) -> float:
        env = str(environment or "practice").strip().lower()
        if env == "real":
            return float(os.getenv("KIS_GLOBAL_RPS_REAL", "2.0") or "2.0")
        return float(os.getenv("KIS_GLOBAL_RPS_PRACTICE", "1.5") or "1.5")

    def _env_burst(self, environment: str) -> int:
        env = str(environment or "practice").strip().lower()
        if env == "real":
            return int(float(os.getenv("KIS_GLOBAL_BURST_REAL", "1") or "1"))
        return int(float(os.getenv("KIS_GLOBAL_BURST_PRACTICE", "1") or "1"))

    def _endpoint_rate(self, category: str) -> float:
        c = str(category or "data").strip().lower()
        if c in {"order", "orders", "trade"}:
            return float(os.getenv("KIS_ENDPOINT_RPS_ORDER", "1.5") or "1.5")
        if c in {"balance", "cash", "reconcile"}:
            return float(os.getenv("KIS_ENDPOINT_RPS_BALANCE", "1.2") or "1.2")
        if c in {"price", "quote"}:
            return float(os.getenv("KIS_ENDPOINT_RPS_PRICE", "1.0") or "1.0")
        return float(os.getenv("KIS_ENDPOINT_RPS_DEFAULT", "1.0") or "1.0")

    def _endpoint_burst(self, category: str) -> int:
        c = str(category or "data").strip().lower()
        if c in {"order", "orders", "trade"}:
            return int(float(os.getenv("KIS_ENDPOINT_BURST_ORDER", "1") or "1"))
        return int(float(os.getenv("KIS_ENDPOINT_BURST_DEFAULT", "1") or "1"))

    def _bucket_key(self, environment: str, account_key: str) -> str:
        return f"{str(environment or 'practice').strip().lower()}:{str(account_key or 'default').strip()}"

    def set_global_cooldown(self, environment: str, account_key: str, seconds: float = RATE_LIMIT_COOLDOWN_SEC) -> None:
        key = self._bucket_key(environment, account_key)
        with self.lock:
            self.global_cooldown_until[key] = max(self.global_cooldown_until[key], time.time() + max(0.1, float(seconds)))

    def acquire(
        self,
        *,
        environment: str,
        account_key: str,
        endpoint_category: str,
        priority: bool = False,
    ) -> float:
        bucket_key = self._bucket_key(environment, account_key)
        endpoint_key = f"{bucket_key}:{str(endpoint_category or 'data').strip().lower()}"
        with self.lock:
            now = time.time()
            cd_until = float(self.global_cooldown_until.get(bucket_key, 0.0) or 0.0)
            if now < cd_until:
                return cd_until - now

            global_bucket = self.global_buckets.get(bucket_key)
            if global_bucket is None:
                global_bucket = _TokenBucket(self._env_rate(environment), self._env_burst(environment))
                self.global_buckets[bucket_key] = global_bucket

            endpoint_bucket = self.endpoint_buckets.get(endpoint_key)
            if endpoint_bucket is None:
                endpoint_bucket = _TokenBucket(self._endpoint_rate(endpoint_category), self._endpoint_burst(endpoint_category))
                self.endpoint_buckets[endpoint_key] = endpoint_bucket

            wait_global = global_bucket.consume(1)
            wait_endpoint = 0.0 if bool(priority) else endpoint_bucket.consume(1)
            return max(wait_global, wait_endpoint)

    def allow(self, endpoint: str) -> bool:
        now = time.time()
        with self.lock:
            # 1. Cooldown 확인
            if now < self.cooldown_until[endpoint]:
                return False
            
            # 2. 최소 간격 보장 (스로틀링)
            last_call = self.last_call_time[endpoint]
            if last_call > 0:
                elapsed = now - last_call
                if elapsed < MIN_INTERVAL_SEC:
                    # 최소 간격 미달 -> 차단
                    return False
            
            # 3. 초당 최대 호출 수 체크
            if now - self.call_windows[endpoint] >= 1.0:
                self.call_counts[endpoint] = 0
                self.call_windows[endpoint] = now
            if self.call_counts[endpoint] >= MAX_CALLS_PER_SEC:
                return False
            
            # 허용
            self.call_counts[endpoint] += 1
            self.last_call_time[endpoint] = now
            return True
    
    def wait_if_needed(self, endpoint: str) -> float:
        """최소 간격을 보장하기 위해 필요한 sleep 시간 반환"""
        now = time.time()
        with self.lock:
            last_call = self.last_call_time[endpoint]
            if last_call > 0:
                elapsed = now - last_call
                if elapsed < MIN_INTERVAL_SEC:
                    sleep_time = MIN_INTERVAL_SEC - elapsed
                    return sleep_time
        return 0.0

    def penalize(self, endpoint: str, seconds: float = RATE_LIMIT_COOLDOWN_SEC):
        now = time.time()
        with self.lock:
            self.cooldown_until[endpoint] = max(self.cooldown_until[endpoint], now + seconds)

    def get_cooldown_until(self, endpoint: str) -> float:
        with self.lock:
            return self.cooldown_until[endpoint]

# 싱글톤
_gate_instance = KisCallGate()

def get_kis_gate() -> KisCallGate:
    return _gate_instance