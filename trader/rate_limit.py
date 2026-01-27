# -*- coding: utf-8 -*-
# trader/rate_limit.py — KIS 호출 게이트 (레이트리밋 방어)
import time
import threading
from collections import defaultdict
from typing import Dict

MAX_CALLS_PER_SEC = 3  # 보수적 기본값
RATE_LIMIT_COOLDOWN_SEC = 8.0

class KisCallGate:
    def __init__(self):
        self.cooldown_until: Dict[str, float] = defaultdict(float)
        self.call_counts: Dict[str, int] = defaultdict(int)
        self.call_windows: Dict[str, float] = defaultdict(float)
        self.lock = threading.Lock()

    def allow(self, endpoint: str) -> bool:
        now = time.time()
        with self.lock:
            if now < self.cooldown_until[endpoint]:
                return False
            # 간단한 RPS 체크 (초당 MAX_CALLS_PER_SEC)
            if now - self.call_windows[endpoint] >= 1.0:
                self.call_counts[endpoint] = 0
                self.call_windows[endpoint] = now
            if self.call_counts[endpoint] >= MAX_CALLS_PER_SEC:
                return False
            self.call_counts[endpoint] += 1
            return True

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