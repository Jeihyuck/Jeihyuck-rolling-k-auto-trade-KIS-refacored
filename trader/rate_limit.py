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

class KisCallGate:
    def __init__(self):
        self.cooldown_until: Dict[str, float] = defaultdict(float)
        self.call_counts: Dict[str, int] = defaultdict(int)
        self.call_windows: Dict[str, float] = defaultdict(float)
        self.last_call_time: Dict[str, float] = defaultdict(float)  # 마지막 호출 시각
        self.lock = threading.Lock()

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