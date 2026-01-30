"""
DIAG 모드 판정 유틸리티

DIAG 모드에서만 우회 로직을 활성화하고,
LIVE 환경은 절대 건드리지 않기 위한 스위치 함수 제공
"""
from __future__ import annotations
import os


def is_diag_mode() -> bool:
    """
    DIAG 판단:
    - STRATEGY_MODE=DIAG 이거나
    - KIS_HTTP_ENABLED=0 이면 무조건 DIAG 취급
    
    LIVE는 절대 건드리지 않기 위해, 'DIAG일 때만' 우회 로직을 활성화한다.
    
    Returns:
        bool: DIAG 모드이면 True, 아니면 False
    """
    mode = (os.getenv("STRATEGY_MODE") or "").upper().strip()
    kis_http = (os.getenv("KIS_HTTP_ENABLED") or "").strip()
    return mode == "DIAG" or kis_http == "0"
