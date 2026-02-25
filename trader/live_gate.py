# -*- coding: utf-8 -*-
"""Live Gate Policy — 시간 기반 자동 Live Trading 허용/차단
거래 시간대 내에서만 주문을 허용하고, 그 외 시간대에는 자동으로 차단합니다.
사람이 매일 수동으로 스위치를 켜고 끌 필요가 없습니다.

핵심 원칙:
- 정책은 한 곳(compute_live_gate)에서만 결정
- 긴급 제어 (KILL_SWITCH, FORCE_BLOCK_LIVE, FORCE_LIVE) 최우선
- 거래일/거래시간/전략모드 기반 자동 정책
- 모든 결정은 로깅 가능하도록 reason 포함
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, time
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True)
class LiveGateStatus:
    """Live Gate 상태를 나타내는 불변 객체
    
    Attributes:
        allow_live_gate: True이면 주문 허용, False이면 차단
        force_block_live: True이면 강제 차단 (KIS API 호출 차단)
        reason: 결정 이유 (로깅/디버깅용)
        trading_day: 거래일 여부
        window: 시간대 구분 (preopen/morning/intraday/close/after)
        now_kst: 현재 KST 시각
    """
    allow_live_gate: bool
    force_block_live: bool
    reason: str
    trading_day: bool
    window: str
    now_kst: datetime


def is_trading_day_korea(now_kst: datetime) -> bool:
    """한국 거래일 판정 (간단 구현: 평일만 거래일)
    
    고도화 옵션:
    - 한국거래소 휴장일 DB/API 연동
    - trading_calendar 라이브러리 활용
    - 수동 관리되는 holiday.csv 참조
    
    Args:
        now_kst: KST 시각
        
    Returns:
        평일(월~금)이면 True, 주말이면 False
    """
    # 월요일=0, 금요일=4, 토요일=5, 일요일=6
    return now_kst.weekday() < 5


def classify_window(now_kst: datetime) -> str:
    """거래 시간대 구분
    
    한국 주식시장:
    - 08:30-09:00: 동시호가 (주문 접수만, 체결 안 됨)
    - 09:00-15:20: 정규장
    - 15:20-15:30: 동시호가 마감
    
    실전 운영 정책:
    - preopen (09:00 이전): 주문 차단 (동시호가 혼란 방지)
    - morning (09:00-10:00): 조심스럽게 주문 (변동성 큼)
    - intraday (10:00-15:15): 정규 주문
    - close (15:15-15:30): 마감 주문 (필요시)
    - after (15:30 이후): 주문 차단
    
    Args:
        now_kst: KST 시각
        
    Returns:
        시간대 문자열
    """
    t = now_kst.time()
    if t < time(9, 0):
        return "preopen"
    if time(9, 0) <= t < time(10, 0):
        return "morning"
    if time(10, 0) <= t < time(15, 15):
        return "intraday"
    if time(15, 15) <= t < time(15, 30):
        return "close"
    return "after"


def compute_live_gate(
    now_kst: datetime,
    *,
    kis_env: str,
    strategy_mode: str,
    dryrun: bool,
    analysis_only: bool,
) -> LiveGateStatus:
    """Live Gate 정책 - 단일 진실 공급원 (Single Source of Truth)
    
    우선순위 (높음 → 낮음):
    1. 긴급 제어 (KILL_SWITCH, FORCE_BLOCK_LIVE, FORCE_LIVE)
    2. 분석 전용 / Dry-run / 전략 모드
    3. 거래일 체크
    4. 시간대 체크
    
    Args:
        now_kst: 현재 KST 시각
        kis_env: "practice" 또는 "real"
        strategy_mode: "LIVE", "DIAG" 등
        dryrun: Dry-run 모드 여부
        analysis_only: 분석 전용 모드 (MINERVINI_ONLY 등)
        
    Returns:
        LiveGateStatus 객체
    """
    # ================================================================
    # [PRIORITY 1] 긴급 제어 (최우선)
    # ================================================================
    
    # KILL_SWITCH: 모든 주문 차단 (긴급 정지)
    if os.getenv("KILL_SWITCH", "0") == "1":
        return LiveGateStatus(
            allow_live_gate=False,
            force_block_live=True,
            reason="KILL_SWITCH",
            trading_day=is_trading_day_korea(now_kst),
            window=classify_window(now_kst),
            now_kst=now_kst,
        )
    
    # FORCE_BLOCK_LIVE: 강제 차단 (테스트/검증 시)
    if os.getenv("FORCE_BLOCK_LIVE", "0") == "1":
        return LiveGateStatus(
            allow_live_gate=False,
            force_block_live=True,
            reason="FORCE_BLOCK_LIVE",
            trading_day=is_trading_day_korea(now_kst),
            window=classify_window(now_kst),
            now_kst=now_kst,
        )
    
    # FORCE_LIVE: 강제 허용 (조심스럽게 사용)
    # real 환경에서는 추가 확인 필요
    if os.getenv("FORCE_LIVE", "0") == "1":
        if kis_env == "real" and os.getenv("FORCE_LIVE_CONFIRM", "") != "YES":
            return LiveGateStatus(
                allow_live_gate=False,
                force_block_live=True,
                reason="FORCE_LIVE_CONFIRM_REQUIRED",
                trading_day=is_trading_day_korea(now_kst),
                window=classify_window(now_kst),
                now_kst=now_kst,
            )
        return LiveGateStatus(
            allow_live_gate=True,
            force_block_live=False,
            reason="FORCE_LIVE",
            trading_day=is_trading_day_korea(now_kst),
            window=classify_window(now_kst),
            now_kst=now_kst,
        )
    
    # ================================================================
    # [PRIORITY 2] 분석 전용 / Dry-run / 전략 모드
    # ================================================================
    
    if analysis_only:
        return LiveGateStatus(
            allow_live_gate=False,
            force_block_live=True,
            reason="ANALYSIS_ONLY",
            trading_day=is_trading_day_korea(now_kst),
            window=classify_window(now_kst),
            now_kst=now_kst,
        )
    
    if dryrun:
        return LiveGateStatus(
            allow_live_gate=False,
            force_block_live=True,
            reason="DRYRUN",
            trading_day=is_trading_day_korea(now_kst),
            window=classify_window(now_kst),
            now_kst=now_kst,
        )
    
    if strategy_mode != "LIVE":
        return LiveGateStatus(
            allow_live_gate=False,
            force_block_live=True,
            reason=f"MODE={strategy_mode}",
            trading_day=is_trading_day_korea(now_kst),
            window=classify_window(now_kst),
            now_kst=now_kst,
        )
    
    # ================================================================
    # [PRIORITY 3] 거래일 체크
    # ================================================================
    
    trading_day = is_trading_day_korea(now_kst)
    window = classify_window(now_kst)
    
    if not trading_day:
        return LiveGateStatus(
            allow_live_gate=False,
            force_block_live=True,
            reason="NOT_TRADING_DAY",
            trading_day=trading_day,
            window=window,
            now_kst=now_kst,
        )
    
    # ================================================================
    # [PRIORITY 4] 시간대 체크
    # ================================================================
    
    # 주문 허용 시간대: morning, intraday, (선택적) close
    # - morning: 09:00-10:00 (변동성 크지만 일부 전략에는 필요)
    # - intraday: 10:00-15:15 (안정적인 주문 시간)
    # - close: 15:15-15:30 (마감 주문, 필요시 활성화)
    
    if window in ("morning", "intraday", "close"):
        # 미세 버퍼: 09:00 직후 N초간 주문 보류 (선택사항)
        # 동시호가 체결 직후 급등락 리스크 회피
        buf = int(os.getenv("OPEN_BUFFER_SEC", "0"))
        if window == "morning" and buf > 0:
            if (now_kst.hour, now_kst.minute) == (9, 0) and now_kst.second < buf:
                return LiveGateStatus(
                    allow_live_gate=False,
                    force_block_live=True,
                    reason=f"OPEN_BUFFER<{buf}s",
                    trading_day=trading_day,
                    window=window,
                    now_kst=now_kst,
                )
        
        # ✅ 정상 허용
        return LiveGateStatus(
            allow_live_gate=True,
            force_block_live=False,
            reason="TIME_WINDOW_OK",
            trading_day=trading_day,
            window=window,
            now_kst=now_kst,
        )
    
    # 허용되지 않은 시간대 (preopen, after)
    return LiveGateStatus(
        allow_live_gate=False,
        force_block_live=True,
        reason=f"WINDOW={window}",
        trading_day=trading_day,
        window=window,
        now_kst=now_kst,
    )
