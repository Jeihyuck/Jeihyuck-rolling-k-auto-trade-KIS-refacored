# -*- coding: utf-8 -*-
"""
guards.py — 모드 전환(레짐/지수), 버킷 분류(A/B), 재난 스톱/쿨다운/당일청산 조건

역할
- 지수 하락폭에 따라 SAFE / INTRADAY-ONLY 모드 자동 전환
- 신규 진입 수량 배율(모드별) 및 장초 N분 노트레이드
- 버킷 분류: A(스윙 후보) / B(당일청산)
- 장초 재난 스톱(예: -9%) / 재진입 쿨다운 / 강제 당일청산 타이밍 판단

주의
- 'state'는 상위(trader.py)의 상태 딕셔너리를 그대로 사용(참조 전달)합니다.
"""
from __future__ import annotations
import os
import logging
from datetime import datetime
from typing import Dict, Any

log = logging.getLogger(__name__)

ENV = lambda k, d=None: os.getenv(k, d)

# 환경 파라미터 로드
NO_TRADE_MIN = int(ENV("NO_TRADE_MIN", "8"))
ORB_MIN = int(ENV("ORB_MIN", "10"))
VWAP_TOL = float(ENV("VWAP_TOL_PCT", "0.3")) / 100.0
MIN_TURNOVER_1M = float(ENV("MIN_TURNOVER_1M", "5e8"))
MAX_SPREAD_TICKS = int(ENV("MAX_SPREAD_TICKS", "3"))
REENTER_CD_MIN = int(ENV("REENTER_COOLDOWN_MIN", "30"))
STOP_FREEZE_MIN = int(ENV("STOP_FREEZE_MIN", "12"))
DISASTER_STOP_PCT = float(ENV("DISASTER_STOP_PCT", "9.0")) / 100.0

INDEX_SAFE_BLOCK_PCT = float(ENV("INDEX_SAFE_BLOCK_PCT", "-3.0"))
INDEX_INTRADAY_CUTOFF_PCT = float(ENV("INDEX_INTRADAY_CUTOFF_PCT", "-4.0"))
INDEX_CHECK_MIN = int(ENV("INDEX_CHECK_MIN", "30"))
FORCE_FLAT_TIME = ENV("FORCE_FLAT_TIME", "14:40")
CRASH_INTRADAY_ONLY = ENV("INTRADAY_ONLY_ON_CRASH", "1") == "1"
OPEN_SCALE_DOWN = float(ENV("OPEN_SCALE_DOWN", "0.5"))
MAX_DRAWDOWN_DAY_PCT = float(ENV("MAX_DRAWDOWN_DAY_PCT", "2.0"))


# ---- 시간/경과 계산 ---------------------------------------------------------

def minutes_since(open_ts, now: datetime | None = None) -> int:
    """개장 시각 open_ts로부터 경과 분."""
    now = now or datetime.now(open_ts.tzinfo)
    return int((now - open_ts).total_seconds() // 60)


def should_no_trade(open_ts, now: datetime | None = None) -> bool:
    """개장 NO_TRADE_MIN 분 동안 신규 매수 금지."""
    return minutes_since(open_ts, now) < NO_TRADE_MIN


# ---- 지수 기반 모드 전환 ----------------------------------------------------

def update_mode(state: Dict[str, Any], kosdaq_pct: float, open_ts, now: datetime | None = None) -> str:
    """
    kosdaq_pct(코스닥 당일 등락률 %)와 경과 시간으로 모드 전환.
    - INDEX_CHECK_MIN 경과 후 kosdaq_pct <= -3% → SAFE (수량 축소)
    - 90분 이내 kosdaq_pct <= -4% → INTRADAY-ONLY (신규 금지 + 당일청산)
    """
    now = now or datetime.now(open_ts.tzinfo)
    mins = minutes_since(open_ts, now)
    state.setdefault("mode", "normal")

    if mins >= INDEX_CHECK_MIN and kosdaq_pct <= INDEX_SAFE_BLOCK_PCT and state["mode"] == "normal":
        state["mode"] = "safe"
        log.warning("[MODE] SAFE 진입 KOSDAQ=%s%%", kosdaq_pct)

    if mins < 90 and kosdaq_pct <= INDEX_INTRADAY_CUTOFF_PCT:
        if state.get("mode") != "intraday":
            state["mode"] = "intraday"
            log.error("[MODE] INTRADAY-ONLY 진입 KOSDAQ=%s%%", kosdaq_pct)

    return state["mode"]


def allocation_multiplier(state: Dict[str, Any]) -> float:
    """모드별 신규 진입 수량 배율."""
    m = state.get("mode", "normal")
    if m == "safe":
        return OPEN_SCALE_DOWN  # 예: 0.5
    if m == "intraday":
        return 0.0              # 신규 금지
    return 1.0


def should_force_flat(state: Dict[str, Any], now_local: datetime) -> bool:
    """
    INTRADAY-ONLY 모드에서 14:40 이후 강제 당일청산 수행 여부.
    """
    if state.get("mode") != "intraday":
        return False
    return now_local.strftime("%H:%M") >= FORCE_FLAT_TIME


# ---- 손절/쿨다운 ------------------------------------------------------------

def disaster_stop(entry: float, last: float) -> bool:
    """
    장초 재난 스톱: 진입가 대비 DISASTER_STOP_PCT 이상 하락 시 즉시 컷.
    (예: 9% 하락 시 즉시 청산)
    """
    if entry <= 0:
        return False
    draw = (entry - last) / entry
    return draw >= DISASTER_STOP_PCT


def can_reenter(state: Dict[str, Any], code: str, now: datetime) -> bool:
    """
    재진입 쿨다운: 최근 청산 기록(last_exit[code]["time"])으로부터
    REENTER_CD_MIN 분 이후에만 재진입 허용.
    """
    info = state.get("last_exit", {}).get(code)
    if not info:
        return True
    elapse = (now - info["time"]).total_seconds() / 60.0
    return elapse >= REENTER_CD_MIN


# ---- 버킷 분류 --------------------------------------------------------------

def qualifies_A_bucket(metrics: Dict[str, Any]) -> bool:
    """
    A버킷(스윙 후보) 판정: 4개 축 중 3점 이상이면 A.
      1) VWAP 지지: last >= vwap*(1 - VWAP_TOL)
      2) 거래대금 플로우: turnover_1m >= MIN_TURNOVER_1M or turnover_rank_top30
      3) 모멘텀 강도(택1): ORB 상단 안착 / 당일 +3% & 저점대비 50% 회복 / 상대강도 상위
      4) 스프레드 양호: spread_ticks <= MAX_SPREAD_TICKS
    """
    last = float(metrics.get("last") or 0.0)
    vwap = float(metrics.get("vwap") or 0.0)
    spread_ticks = int(metrics.get("spread_ticks") or 9999)
    turnover_1m = float(metrics.get("turnover_1m") or 0.0)

    orb_ready = bool(metrics.get("orb_ready"))
    in_orb_box = bool(metrics.get("in_orb_box"))
    orb_high = metrics.get("orb_high")  # float|None

    day_return_pct = float(metrics.get("day_return_pct") or 0.0)
    retrace_from_low_pct = float(metrics.get("retrace_from_low_pct") or 0.0)
    rs_rank_top30 = bool(metrics.get("rs_rank_top30"))
    turnover_rank_top30 = bool(metrics.get("turnover_rank_top30"))

    cond_vwap = (vwap > 0 and last >= vwap * (1 - VWAP_TOL))
    cond_flow = (turnover_1m >= MIN_TURNOVER_1M) or turnover_rank_top30
    cond_momo = (
        (orb_ready and (orb_high is not None) and (last > float(orb_high)) and (not in_orb_box))
        or (day_return_pct >= 3.0 and retrace_from_low_pct >= 50.0)
        or rs_rank_top30
    )
    cond_spread = (spread_ticks <= MAX_SPREAD_TICKS)

    score = sum([cond_vwap, cond_flow, cond_momo, cond_spread])
    log.info("[BUCKET-CHECK] score=%d vwap=%s flow=%s momo=%s spread=%s",
             score, cond_vwap, cond_flow, cond_momo, cond_spread)
    return score >= 3


def assign_bucket(state: Dict[str, Any], code: str, metrics: Dict[str, Any], kosdaq_pct: float) -> str:
    """
    버킷 결정:
    - 폭락장(-4% 이하) + INTRADAY_ONLY_ON_CRASH=1 → 강제 B
    - 그 외에는 qualifies_A_bucket 점수로 A/B
    """
    if CRASH_INTRADAY_ONLY and kosdaq_pct <= INDEX_INTRADAY_CUTOFF_PCT:
        state.setdefault("bucket", {})[code] = "B"
        log.warning("[BUCKET] %s 폭락장으로 강제 B", code)
        return "B"

    isA = qualifies_A_bucket(metrics)
    state.setdefault("bucket", {})[code] = "A" if isA else "B"
    log.info("[BUCKET] %s -> %s", code, state["bucket"][code])
    return state["bucket"][code]
