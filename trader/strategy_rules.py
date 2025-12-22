from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, time
from typing import Any, Dict, List, Optional


def _parse_hhmm(val: str, default: time) -> time:
    try:
        s = str(val).strip()
        if not s:
            return default
        hh, mm = s.split(":")
        return time(int(hh), int(mm))
    except Exception:
        return default


def strategy_trigger_label(strategy_id: int | None, strategy_name: Any = None) -> str:
    """전략 ID 기반 trigger label.
    - signals.evaluate_trigger_gate()가 이해하는 trigger_name을 반환한다.
    """
    sid = int(strategy_id or 1)
    if sid == 5:
        return "pullback_rebound"
    if sid == 4:
        return "close_betting"
    return "breakout_cross"


def strategy_entry_gate(
    strategy_id: int | None,
    info: Dict[str, Any],
    daily_ctx: Dict[str, Any],
    intraday_ctx: Dict[str, Any],
    *,
    now_dt_kst: datetime,
    regime_state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """전략 1~5 진입 게이트.

    반환:
      ok: bool
      reasons: list[str]
      trigger_label: str
      qty_scale: float
      entry_reason: str
    """
    sid = int(strategy_id or 1)
    reasons: List[str] = []

    # 전략5는 legacy_kosdaq_runner에서 별도 pullback 엔진으로 처리.
    if sid == 5:
        return {
            "ok": False,
            "reasons": ["use_pullback_engine"],
            "trigger_label": "pullback_rebound",
            "qty_scale": 0.0,
            "entry_reason": "S5_PULLBACK_ENGINE",
        }

    trigger_label = strategy_trigger_label(sid, info.get("strategy"))
    qty_scale = 1.0
    entry_reason = f"S{sid}"

    # 공통 참고 값
    champion_grade = str(info.get("champion_grade") or "").upper()
    strong_trend = bool(daily_ctx.get("strong_trend"))
    vwap_reclaim = bool(intraday_ctx.get("vwap_reclaim"))
    range_break = bool(intraday_ctx.get("range_break"))
    prev_high_retest = bool(intraday_ctx.get("prev_high_retest"))
    volume_spike = bool(intraday_ctx.get("volume_spike"))

    # === 전략별 규칙 ===
    if sid == 1:
        # 기본 돌파: setup_gate + trigger_gate 통과가 핵심 (추가 제약 없음)
        entry_reason = "S1_BREAKOUT"

    elif sid == 2:
        # 강한 돌파(범위/전고점 재돌파 중 하나는 필수)
        if not (range_break or prev_high_retest):
            reasons.append("need_range_or_prevhigh_retest")
        entry_reason = "S2_RANGE_BREAK"

    elif sid == 3:
        # VWAP 리클레임(상승 전환) 필수
        if not vwap_reclaim:
            reasons.append("need_vwap_reclaim")
        entry_reason = "S3_VWAP_RECLAIM"

    elif sid == 4:
        # 종가베팅: 시간 조건 + 최소 모멘텀 + (기본) 챔피언 등급
        start = _parse_hhmm(os.getenv("CLOSE_BETTING_START", "14:30"), time(14, 30))
        end = _parse_hhmm(os.getenv("CLOSE_BETTING_END", "15:10"), time(15, 10))
        if not (start <= now_dt_kst.time() <= end):
            reasons.append(f"time_window({start.strftime('%H:%M')}-{end.strftime('%H:%M')})")

        require_grade = os.getenv("CLOSE_BETTING_REQUIRE_GRADE", "AB").upper()
        if require_grade and champion_grade:
            if require_grade == "A" and champion_grade != "A":
                reasons.append("need_champion_A")
            elif require_grade == "AB" and champion_grade not in ("A", "B"):
                reasons.append("need_champion_A_or_B")
        elif require_grade and not champion_grade:
            # grade가 없으면 안전하게 차단(리밸런싱 응답/가공 누락 감지)
            reasons.append("missing_champion_grade")

        # 최소 모멘텀: strong_trend 또는 (vwap_reclaim/범위돌파/거래량스파이크)
        if not (strong_trend or vwap_reclaim or range_break or volume_spike):
            reasons.append("need_momentum_confirm")

        # 리스크 축소(기본 0.5)
        try:
            qty_scale = float(os.getenv("CLOSE_BETTING_QTY_SCALE", "0.5"))
        except Exception:
            qty_scale = 0.5
        qty_scale = max(0.1, min(qty_scale, 1.0))
        entry_reason = "S4_CLOSE_BETTING"

    else:
        # 알 수 없는 ID는 전략1로 안전 처리
        entry_reason = "S1_BREAKOUT"

    ok = len(reasons) == 0
    return {
        "ok": ok,
        "reasons": reasons,
        "trigger_label": trigger_label,
        "qty_scale": qty_scale,
        "entry_reason": entry_reason,
    }
