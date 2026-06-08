# -*- coding: utf-8 -*-
"""trader/us/schedule/us_time.py

미국장 전용 시간 유틸리티.

서머타임(EDT/EST) 판단을 문자열 분기 없이 ZoneInfo("America/New_York") 기준으로
처리한다.

금지:
    if month in ...
    if tz == "EDT": ...
    if tz == "EST": ...

허용:
    now_et = datetime.now(ZoneInfo("UTC")).astimezone(ZoneInfo("America/New_York"))

로그 예시:
    [US_TIME][NOW] utc=2026-05-28T13:35:02Z ny=2026-05-28T09:35:02-04:00 tz=EDT
    [US_PHASE_GUARD] session=am should_run=1 window=0930-1230 timezone=America/New_York
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

NY_TZ = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")

# ── 각 세션별 phase guard 시간 윈도우 (ET) ──────────────────────────────────
_PHASE_WINDOWS: dict[str, tuple[str, str]] = {
    "prep":      ("0600", "0810"),   # scheduled 06:00  (EDT 10:00 UTC / EST 11:00 UTC)
    "am":        ("0930", "1230"),   # scheduled 09:35
    "afternoon": ("1230", "1550"),   # scheduled 12:35
    "close":     ("1545", "1630"),   # scheduled 15:55
}

_PHASE_SCHEDULED: dict[str, str] = {
    "prep":      "0600",
    "am":        "0935",
    "afternoon": "1235",
    "close":     "1555",
}


def now_ny() -> datetime:
    """현재 시각을 America/New_York 기준으로 반환한다."""
    return datetime.now(UTC_TZ).astimezone(NY_TZ)


def get_us_trade_date(now: datetime | None = None) -> date:
    """NY 기준 거래일 date를 반환한다."""
    dt = now if now is not None else now_ny()
    return dt.date()


def in_time_window(dt: datetime, start_hhmm: str, end_hhmm: str) -> bool:
    """dt.time()이 [start_hhmm, end_hhmm] 범위 내인지 확인한다.

    Args:
        dt: timezone-aware datetime (America/New_York 권장)
        start_hhmm: "HHMM" 형식 문자열 (예: "0930")
        end_hhmm:   "HHMM" 형식 문자열 (예: "1230")
    """
    start = time(int(start_hhmm[:2]), int(start_hhmm[2:]))
    end = time(int(end_hhmm[:2]), int(end_hhmm[2:]))
    return start <= dt.time() <= end


def check_phase_window(
    session: str,
    now: datetime | None = None,
) -> dict:
    """세션의 phase guard를 확인한다.

    Returns:
        {
            "should_run": bool,
            "window": "HHMM-HHMM",
            "now_et": "HH:MM:SS",
            "tz_abbr": "EDT" | "EST",
            "trade_date": "YYYY-MM-DD",
            "timezone": "America/New_York",
        }
    """
    dt = (now or now_ny()).astimezone(NY_TZ)
    trade_date = dt.strftime("%Y-%m-%d")
    now_et_str = dt.strftime("%H:%M:%S")
    tz_abbr = dt.strftime("%Z")  # "EDT" or "EST" — ZoneInfo가 자동 계산

    if session not in _PHASE_WINDOWS:
        logger.warning("[US_PHASE_GUARD] unknown session=%s — skipping window check", session)
        return {
            "should_run": True,
            "window": "0000-2359",
            "now_et": now_et_str,
            "tz_abbr": tz_abbr,
            "trade_date": trade_date,
            "timezone": "America/New_York",
        }

    start_hhmm, end_hhmm = _PHASE_WINDOWS[session]
    should_run = in_time_window(dt, start_hhmm, end_hhmm)

    logger.info(
        "[US_PHASE_GUARD] session=%s should_run=%d window=%s-%s now_et=%s tz=%s timezone=America/New_York",
        session, int(should_run), start_hhmm, end_hhmm, now_et_str, tz_abbr,
    )
    logger.info(
        "[US_TIME][NOW] utc=%s ny=%s tz=%s trade_date=%s",
        datetime.now(UTC_TZ).strftime("%Y-%m-%dT%H:%M:%SZ"),
        dt.isoformat(),
        tz_abbr,
        trade_date,
    )

    return {
        "should_run": should_run,
        "window": f"{start_hhmm}-{end_hhmm}",
        "now_et": now_et_str,
        "tz_abbr": tz_abbr,
        "trade_date": trade_date,
        "timezone": "America/New_York",
    }


def log_schedule_config(cron_count: int = 1) -> None:
    """workflow 시작 시 schedule 설정 정보를 로그에 남긴다."""
    logger.info(
        "[US_SCHEDULE][CONFIG] schedule_mode=timezone_single_cron crons=%d timezone=America/New_York",
        cron_count,
    )
