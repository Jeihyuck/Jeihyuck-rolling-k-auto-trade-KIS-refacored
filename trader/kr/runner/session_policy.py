# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, time
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

@dataclass(frozen=True)
class SessionDecision:
    action: str
    reason: str
    wait_seconds: int = 0
    exit_code: int = 0
    target: datetime | None = None


def parse_force_now(value: str | None = None) -> datetime | None:
    raw = (value if value is not None else os.getenv("FORCE_NOW", "")).strip()
    if not raw:
        return None
    return datetime.fromisoformat(raw).astimezone(KST)


def now_kst() -> datetime:
    return parse_force_now() or datetime.now(KST)


def kr_am_policy(now: datetime | None = None, *, max_wait_seconds: int | None = None) -> SessionDecision:
    """KR AM policy: do not proceed before 09:00:05 KST.

    Starts before 08:15 are treated as scheduler contamination and fail. Starts
    between 08:15 and 09:00:05 wait only if the caller can wait all the way to
    target; truncated waits fail rather than proceeding early.
    """
    now = (now or now_kst()).astimezone(KST)
    day = now.date()
    prewarm = datetime.combine(day, time(8, 15), tzinfo=KST)
    target = datetime.combine(day, time(9, 0, 5), tzinfo=KST)
    allow_until = datetime.combine(day, time(9, 25), tzinfo=KST)
    if now < prewarm:
        return SessionDecision("FAIL", "TOO_EARLY_FAIL", 0, 2, target)
    if now < target:
        required_wait = max(0, int((target - now).total_seconds()))
        if max_wait_seconds is not None and int(max_wait_seconds) < required_wait:
            return SessionDecision("FAIL", "WAIT_TRUNCATED_BEFORE_TARGET", int(max_wait_seconds), 2, target)
        return SessionDecision("WAIT", "WAIT_UNTIL_TARGET", required_wait, 0, target)
    if now <= allow_until:
        return SessionDecision("PROCEED", "ON_TIME", 0, 0, target)
    return SessionDecision("PROCEED", "LATE_START_RISK_MANAGEMENT_ALLOWED", 0, 0, target)


def kr_prep_schedule_guard(now: datetime | None = None, *, allow_outside: bool | None = None) -> SessionDecision:
    now = (now or now_kst()).astimezone(KST)
    allow_outside = os.getenv("ALLOW_KR_PREP_OUTSIDE_WINDOW", "0") == "1" if allow_outside is None else allow_outside
    start = time(6, 30)
    end = time(8, 50)
    allowed = start <= now.time() <= end
    if allowed or allow_outside:
        return SessionDecision("PROCEED", "PREP_WINDOW_OK" if allowed else "OUTSIDE_PREP_WINDOW_ALLOWED", 0, 0)
    return SessionDecision("BLOCK", "OUTSIDE_PREP_WINDOW", 0, 2)


EXIT_CODE_BY_REASON = {
    "OK": 0,
    "KR_PREP_DONE": 0,
    "RECOVERED_WITH_ARTIFACT": 0,
    "SAFE_STOP_MARKET_CLOSED": 0,
    "INTENTIONAL_NOOP": 0,
    "PREOPEN_NO_ORDER": 2,
    "TOO_EARLY_FAIL": 2,
    "WAIT_TRUNCATED_BEFORE_TARGET": 2,
    "KR_PREP_ARTIFACT_MISSING": 2,
    "REQUIRED_PREP_NOT_READY": 2,
    "STALE_PREP_ARTIFACT": 2,
    "FINAL30_ROW_COUNT": 2,
    "OUTSIDE_PREP_WINDOW": 2,
    "UNHANDLED_EXCEPTION": 1,
    "PREP_RUNNER_FAILED": 1,
}

def exit_code_for_result(result: dict) -> int:
    if "exit_code" in result and result.get("reason") not in EXIT_CODE_BY_REASON:
        return int(result.get("exit_code") or 0)
    status = str(result.get("status") or "").upper()
    reason = str(result.get("reason") or status)
    if status == "OK":
        return 0
    return EXIT_CODE_BY_REASON.get(reason, 1 if status in {"FAIL", "ERROR", "FAILED"} else 2)
