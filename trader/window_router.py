from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Optional

KST = timezone(timedelta(hours=9))

MORNING_WINDOW_START = time.fromisoformat(os.getenv("MORNING_WINDOW_START", "08:50"))
MORNING_WINDOW_END = time.fromisoformat(os.getenv("MORNING_WINDOW_END", "15:20"))
AFTERNOON_WINDOW_START = time.fromisoformat(os.getenv("AFTERNOON_WINDOW_START", "15:20"))
AFTERNOON_WINDOW_END = time.fromisoformat(os.getenv("AFTERNOON_WINDOW_END", "15:30"))
MORNING_EXIT_START = time.fromisoformat(os.getenv("MORNING_EXIT_START", "09:00"))
MORNING_EXIT_END = time.fromisoformat(os.getenv("MORNING_EXIT_END", "09:20"))
CLOSE_AUCTION_START = time.fromisoformat(os.getenv("CLOSE_AUCTION_START", "15:20"))
CLOSE_AUCTION_END = time.fromisoformat(os.getenv("CLOSE_AUCTION_END", "15:30"))
PREOPEN_START = time.fromisoformat(os.getenv("PB1_PREOPEN_START", "08:45"))
PREOPEN_END = time.fromisoformat(os.getenv("PB1_PREOPEN_END", "09:00"))


@dataclass
class WindowDecision:
    name: str
    phase: str


def get_kst_now() -> datetime:
    return datetime.now(tz=KST)


def in_window(now: datetime, start: time, end: time) -> bool:
    return start <= now.time() < end


def decide_window(now: datetime | None = None, override: str = "auto") -> Optional[WindowDecision]:
    now = now or get_kst_now()
    if override == "morning":
        if in_window(now, MORNING_WINDOW_START, MORNING_WINDOW_END):
            phase = "exit" if in_window(now, MORNING_EXIT_START, MORNING_EXIT_END) else "trade"
            return WindowDecision(name="morning", phase=phase)
        return None
    if override == "afternoon":
        if in_window(now, AFTERNOON_WINDOW_START, AFTERNOON_WINDOW_END):
            phase = "trade"
            return WindowDecision(name="afternoon", phase=phase)
        return None

    if in_window(now, PREOPEN_START, PREOPEN_END):
        return WindowDecision(name="preopen", phase="entry")
    if in_window(now, MORNING_WINDOW_START, MORNING_WINDOW_END):
        phase = "exit" if in_window(now, MORNING_EXIT_START, MORNING_EXIT_END) else "trade"
        return WindowDecision(name="morning", phase=phase)
    if in_window(now, AFTERNOON_WINDOW_START, AFTERNOON_WINDOW_END):
        phase = "trade"
        return WindowDecision(name="afternoon", phase=phase)
    return None


def resolve_phase(window: WindowDecision, phase_override: str) -> str:
    if phase_override != "auto":
        return phase_override
    return window.phase
