"""
tests/test_trade_tick_prewarm_wait.py

trader.trade_tick._apply_prewarm_guard() 동작 검증.

검증 케이스:
- PB1_PREWARM_ENABLED=0 → None 반환 (skip)
- 토요일 → 0 반환 (OK_NO_TRADE/NON_TRADING_DAY)
- 일요일 → 0 반환
- now < target → sleep 호출, None 반환
- now == target → sleep 없음, None 반환
- now in (target, allow_until] → sleep 없음, None 반환
- now > allow_until → 0 반환 (SKIP_PHASE_WINDOW)
- PB1_TARGET_START_TIME 미설정 → None 반환 (guard 미적용)
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

_KST = timezone(timedelta(hours=9))

# 고정 날짜 (2026-04-27 = 월요일)
_MONDAY = 0
_SATURDAY = 5
_SUNDAY = 6

_BASE_DATES = {
    0: (2026, 4, 27),   # Mon
    1: (2026, 4, 28),   # Tue
    2: (2026, 4, 29),   # Wed
    3: (2026, 4, 30),   # Thu
    4: (2026, 5, 1),    # Fri
    5: (2026, 5, 2),    # Sat
    6: (2026, 5, 3),    # Sun
}


def _make_kst(hhmm: str, weekday: int = _MONDAY) -> datetime:
    h, m = int(hhmm[:2]), int(hhmm[2:])
    y, mo, d = _BASE_DATES[weekday]
    return datetime(y, mo, d, h, m, 0, tzinfo=_KST)


def _run(
    now_dt: datetime,
    *,
    target: str = "09:00",
    allow_until: str = "09:10",
    session: str = "am",
    prewarm_enabled: str = "1",
):
    """_apply_prewarm_guard를 env 패치 후 실행. (result, mock_sleep) 반환."""
    from trader.trade_tick import _apply_prewarm_guard
    import time as time_mod

    env = {
        "PB1_PREWARM_ENABLED": prewarm_enabled,
        "PB1_TARGET_START_TIME": target,
        "PB1_START_ALLOW_UNTIL": allow_until,
        "PB1_SESSION_KIND": session,
    }
    with patch.dict(os.environ, env), \
         patch.object(time_mod, "sleep") as mock_sleep:
        result = _apply_prewarm_guard(now_override=now_dt)
    return result, mock_sleep


# ── disabled ──────────────────────────────────────────────────────────────────

def test_prewarm_disabled_returns_none():
    """PB1_PREWARM_ENABLED=0이면 즉시 None 반환."""
    now_dt = _make_kst("0850")
    result, mock_sleep = _run(now_dt, prewarm_enabled="0")
    assert result is None
    mock_sleep.assert_not_called()


def test_prewarm_disabled_on_saturday_still_none():
    """prewarm 꺼져 있으면 토요일도 None 반환 (pb1_runner가 처리)."""
    now_dt = _make_kst("0900", weekday=_SATURDAY)
    result, _ = _run(now_dt, prewarm_enabled="0")
    assert result is None


# ── 비거래일 (주말) ────────────────────────────────────────────────────────────

def test_saturday_returns_0():
    """토요일 → 0 (OK_NO_TRADE / NON_TRADING_DAY)."""
    now_dt = _make_kst("0900", weekday=_SATURDAY)
    result, mock_sleep = _run(now_dt)
    assert result == 0
    mock_sleep.assert_not_called()


def test_sunday_returns_0():
    """일요일 → 0 (OK_NO_TRADE / NON_TRADING_DAY)."""
    now_dt = _make_kst("1300", weekday=_SUNDAY)
    result, mock_sleep = _run(now_dt, target="13:00", allow_until="13:10", session="pm")
    assert result == 0
    mock_sleep.assert_not_called()


# ── before target: sleep ──────────────────────────────────────────────────────

def test_before_target_calls_sleep():
    """target 전 → sleep 호출, None 반환."""
    now_dt = _make_kst("0850")   # 08:50, target=09:00
    result, mock_sleep = _run(now_dt)
    assert result is None
    mock_sleep.assert_called_once()
    wait_sec = mock_sleep.call_args[0][0]
    # 08:50 → 09:00 = 10분 = ~600초 (초 단위 오차 허용)
    assert 580 <= wait_sec <= 620


def test_am_0845_waits_approx_900s():
    """08:45 → 09:00 = 15분 ≈ 900초."""
    now_dt = _make_kst("0845")
    result, mock_sleep = _run(now_dt)
    assert result is None
    mock_sleep.assert_called_once()
    wait_sec = mock_sleep.call_args[0][0]
    assert 880 <= wait_sec <= 920


def test_pm_before_target_calls_sleep():
    """PM 12:45 → 13:00 ≈ 900초."""
    now_dt = _make_kst("1245")
    result, mock_sleep = _run(now_dt, target="13:00", allow_until="13:10", session="pm")
    assert result is None
    mock_sleep.assert_called_once()
    wait_sec = mock_sleep.call_args[0][0]
    assert 880 <= wait_sec <= 920


def test_close_before_target_calls_sleep():
    """CLOSE 15:00 → 15:15 ≈ 900초."""
    now_dt = _make_kst("1500")
    result, mock_sleep = _run(now_dt, target="15:15", allow_until="15:20", session="close")
    assert result is None
    mock_sleep.assert_called_once()
    wait_sec = mock_sleep.call_args[0][0]
    assert 880 <= wait_sec <= 920


# ── at target: no sleep ───────────────────────────────────────────────────────

def test_at_target_no_sleep():
    """정각 target 도달 → sleep 없음, None 반환."""
    now_dt = _make_kst("0900")
    result, mock_sleep = _run(now_dt)
    assert result is None
    mock_sleep.assert_not_called()


# ── in window (target, allow_until]: no sleep ────────────────────────────────

def test_within_window_no_sleep():
    """09:00 < now <= 09:10 → 즉시 실행, sleep 없음."""
    for hhmm in ["0901", "0905", "0910"]:
        now_dt = _make_kst(hhmm)
        result, mock_sleep = _run(now_dt)
        assert result is None, f"hhmm={hhmm}: expected None"
        mock_sleep.assert_not_called()


# ── after allow_until: stale skip ────────────────────────────────────────────

def test_after_allow_until_returns_0():
    """09:11 이후 → SKIP_PHASE_WINDOW (0 반환)."""
    now_dt = _make_kst("0911")
    result, mock_sleep = _run(now_dt)
    assert result == 0
    mock_sleep.assert_not_called()


def test_am_0956_stale_skip():
    """09:56 (실제 버그 케이스) → stale skip."""
    now_dt = _make_kst("0956")
    result, mock_sleep = _run(now_dt)
    assert result == 0
    mock_sleep.assert_not_called()


def test_pm_after_allow_until_stale():
    """PM 13:11 → stale skip."""
    now_dt = _make_kst("1311")
    result, mock_sleep = _run(now_dt, target="13:00", allow_until="13:10", session="pm")
    assert result == 0
    mock_sleep.assert_not_called()


def test_close_after_allow_until_stale():
    """CLOSE 15:21 → stale skip."""
    now_dt = _make_kst("1521")
    result, mock_sleep = _run(now_dt, target="15:15", allow_until="15:20", session="close")
    assert result == 0
    mock_sleep.assert_not_called()


# ── env 미설정 시 guard 비활성 ────────────────────────────────────────────────

def test_missing_target_start_time_returns_none():
    """PB1_TARGET_START_TIME 미설정 → None (guard 미적용)."""
    from trader.trade_tick import _apply_prewarm_guard
    import time as time_mod

    env = {
        "PB1_PREWARM_ENABLED": "1",
        "PB1_SESSION_KIND": "am",
    }
    # PB1_TARGET_START_TIME / PB1_START_ALLOW_UNTIL 제거
    clean_env = {k: v for k, v in os.environ.items()
                 if k not in ("PB1_TARGET_START_TIME", "PB1_START_ALLOW_UNTIL")}
    clean_env.update(env)

    now_dt = _make_kst("0900")
    with patch.dict(os.environ, clean_env, clear=True), \
         patch.object(time_mod, "sleep") as mock_sleep:
        result = _apply_prewarm_guard(now_override=now_dt)

    assert result is None
    mock_sleep.assert_not_called()
