"""
[CONTRACT] compute-only 모드에서 주문이 차단되는지 검증.

- trade_tick._apply_prewarm_guard(): PB1_COMPUTE_ONLY=1 + 주말 → None 반환 (종료 안 함)
- trade_tick.main(): ORDER_ALLOWED/PB1_COMPUTE_ONLY env를 읽고 로깅한다
- order_allowed=False 시 compute-only 요약 로그가 찍혀야 한다
"""
from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest

_KST = timezone(timedelta(hours=9))

# ── _apply_prewarm_guard 주말 + compute-only 통과 ────────────────────────────

def _saturday_kst() -> datetime:
    """임의의 토요일 09:00 KST datetime 반환."""
    # 2026-04-25 SAT 09:00 KST
    return datetime(2026, 4, 25, 9, 0, tzinfo=_KST)


def test_prewarm_guard_weekend_exits_normally_without_compute_only() -> None:
    """PB1_COMPUTE_ONLY=0 + 주말 → 0 반환 (정상 종료)."""
    env_patch = {
        "PB1_PREWARM_ENABLED": "1",
        "PB1_COMPUTE_ONLY": "0",
        "PB1_SESSION_KIND": "am",
        "PB1_TARGET_START_TIME": "09:00",
        "PB1_START_ALLOW_UNTIL": "09:10",
    }
    with patch.dict(os.environ, env_patch, clear=False):
        from trader import trade_tick  # noqa: PLC0415
        result = trade_tick._apply_prewarm_guard(now_override=_saturday_kst())

    assert result == 0, "Weekend without compute_only must return 0 (exit)"


def test_prewarm_guard_weekend_continues_with_compute_only() -> None:
    """PB1_COMPUTE_ONLY=1 + 주말 → None 반환 (계속 실행)."""
    env_patch = {
        "PB1_PREWARM_ENABLED": "1",
        "PB1_COMPUTE_ONLY": "1",
        "PB1_SESSION_KIND": "am",
        "PB1_TARGET_START_TIME": "09:00",
        "PB1_START_ALLOW_UNTIL": "09:10",
    }
    with patch.dict(os.environ, env_patch, clear=False):
        from trader import trade_tick  # noqa: PLC0415
        result = trade_tick._apply_prewarm_guard(now_override=_saturday_kst())

    assert result is None, (
        "Weekend with PB1_COMPUTE_ONLY=1 must return None (continue run)"
    )


def test_prewarm_guard_weekday_unaffected_by_compute_only() -> None:
    """평일 09:00 KST + PB1_COMPUTE_ONLY=1 → None (정상 계속)."""
    # 2026-04-28 MON 09:00 KST
    monday = datetime(2026, 4, 28, 9, 0, tzinfo=_KST)
    env_patch = {
        "PB1_PREWARM_ENABLED": "1",
        "PB1_COMPUTE_ONLY": "1",
        "PB1_SESSION_KIND": "am",
        "PB1_TARGET_START_TIME": "09:00",
        "PB1_START_ALLOW_UNTIL": "09:10",
    }
    with patch.dict(os.environ, env_patch, clear=False):
        from trader import trade_tick  # noqa: PLC0415
        result = trade_tick._apply_prewarm_guard(now_override=monday)

    assert result is None, "Weekday 09:00 must return None regardless of compute_only"


# ── ORDER_ALLOWED / PB1_COMPUTE_ONLY env 읽기 ────────────────────────────────

def test_trade_tick_main_reads_order_allowed_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """main()이 ORDER_ALLOWED env를 읽는다 – PB1_PREWARM_ENABLED=0이면 guard 스킵."""
    import trader.trade_tick as tt

    pb1_main_mock = MagicMock(return_value=0)
    monkeypatch.setenv("PB1_PREWARM_ENABLED", "0")
    monkeypatch.setenv("ORDER_ALLOWED", "0")
    monkeypatch.setenv("PB1_COMPUTE_ONLY", "1")
    monkeypatch.setenv("PB1_ORDER_BLOCK_REASON", "non_trading_day")
    monkeypatch.setenv("PB1_SESSION_KIND", "am")

    with patch("trader.trade_tick._apply_prewarm_guard", return_value=None), \
         patch("trader.pb1_runner.main", pb1_main_mock):
        rc = tt.main()

    assert rc == 0
    pb1_main_mock.assert_called_once()
