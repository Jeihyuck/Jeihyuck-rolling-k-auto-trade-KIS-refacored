"""한국장 workflow guard 관련 테스트.

검증 항목:
1. final30 rows가 30이 아니면 trade-am engine 실행 금지 (prep_ready=0)
2. final30 rows가 30이 아니면 trade-afternoon engine 실행 금지 (prep_ready=0)
3. AM에서 PNL만 실행되고 engine이 skipped이면 최종 assert 실패
4. Afternoon에서 PNL만 실행되고 engine이 skipped이면 최종 assert 실패
"""
from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

import trader.time_utils as time_utils


KST = ZoneInfo("Asia/Seoul")


@pytest.fixture(autouse=True)
def reset_krx_cache():
    time_utils._KRX_HOLIDAYS_CACHE = None
    yield
    time_utils._KRX_HOLIDAYS_CACHE = None


# ---------------------------------------------------------------------------
# prep_ready 판정 로직 (verify_final30 단계 Python 로직 재현)
# ---------------------------------------------------------------------------

def _simulate_verify_final30(*, today: date, rows: int, as_of_override: str = "") -> dict:
    """
    verify_final30 단계의 Python 로직을 시뮬레이션한다.
    Returns dict with: prep_ready, expected_as_of, final30_rows, skip_reason
    """
    if as_of_override:
        expected_as_of = date.fromisoformat(as_of_override)
    else:
        expected_as_of = time_utils.resolve_prev_krx_trading_day(today)

    if not time_utils.is_krx_trading_day(today):
        return {
            "prep_ready": "0",
            "expected_as_of": expected_as_of.isoformat(),
            "final30_rows": "0",
            "skip_reason": "NON_TRADING_DAY",
        }

    if rows == 30:
        return {
            "prep_ready": "1",
            "expected_as_of": expected_as_of.isoformat(),
            "final30_rows": str(rows),
            "skip_reason": "OK",
        }
    else:
        return {
            "prep_ready": "0",
            "expected_as_of": expected_as_of.isoformat(),
            "final30_rows": str(rows),
            "skip_reason": f"FINAL30_NOT_FOUND:rows={rows}",
        }


def _simulate_assert_am_engine(
    *,
    is_trading_day: bool,
    prep_ready: str,
    engine_outcome: str,
) -> tuple[bool, str]:
    """
    Assert AM engine execution 단계의 로직을 시뮬레이션한다.
    Returns: (ok: bool, reason: str)
    """
    if not is_trading_day:
        return True, "non_trading_day_ok"
    if prep_ready != "1":
        return False, f"PREP_NOT_READY:prep_ready={prep_ready}"
    if engine_outcome != "success":
        return False, f"ENGINE_NOT_RUN:engine_outcome={engine_outcome}"
    return True, "ASSERT_OK"


def _simulate_assert_afternoon_engine(
    *,
    is_trading_day: bool,
    prep_ready: str,
    pm_outcome: str,
    close_outcome: str,
) -> tuple[bool, str]:
    """
    Assert Afternoon engine execution 단계의 로직을 시뮬레이션한다.
    """
    if not is_trading_day:
        return True, "non_trading_day_ok"
    if prep_ready != "1":
        return False, f"PREP_NOT_READY:prep_ready={prep_ready}"
    if pm_outcome != "success" and close_outcome != "success":
        return False, f"ENGINE_NOT_RUN:pm={pm_outcome},close={close_outcome}"
    return True, "ASSERT_OK"


# ---------------------------------------------------------------------------
# AM engine guard
# ---------------------------------------------------------------------------

class TestTradeAmEngineGuard:
    def test_am_engine_blocked_when_final30_missing(self):
        """final30 rows != 30 → prep_ready=0 → assert 실패."""
        result = _simulate_verify_final30(today=date(2026, 5, 26), rows=0)
        assert result["prep_ready"] == "0"
        assert "FINAL30_NOT_FOUND" in result["skip_reason"]

        ok, reason = _simulate_assert_am_engine(
            is_trading_day=True,
            prep_ready=result["prep_ready"],
            engine_outcome="skipped",
        )
        assert ok is False
        assert "PREP_NOT_READY" in reason

    def test_am_engine_blocked_when_partial_rows(self):
        """final30 rows=15 (부분 데이터) → prep_ready=0."""
        result = _simulate_verify_final30(today=date(2026, 5, 26), rows=15)
        assert result["prep_ready"] == "0"

    def test_am_engine_allowed_when_30_rows(self):
        """final30 rows=30 → prep_ready=1 → engine 실행 허용."""
        result = _simulate_verify_final30(today=date(2026, 5, 26), rows=30)
        assert result["prep_ready"] == "1"
        assert result["expected_as_of"] == "2026-05-22"

        ok, reason = _simulate_assert_am_engine(
            is_trading_day=True,
            prep_ready="1",
            engine_outcome="success",
        )
        assert ok is True
        assert reason == "ASSERT_OK"

    def test_am_assert_fails_when_engine_skipped_despite_prep_ready(self):
        """prep_ready=1인데 engine이 skipped → assert 실패."""
        ok, reason = _simulate_assert_am_engine(
            is_trading_day=True,
            prep_ready="1",
            engine_outcome="skipped",
        )
        assert ok is False
        assert "ENGINE_NOT_RUN" in reason

    def test_am_assert_passes_on_non_trading_day(self):
        """비거래일 (토요일 등)은 engine 없이도 assert 통과."""
        result = _simulate_verify_final30(today=date(2026, 5, 25), rows=0)
        assert result["skip_reason"] == "NON_TRADING_DAY"

        ok, _ = _simulate_assert_am_engine(
            is_trading_day=False,
            prep_ready="0",
            engine_outcome="skipped",
        )
        assert ok is True

    def test_am_expected_as_of_2026_05_22(self):
        """2026-05-26 기준 expected_as_of는 2026-05-22다."""
        result = _simulate_verify_final30(today=date(2026, 5, 26), rows=30)
        assert result["expected_as_of"] == "2026-05-22"


# ---------------------------------------------------------------------------
# Afternoon engine guard
# ---------------------------------------------------------------------------

class TestTradeAfternoonEngineGuard:
    def test_afternoon_engine_blocked_when_final30_missing(self):
        """final30 rows != 30 → afternoon engine 실행 금지."""
        result = _simulate_verify_final30(today=date(2026, 5, 26), rows=0)
        assert result["prep_ready"] == "0"

        ok, reason = _simulate_assert_afternoon_engine(
            is_trading_day=True,
            prep_ready=result["prep_ready"],
            pm_outcome="skipped",
            close_outcome="skipped",
        )
        assert ok is False
        assert "PREP_NOT_READY" in reason

    def test_afternoon_engine_allowed_with_30_rows(self):
        """final30 rows=30 → engine 실행 허용."""
        result = _simulate_verify_final30(today=date(2026, 5, 26), rows=30)
        assert result["prep_ready"] == "1"

        ok, reason = _simulate_assert_afternoon_engine(
            is_trading_day=True,
            prep_ready="1",
            pm_outcome="success",
            close_outcome="success",
        )
        assert ok is True

    def test_afternoon_assert_fails_when_both_loops_skipped(self):
        """prep_ready=1인데 pm_loop, close_loop 모두 skipped → assert 실패."""
        ok, reason = _simulate_assert_afternoon_engine(
            is_trading_day=True,
            prep_ready="1",
            pm_outcome="skipped",
            close_outcome="skipped",
        )
        assert ok is False
        assert "ENGINE_NOT_RUN" in reason

    def test_afternoon_assert_passes_when_close_only_success(self):
        """pm_loop 실패 + close_loop 성공 → assert 통과 (close가 실행됐으므로)."""
        ok, reason = _simulate_assert_afternoon_engine(
            is_trading_day=True,
            prep_ready="1",
            pm_outcome="failure",
            close_outcome="success",
        )
        assert ok is True

    def test_afternoon_assert_passes_on_non_trading_day(self):
        """비거래일이면 assert 통과."""
        ok, _ = _simulate_assert_afternoon_engine(
            is_trading_day=False,
            prep_ready="0",
            pm_outcome="skipped",
            close_outcome="skipped",
        )
        assert ok is True

    def test_afternoon_expected_as_of_after_holiday(self):
        """2026-05-26 실행 시 오후장도 expected_as_of = 2026-05-22."""
        result = _simulate_verify_final30(today=date(2026, 5, 26), rows=30)
        assert result["expected_as_of"] == "2026-05-22"


# ---------------------------------------------------------------------------
# 2026-05-25 휴장일 당일 실행
# ---------------------------------------------------------------------------

class TestHolidayDayGuard:
    def test_verify_final30_on_holiday_returns_non_trading(self):
        """2026-05-25 (휴장일)에 실행하면 skip_reason=NON_TRADING_DAY."""
        result = _simulate_verify_final30(today=date(2026, 5, 25), rows=30)
        assert result["prep_ready"] == "0"
        assert result["skip_reason"] == "NON_TRADING_DAY"

    def test_am_assert_ok_on_holiday(self):
        """휴장일에 engine이 skipped이어도 assert 통과 (is_trading_day=False)."""
        ok, _ = _simulate_assert_am_engine(
            is_trading_day=False,
            prep_ready="0",
            engine_outcome="skipped",
        )
        assert ok is True

    def test_afternoon_assert_ok_on_holiday(self):
        """휴장일에 afternoon engine이 skipped이어도 assert 통과."""
        ok, _ = _simulate_assert_afternoon_engine(
            is_trading_day=False,
            prep_ready="0",
            pm_outcome="skipped",
            close_outcome="skipped",
        )
        assert ok is True
