"""KRX 한국장 trade context / as_of resolver 테스트.

검증 항목:
1. 2026-05-26 오전 실행 context의 actual_as_of는 2026-05-22다
2. 2026-05-26 오후 실행 context의 actual_as_of도 2026-05-22다
3. AS_OF_OVERRIDE가 휴장일이면 prep 실패
4. resolve_trade_context() 결과의 as_of가 2026-05-25가 아닌지 확인
"""
from __future__ import annotations

from datetime import date, datetime
from unittest.mock import patch
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
# resolve_trade_context() — KR 시장 기준
# ---------------------------------------------------------------------------

class TestTradeContextKr2026:
    def test_trade_context_2026_05_26_am(self):
        """2026-05-26 09:00 KST 실행 시 actual_as_of = 2026-05-22."""
        now = datetime(2026, 5, 26, 9, 0, 0, tzinfo=KST)
        ctx = time_utils.resolve_trade_context(now=now, env="practice")
        actual_as_of = ctx["as_of"]
        assert actual_as_of == "2026-05-22", (
            f"Expected as_of=2026-05-22 but got {actual_as_of}. "
            "2026-05-25는 KRX 휴장일이므로 직전 거래일은 2026-05-22여야 함"
        )

    def test_trade_context_2026_05_26_afternoon(self):
        """2026-05-26 13:00 KST 실행 시 actual_as_of = 2026-05-22."""
        now = datetime(2026, 5, 26, 13, 0, 0, tzinfo=KST)
        ctx = time_utils.resolve_trade_context(now=now, env="practice")
        actual_as_of = ctx["as_of"]
        assert actual_as_of == "2026-05-22", (
            f"Expected as_of=2026-05-22 but got {actual_as_of}."
        )

    def test_trade_context_as_of_never_2026_05_25(self):
        """2026-05-26 실행 시 as_of가 절대 2026-05-25여서는 안 된다."""
        for hour in [7, 8, 9, 10, 13, 15]:
            now = datetime(2026, 5, 26, hour, 0, 0, tzinfo=KST)
            ctx = time_utils.resolve_trade_context(now=now, env="practice")
            assert ctx["as_of"] != "2026-05-25", (
                f"as_of={ctx['as_of']} at hour={hour}: "
                "2026-05-25는 KRX 휴장일이므로 as_of로 사용되면 안 됨"
            )

    def test_trade_context_2026_05_27_am(self):
        """2026-05-27 09:00 KST 실행 시 actual_as_of = 2026-05-26."""
        now = datetime(2026, 5, 27, 9, 0, 0, tzinfo=KST)
        ctx = time_utils.resolve_trade_context(now=now, env="practice")
        actual_as_of = ctx["as_of"]
        assert actual_as_of == "2026-05-26", (
            f"Expected as_of=2026-05-26 but got {actual_as_of}."
        )


# ---------------------------------------------------------------------------
# AS_OF_OVERRIDE + 휴장일 검증
# ---------------------------------------------------------------------------

class TestPrepAsofKr:
    def test_prep_rejects_holiday_as_of_override(self):
        """AS_OF_OVERRIDE가 KRX 휴장일이면 _pick_as_of_date_always_prev() 실패."""
        from trader.prep_runner import _pick_as_of_date_always_prev

        now = datetime(2026, 5, 26, 8, 0, 0, tzinfo=KST)
        with patch.dict("os.environ", {"AS_OF_OVERRIDE": "2026-05-25"}):
            with patch.object(time_utils, "now_kst", return_value=now):
                with pytest.raises((ValueError, SystemExit), match="2026-05-25|NOT a KRX trading day|AS_OF_OVERRIDE_INVALID"):
                    _pick_as_of_date_always_prev()

    def test_prep_accepts_valid_trading_day_override(self):
        """AS_OF_OVERRIDE가 유효한 거래일이면 정상 동작."""
        from trader.prep_runner import _pick_as_of_date_always_prev

        now = datetime(2026, 5, 26, 8, 0, 0, tzinfo=KST)
        with patch.dict("os.environ", {"AS_OF_OVERRIDE": "2026-05-22"}):
            with patch.object(time_utils, "now_kst", return_value=now):
                result = _pick_as_of_date_always_prev()
                assert result == date(2026, 5, 22)

    def test_prep_asof_canonical_2026_05_26(self):
        """AS_OF_OVERRIDE 없이 2026-05-26 실행 시 as_of = 2026-05-22."""
        from trader.prep_runner import _pick_as_of_date_always_prev

        now = datetime(2026, 5, 26, 8, 0, 0, tzinfo=KST)
        with patch.dict("os.environ", {}, clear=False):
            # AS_OF_OVERRIDE 없애기
            import os
            env_backup = os.environ.pop("AS_OF_OVERRIDE", None)
            try:
                with patch.object(time_utils, "now_kst", return_value=now):
                    result = _pick_as_of_date_always_prev()
                    assert result == date(2026, 5, 22), (
                        f"Expected 2026-05-22 but got {result}"
                    )
            finally:
                if env_backup is not None:
                    os.environ["AS_OF_OVERRIDE"] = env_backup


# ---------------------------------------------------------------------------
# is_krx_trading_day + resolve_prev_krx_trading_day 직접 테스트
# ---------------------------------------------------------------------------

class TestKrxTradingDayHelpers:
    def test_is_krx_trading_day_2026_05_25_false(self):
        assert time_utils.is_krx_trading_day("2026-05-25") is False

    def test_resolve_prev_krx_trading_day_2026_05_26(self):
        assert time_utils.resolve_prev_krx_trading_day("2026-05-26") == date(2026, 5, 22)
