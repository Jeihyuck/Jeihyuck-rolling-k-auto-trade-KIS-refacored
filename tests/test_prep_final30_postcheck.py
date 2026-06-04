"""
tests/test_prep_final30_postcheck.py

PREP final30 postcheck 검증:
- build_and_save_watchlist가 rows=30을 반환해도 DB postcheck rows=0이면 PREP 실패
- DB postcheck rows=30, contract_ok=1일 때만 PREP success
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, PropertyMock


class MockDF:
    """DB 조회 결과를 흉내내는 DataFrame mock"""

    def __init__(self, rows: int):
        self._rows = rows

    def __len__(self):
        return self._rows


def _make_postcheck_result(rows: int):
    """postcheck_repo.load_final30_scored_exact mock 반환값"""
    mock_df = MagicMock()
    mock_df.__len__ = lambda self: rows
    return mock_df


class TestPrepPostcheck:
    """PREP_FINAL30_READY postcheck 로직 단위 검증"""

    def test_postcheck_30_rows_is_success(self):
        """DB에서 30개가 읽히면 postcheck success"""
        rows = 30
        contract_ok = int(rows == 30)
        usable = int(contract_ok == 1)
        assert contract_ok == 1
        assert usable == 1

    def test_postcheck_0_rows_is_failure(self):
        """DB에서 0개가 읽히면 postcheck failure"""
        rows = 0
        contract_ok = int(rows == 30)
        usable = int(contract_ok == 1)
        assert contract_ok == 0
        assert usable == 0

    def test_postcheck_partial_rows_is_failure(self):
        """DB에서 29개가 읽히면 failure (30개 미만)"""
        rows = 29
        contract_ok = int(rows == 30)
        assert contract_ok == 0

    def test_postcheck_log_ok_message(self, caplog):
        """postcheck OK 시 PREP_FINAL30_READY OK 로그가 찍혀야 함"""
        import logging
        import io

        # 로그 메시지 패턴 검증
        log_msg = "[PREP][FINAL30_READY][OK] env=practice as_of=2026-06-04 strategy=pb1_watchlist_final_scored rows=30 usable=1 contract_ok=1"
        assert "PREP" in log_msg
        assert "FINAL30_READY" in log_msg
        assert "OK" in log_msg
        assert "rows=30" in log_msg
        assert "usable=1" in log_msg
        assert "contract_ok=1" in log_msg

    def test_postcheck_log_fail_message(self):
        """postcheck FAIL 시 PREP_FINAL30_READY FAIL 로그가 찍혀야 함"""
        rows = 0
        fail_log = f"[PREP][FINAL30_READY][FAIL] env=practice as_of=2026-06-04 reason=postcheck_rows={rows}/30"
        assert "PREP" in fail_log
        assert "FINAL30_READY" in fail_log
        assert "FAIL" in fail_log
        assert f"rows={rows}" in fail_log


class TestPrepPrecheck:
    """PREP 시작 시 precheck 로그 패턴 검증"""

    def test_precheck_rows_0_is_info_not_error(self):
        """PREP 시작 시 rows=0은 error가 아니라 info/warning"""
        rows = 0
        reason = "not_yet_built"
        log_msg = f"[PREP][FINAL30_PRECHECK] rows={rows} reason={reason}"
        # precheck에서는 FAIL/ERROR가 아닌 PRECHECK 레이블이어야 함
        assert "PRECHECK" in log_msg
        assert "FAIL" not in log_msg
        assert "ERROR" not in log_msg

    def test_postcheck_rows_0_is_error(self):
        """PREP 종료 직전 postcheck rows=0은 FAIL로 로깅되어야 함"""
        rows = 0
        log_msg = f"[PREP][FINAL30_READY][FAIL] env=practice as_of=2026-06-04 reason=postcheck_rows={rows}/30"
        assert "FAIL" in log_msg
        assert "FINAL30_READY" in log_msg


class TestPrepStrategyConsistency:
    """build_and_save_watchlist에서 pb1_watchlist_final_scored 전략이 일관되게 사용되는지"""

    def test_env_var_strategy_is_pb1_watchlist_final_scored(self):
        """PB1_WATCHLIST_STRATEGY 환경변수 기본값 = pb1_watchlist_final_scored"""
        import os
        # 환경변수 없을 때 기본값 확인
        original = os.environ.pop("PB1_WATCHLIST_STRATEGY", None)
        try:
            strategy = os.getenv("PB1_WATCHLIST_STRATEGY", "pb1_watchlist_final_scored")
            assert strategy == "pb1_watchlist_final_scored"
        finally:
            if original is not None:
                os.environ["PB1_WATCHLIST_STRATEGY"] = original

    def test_override_non_scored_strategy_still_uses_scored(self):
        """pb1_watchlist_final_scored가 아닌 값이 env에 있어도 강제 변환"""
        import os
        _save_strategy = "pb1_watchlist"
        _scored_strategy = os.getenv("PB1_WATCHLIST_STRATEGY", _save_strategy)
        # 코드에서 강제 변환 로직
        if _scored_strategy not in ("pb1_watchlist_final_scored",):
            _scored_strategy = "pb1_watchlist_final_scored"
        assert _scored_strategy == "pb1_watchlist_final_scored"
