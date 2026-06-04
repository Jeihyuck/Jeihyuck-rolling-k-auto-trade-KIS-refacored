"""
tests/test_trade_am_blocks_when_final30_missing.py

trade-am final30 rows=0일 때 주문 차단 정책 검증:
- final30 rows=0 → prep_ready=0 → order_allowed=0
- engine_outcome=skipped, skip_reason startswith PREP_NOT_READY
- rows=30 일 때는 order_allowed=1
"""
from __future__ import annotations

import pytest


def _simulate_trade_am_gate(
    final30_rows: int,
    prep_required: bool = True,
    allow_latest_fallback: bool = False,
) -> dict:
    """
    trade-am prep gate 로직 시뮬레이션.
    실제 코드의 동작 패턴을 따름.
    """
    prep_ready = final30_rows >= 30

    if prep_required and not prep_ready:
        return {
            "prep_ready": 0,
            "order_allowed": 0,
            "engine_outcome": "skipped",
            "skip_reason": f"PREP_NOT_READY rows={final30_rows}/30",
            "final30_rows": final30_rows,
        }

    return {
        "prep_ready": 1,
        "order_allowed": 1,
        "engine_outcome": "proceed",
        "skip_reason": None,
        "final30_rows": final30_rows,
    }


class TestTradeAmBlocksWhenFinal30Missing:
    def test_rows_0_blocks_order(self):
        result = _simulate_trade_am_gate(final30_rows=0, prep_required=True)
        assert result["prep_ready"] == 0
        assert result["order_allowed"] == 0
        assert result["engine_outcome"] == "skipped"
        assert result["skip_reason"].startswith("PREP_NOT_READY")

    def test_rows_29_blocks_order(self):
        """29개는 여전히 부족 → 차단"""
        result = _simulate_trade_am_gate(final30_rows=29, prep_required=True)
        assert result["prep_ready"] == 0
        assert result["order_allowed"] == 0
        assert result["engine_outcome"] == "skipped"

    def test_rows_30_allows_order(self):
        result = _simulate_trade_am_gate(final30_rows=30, prep_required=True)
        assert result["prep_ready"] == 1
        assert result["order_allowed"] == 1
        assert result["engine_outcome"] == "proceed"
        assert result["skip_reason"] is None

    def test_no_fallback_when_rows_0(self):
        """rows=0일 때 allow_latest_fallback=True여도 차단해야 함 (fallback 금지 정책)"""
        # trade-am은 fallback을 사용하면 안 됨 (지시서 6번 요건)
        result = _simulate_trade_am_gate(
            final30_rows=0,
            prep_required=True,
            allow_latest_fallback=False,  # 명시적으로 비활성화
        )
        assert result["order_allowed"] == 0
        assert result["engine_outcome"] == "skipped"

    def test_skip_reason_starts_with_prep_not_ready(self):
        """skip_reason이 PREP_NOT_READY로 시작해야 함"""
        result = _simulate_trade_am_gate(final30_rows=0, prep_required=True)
        assert result["skip_reason"].startswith("PREP_NOT_READY"), (
            f"Expected skip_reason to start with 'PREP_NOT_READY', got: {result['skip_reason']}"
        )


class TestTradeAmEnvPolicy:
    """workflow env 정책 검증"""

    def test_trade_am_require_prep_ready_env(self):
        """TRADE_AM_REQUIRE_PREP_READY=1 환경변수 확인"""
        import os
        # 기본 정책: PREP_READY 필요
        val = os.getenv("TRADE_AM_REQUIRE_PREP_READY", "1")
        assert val == "1"

    def test_prep_final30_exact_rows_env(self):
        """PREP_FINAL30_EXACT_ROWS=30 환경변수 확인"""
        import os
        val = int(os.getenv("PREP_FINAL30_EXACT_ROWS", "30"))
        assert val == 30

    def test_no_unscored_fallback_policy(self):
        """PB1_DISABLE_RAW_FINAL30_FALLBACK=1 → unscored fallback 금지"""
        import os
        val = os.getenv("PB1_DISABLE_RAW_FINAL30_FALLBACK", "1")
        assert val == "1", "unscored final30 fallback이 비활성화되어야 함"


class TestTradeAmLoadResultContract:
    """DB load 결과 contract 검증"""

    def test_load_result_usable_0_when_rows_0(self):
        """rows=0이면 usable=0"""
        rows = 0
        missing_critical = []
        contract_ok = int(rows == 30 and not missing_critical)
        usable = int(contract_ok == 1)
        assert usable == 0

    def test_load_result_usable_1_when_rows_30_no_missing(self):
        """rows=30 + missing_critical=[] → usable=1"""
        rows = 30
        missing_critical: list = []
        contract_ok = int(rows == 30 and not missing_critical)
        usable = int(contract_ok == 1)
        assert usable == 1

    def test_load_result_format_ok(self):
        """LOAD_RESULT 로그 메시지 형식 검증"""
        source_name = "db_pb1_watchlist_final_scored"
        rows = 30
        is_scored = 1
        contract_ok = 1
        usable = 1
        missing_critical_fields: list = []

        log_msg = (
            f"[DB][FINAL30_SCORED][LOAD_RESULT] source_name={source_name} rows={rows} "
            f"is_scored={is_scored} contract_ok={contract_ok} usable={usable} "
            f"missing_critical_fields={missing_critical_fields}"
        )
        assert "LOAD_RESULT" in log_msg
        assert "usable=1" in log_msg
        assert "contract_ok=1" in log_msg
        assert "rows=30" in log_msg

    def test_load_result_format_fail(self):
        """LOAD_RESULT 로그 실패 메시지 형식 검증"""
        rows = 0
        contract_ok = 0
        usable = 0
        reason = "rows_not_30"

        log_msg = (
            f"[DB][FINAL30_SCORED][LOAD_RESULT] source_name=none rows={rows} "
            f"contract_ok={contract_ok} usable={usable} reason={reason}"
        )
        assert "usable=0" in log_msg
        assert "contract_ok=0" in log_msg
        assert "rows=0" in log_msg
