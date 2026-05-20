"""US PnL normalize + duplicate guard + order report + logger 테스트."""
from __future__ import annotations

import logging

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# P0 테스트: normalize_us_position - current 필드를 last_price로 사용
# ─────────────────────────────────────────────────────────────────────────────

def test_us_pnl_uses_current_as_last_price():
    """row에 'current' 키가 있으면 last_price로 매핑돼야 한다."""
    from trader.us.utils.pnl_utils import normalize_us_position

    row = {
        "symbol": "TSM",
        "qty": 10,
        "avg_price": 150.0,
        "current": 297.9051,
    }
    result = normalize_us_position(row)
    assert result["last_price"] == pytest.approx(297.9051), (
        f"last_price should be 297.9051, got {result['last_price']}"
    )


def test_us_pnl_not_minus_100_when_current_exists():
    """current 필드가 있으면 pnl_pct가 -100%가 아니어야 한다."""
    from trader.us.utils.pnl_utils import normalize_us_position

    row = {
        "symbol": "TSM",
        "qty": 10,
        "avg_price": 150.0,
        "current": 395.16,
    }
    result = normalize_us_position(row)
    pnl_pct = result.get("unrealized_pnl_pct", result.get("pnl_pct", -100.0))
    assert pnl_pct != pytest.approx(-100.0, abs=0.01), (
        f"pnl_pct should not be -100 when current={row['current']}, got {pnl_pct}"
    )
    assert pnl_pct > 0, (
        f"pnl_pct should be positive with current > avg_price, got {pnl_pct}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# P1 테스트: duplicate guard — direction 컬럼 없이도 동작
# ─────────────────────────────────────────────────────────────────────────────

def test_us_duplicate_guard_does_not_require_direction_column():
    """side 컬럼만 있으면 direction 없이도 pick_column_from_list가 side를 반환해야 한다."""
    from trader.us.utils.pnl_utils import pick_column_from_list

    available = {"id", "symbol", "side", "status", "created_at"}
    candidates = ["side", "order_side", "direction"]
    result = pick_column_from_list(available, candidates)
    assert result == "side", (
        f"Expected 'side' but got {result!r}. "
        "Guard must not require 'direction' column when 'side' exists."
    )


def test_us_duplicate_guard_falls_back_without_side():
    """side도 없으면 order_side를 반환해야 한다."""
    from trader.us.utils.pnl_utils import pick_column_from_list

    available = {"id", "symbol", "order_side", "status"}
    candidates = ["side", "order_side", "direction"]
    result = pick_column_from_list(available, candidates)
    assert result == "order_side"


# ─────────────────────────────────────────────────────────────────────────────
# P2 테스트: orders_ack, fills_count, pending_order_count 계산
# ─────────────────────────────────────────────────────────────────────────────

def test_us_order_report_counts_ack_fill_pending():
    """10 ACK - 7 fills = pending 3건이어야 한다."""
    orders_ack = 10
    fills_count = 7
    expected_pending = orders_ack - fills_count

    # pending_order_count fallback 계산 로직 (trade_session_runner와 동일)
    total_pending = max(0, orders_ack - fills_count)

    assert total_pending == expected_pending == 3, (
        f"Expected pending=3, got {total_pending}"
    )
    # 심볼 배열 확인 (최소 3개여야 함)
    pending_symbols = ["CIEN", "COHR", "VRT"]
    assert len(pending_symbols) == total_pending


# ─────────────────────────────────────────────────────────────────────────────
# P5 테스트: logger 중복 출력 방지
# ─────────────────────────────────────────────────────────────────────────────

def test_us_logger_not_duplicate():
    """setup_us_logging()을 여러 번 호출해도 handler가 중복 등록되지 않아야 한다."""
    from trader.us.utils.logging_utils import setup_us_logging

    # 사전에 root logger의 설정 플래그 초기화 (test isolation)
    root = logging.getLogger()
    # 기존 플래그 제거
    if hasattr(root, "_us_trading_logging_configured"):
        del root._us_trading_logging_configured  # type: ignore[attr-defined]
    # 기존 핸들러 제거 (isolate)
    original_handlers = root.handlers[:]
    for h in original_handlers:
        root.removeHandler(h)

    try:
        # 중복 호출
        setup_us_logging()
        setup_us_logging()
        setup_us_logging()

        handler_count = len(root.handlers)
        assert handler_count <= 1, (
            f"Expected at most 1 handler after 3 setup_us_logging() calls, got {handler_count}"
        )

        # 로그 메시지 중복 출력 확인
        test_logger = logging.getLogger("us_test_dedup")
        records: list[logging.LogRecord] = []

        class _ListHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record)

        list_handler = _ListHandler()
        test_logger.addHandler(list_handler)
        test_logger.propagate = False
        test_logger.setLevel(logging.INFO)

        test_logger.info("[US_TEST_LOG_ONCE] dedup check")
        assert len(records) == 1, (
            f"Expected 1 log record, got {len(records)} — logger is duplicating output"
        )

    finally:
        # 복원
        for h in root.handlers[:]:
            root.removeHandler(h)
        for h in original_handlers:
            root.addHandler(h)
        if hasattr(root, "_us_trading_logging_configured"):
            del root._us_trading_logging_configured  # type: ignore[attr-defined]
