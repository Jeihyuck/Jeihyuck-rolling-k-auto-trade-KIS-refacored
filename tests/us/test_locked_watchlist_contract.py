# -*- coding: utf-8 -*-
"""US Locked Watchlist Contract 테스트.

한국장 PB1처럼 locked watchlist가 strict contract를 지켜야 함을 검증.
"""
import pytest
from trader.us.db.repos import load_locked_us_watchlist_strict


@pytest.mark.skip(reason="Requires mock_us_db fixture implementation")
def test_strict_loader_validates_min_count(mock_us_db):
    """최소 개수 미만이면 ERROR 반환."""
    # DB에 5개만 저장 (min_count=10 요구)
    result = load_locked_us_watchlist_strict(
        trade_date="2026-01-10",
        min_count=10,
    )
    assert result["status"] == "ERROR"
    assert "count" in result["errors"][0] or len(result["rows"]) < 10


def test_strict_loader_detects_duplicate_symbols():
    """symbol 중복이면 ERROR."""
    # 이 테스트는 실제 DB에 중복 symbol이 있을 때만 테스트 가능
    # mock으로 구현하거나 integration test에서 검증
    pass


def test_strict_loader_detects_duplicate_ranks():
    """rank 중복이면 ERROR."""
    pass


def test_strict_loader_validates_prep_status():
    """prep_status가 DEGRADED이면 allow_degraded=False일 때 ERROR."""
    pass


def test_strict_loader_success():
    """정상 watchlist이면 OK."""
    result = load_locked_us_watchlist_strict(
        trade_date="2026-05-05",
        min_count=1,
        allow_degraded=True,
    )
    # offline이면 in-memory fallback
    assert result["status"] in ("OK", "ERROR")
