# -*- coding: utf-8 -*-
"""US signal-only DB fills tests."""
import pytest
from trader.us.db.repos import load_today_fills


def test_us_signal_only_db_fills_no_import_error():
    """signal-only 모드에서 load_today_fills import error 없음."""
    from trader.us.execution.fills import get_fills_today
    from unittest.mock import MagicMock
    
    provider = MagicMock()
    provider._offline = True
    
    # signal_only=True일 때 DB-only 조회 시도 (import error 없어야 함)
    result = get_fills_today(provider=provider, signal_only=True)
    
    # status는 OK 또는 TEMP_ERROR (DB 없을 수 있음)
    assert result["status"] in ("OK", "TEMP_ERROR")
    # error_type이 있다면 DB_QUERY
    if result.get("error_type"):
        assert result["error_type"] == "DB_QUERY"
    # import error가 아니어야 함
    if result.get("error"):
        assert "cannot import" not in result["error"].lower()


def test_load_today_fills_no_db_returns_empty():
    """DB 없을 때 load_today_fills는 빈 리스트 반환 (crash 없음)."""
    import os
    old_db_url = os.environ.get("PBCORE_DB_URL")
    try:
        # DB URL 제거
        if "PBCORE_DB_URL" in os.environ:
            del os.environ["PBCORE_DB_URL"]
        
        fills = load_today_fills(trade_date="2026-05-07")
        
        # 빈 리스트 반환되어야 함
        assert isinstance(fills, list)
        assert len(fills) == 0
    finally:
        # 복원
        if old_db_url:
            os.environ["PBCORE_DB_URL"] = old_db_url


def test_load_today_fills_memory_store():
    """in-memory 모드에서 load_today_fills 정상 동작."""
    from trader.us.db.repos import reset_memory_stores, save_fills
    
    reset_memory_stores()
    
    # in-memory에 fills 저장
    test_fills = [
        {
            "symbol": "AAPL",
            "exchange": "NASDAQ",
            "side": "BUY",
            "qty": 10,
            "price_usd": 150.0,
            "order_no": "ORD123",
            "filled_at": "2026-05-07T10:00:00",
        }
    ]
    save_fills(test_fills, trade_date="2026-05-07")
    
    # 조회
    result = load_today_fills(trade_date="2026-05-07")
    
    assert len(result) == 1
    assert result[0]["symbol"] == "AAPL"
    assert result[0]["qty"] == 10
