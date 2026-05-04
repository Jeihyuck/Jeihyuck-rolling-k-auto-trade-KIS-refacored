"""tests/test_position_age_timezone.py

position_age.to_kst_date() timezone 처리 검증.

요구사항:
- datetime.fromisoformat(s).date()로 바로 반환하지 말 것
- timezone이 있으면 반드시 KST로 astimezone 후 date 추출
- UTC 15시 이후 timestamp가 KST 다음날로 넘어가는 테스트 추가
"""
from __future__ import annotations

import sys
from datetime import datetime, date
from pathlib import Path
import pytz

sys.path.append(str(Path(__file__).resolve().parents[1]))

from trader.position_age import to_kst_date


def test_iso_string_with_utc_timezone_converts_to_kst():
    """ISO format 문자열에 UTC timezone이 있으면 KST로 변환 후 date 추출."""
    # UTC 2026-05-01 15:00:00 → KST 2026-05-02 00:00:00 (다음날)
    utc_str = "2026-05-01T15:00:00+00:00"
    result = to_kst_date(utc_str)
    
    # KST로 변환하면 다음날이 되어야 함
    assert result == date(2026, 5, 2), f"Expected 2026-05-02, got {result}"


def test_iso_string_with_utc_timezone_before_15_stays_same_day():
    """UTC 14시는 KST로 변환해도 같은 날."""
    # UTC 2026-05-01 14:59:59 → KST 2026-05-01 23:59:59 (같은 날)
    utc_str = "2026-05-01T14:59:59+00:00"
    result = to_kst_date(utc_str)
    
    assert result == date(2026, 5, 1), f"Expected 2026-05-01, got {result}"


def test_iso_string_with_kst_timezone_no_conversion():
    """ISO format 문자열에 KST timezone이 있으면 그대로 date 추출."""
    # KST 2026-05-01 09:00:00
    kst_str = "2026-05-01T09:00:00+09:00"
    result = to_kst_date(kst_str)
    
    assert result == date(2026, 5, 1), f"Expected 2026-05-01, got {result}"


def test_iso_string_without_timezone_assumes_kst():
    """ISO format 문자열에 timezone이 없으면 KST로 간주."""
    # naive datetime → KST로 localize
    naive_str = "2026-05-01T09:00:00"
    result = to_kst_date(naive_str)
    
    assert result == date(2026, 5, 1), f"Expected 2026-05-01, got {result}"


def test_utc_15_after_becomes_next_day_in_kst():
    """UTC 15:00 이후는 KST 다음날이 되어야 함."""
    # UTC 2026-05-01 15:00:00 → KST 2026-05-02 00:00:00
    utc_dt = datetime(2026, 5, 1, 15, 0, 0, tzinfo=pytz.UTC)
    utc_str = utc_dt.isoformat()
    result = to_kst_date(utc_str)
    
    assert result == date(2026, 5, 2), f"Expected 2026-05-02, got {result}"


def test_utc_16_becomes_next_day_in_kst():
    """UTC 16:00 → KST 01:00 (다음날)."""
    # UTC 2026-05-01 16:00:00 → KST 2026-05-02 01:00:00
    utc_str = "2026-05-01T16:00:00+00:00"
    result = to_kst_date(utc_str)
    
    assert result == date(2026, 5, 2), f"Expected 2026-05-02, got {result}"


def test_utc_23_becomes_next_day_in_kst():
    """UTC 23:00 → KST 08:00 (다음날)."""
    # UTC 2026-05-01 23:00:00 → KST 2026-05-02 08:00:00
    utc_str = "2026-05-01T23:00:00+00:00"
    result = to_kst_date(utc_str)
    
    assert result == date(2026, 5, 2), f"Expected 2026-05-02, got {result}"


def test_datetime_object_with_utc_converts_to_kst():
    """datetime 객체도 UTC → KST 변환 수행."""
    # UTC 2026-05-01 15:30:00 → KST 2026-05-02 00:30:00
    utc_dt = datetime(2026, 5, 1, 15, 30, 0, tzinfo=pytz.UTC)
    result = to_kst_date(utc_dt)
    
    assert result == date(2026, 5, 2), f"Expected 2026-05-02, got {result}"


def test_unix_timestamp_converts_to_kst():
    """Unix timestamp도 KST로 변환."""
    # 2026-05-01 15:00:00 UTC → KST 2026-05-02 00:00:00
    # Unix timestamp 계산 (대략적인 값)
    utc_dt = datetime(2026, 5, 1, 15, 0, 0, tzinfo=pytz.UTC)
    unix_ts = utc_dt.timestamp()
    result = to_kst_date(unix_ts)
    
    assert result == date(2026, 5, 2), f"Expected 2026-05-02, got {result}"


def test_iso_string_with_different_timezones():
    """다양한 timezone의 ISO 문자열 처리."""
    # PST (UTC-8) 2026-05-01 01:00:00 → UTC 09:00:00 → KST 18:00:00 (같은 날)
    pst_str = "2026-05-01T01:00:00-08:00"
    result = to_kst_date(pst_str)
    assert result == date(2026, 5, 1), f"Expected 2026-05-01, got {result}"
    
    # JST (UTC+9, same as KST) 2026-05-01 09:00:00
    jst_str = "2026-05-01T09:00:00+09:00"
    result = to_kst_date(jst_str)
    assert result == date(2026, 5, 1), f"Expected 2026-05-01, got {result}"


def test_date_object_returns_as_is():
    """date 객체는 그대로 반환."""
    d = date(2026, 5, 1)
    result = to_kst_date(d)
    
    assert result == d, f"Expected {d}, got {result}"


def test_none_returns_none():
    """None 입력은 None 반환."""
    result = to_kst_date(None)
    assert result is None


def test_empty_string_returns_none():
    """빈 문자열은 None 반환."""
    result = to_kst_date("")
    assert result is None


def test_invalid_string_returns_none():
    """잘못된 형식의 문자열은 None 반환."""
    result = to_kst_date("invalid-date")
    assert result is None


def test_yyyymmdd_string_format():
    """YYYYMMDD 형식 문자열 처리."""
    result = to_kst_date("20260501")
    assert result == date(2026, 5, 1), f"Expected 2026-05-01, got {result}"


def test_critical_utc_boundary_at_15():
    """UTC 15시 경계 테스트 (KST 00시)."""
    # UTC 14:59:59 → KST 23:59:59 (같은 날)
    before_str = "2026-05-01T14:59:59+00:00"
    result_before = to_kst_date(before_str)
    assert result_before == date(2026, 5, 1)
    
    # UTC 15:00:00 → KST 00:00:00 (다음날)
    after_str = "2026-05-01T15:00:00+00:00"
    result_after = to_kst_date(after_str)
    assert result_after == date(2026, 5, 2)


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
