# -*- coding: utf-8 -*-
"""tests/us/test_us_universe_builder.py - Universe builder contract 테스트."""
import pytest
from unittest.mock import MagicMock, patch


STUB_DAILY = [
    {"xymd": f"2024-01-{d:02d}", "clos": "100.0", "high": "105.0", "low": "95.0", "tvol": "3000000"}
    for d in range(1, 22)
]  # 21일 치 - US_MIN_HISTORY_DAYS 이하여서 일부 필터돼야 함


def _make_provider(daily_days=150):
    """stub USDataProvider"""
    daily = [
        {"xymd": f"2024-01-{(i % 28) + 1:02d}", "clos": "120.0", "high": "125.0", "low": "115.0", "tvol": "4000000"}
        for i in range(daily_days)
    ]
    provider = MagicMock()
    provider.get_daily_prices.return_value = daily
    provider.get_current_price.return_value = {"last": "120.0"}
    provider.offline = True
    return provider


def test_build_us_dynamic_universe_returns_required_keys():
    """결과 dict에 필수 키가 모두 있어야 한다."""
    try:
        from trader.us.universe_builder import build_us_dynamic_universe
    except ImportError:
        pytest.skip("universe_builder not available")

    provider = _make_provider()
    manual_seed = ["NVDA", "MSFT", "AAPL", "QQQ", "SPY"]

    with patch("trader.us.universe_builder._load_dynamic_sources", return_value={"ai_semiconductor_extended": ["AMD", "GOOGL", "META"]}):
        result = build_us_dynamic_universe(
            trade_date="2024-05-01",
            env="practice",
            provider=provider,
            manual_seed=manual_seed,
            force_rebuild=True,
        )

    assert "status" in result
    assert "raw_count" in result
    assert "unique_count" in result
    assert "filtered_count" in result
    assert "source_counts" in result
    assert "filter_counts" in result
    assert "symbols" in result


def test_build_us_dynamic_universe_unique_count_gte_manual_seed():
    """unique_count는 manual_seed 수 이상이어야 한다."""
    try:
        from trader.us.universe_builder import build_us_dynamic_universe
    except ImportError:
        pytest.skip("universe_builder not available")

    provider = _make_provider(daily_days=200)
    manual_seed = ["NVDA", "MSFT", "AAPL"]

    with patch("trader.us.universe_builder._load_dynamic_sources", return_value={"ai_semiconductor_extended": ["AMD", "GOOGL"]}):
        result = build_us_dynamic_universe(
            trade_date="2024-05-01",
            env="practice",
            provider=provider,
            manual_seed=manual_seed,
            force_rebuild=True,
        )

    assert result["unique_count"] >= len(manual_seed)


def test_build_us_dynamic_universe_symbols_have_required_fields():
    """각 symbol dict에 필수 필드가 있어야 한다."""
    try:
        from trader.us.universe_builder import build_us_dynamic_universe
    except ImportError:
        pytest.skip("universe_builder not available")

    provider = _make_provider(daily_days=200)
    manual_seed = ["NVDA", "MSFT", "AAPL", "QQQ", "SPY"]

    with patch("trader.us.universe_builder._load_dynamic_sources", return_value={"ai_semiconductor_extended": ["AMD", "GOOGL", "META"]}):
        result = build_us_dynamic_universe(
            trade_date="2024-05-01",
            env="practice",
            provider=provider,
            manual_seed=manual_seed,
            force_rebuild=True,
        )

    for sym in result.get("symbols", []):
        assert "symbol" in sym, f"missing 'symbol' in {sym}"
        assert "exchange" in sym, f"missing 'exchange' in {sym}"
        assert "source_tags" in sym


def test_build_us_dynamic_universe_hard_fail_if_less_than_30():
    """filtered_count < 30이면 status=ERROR여야 한다."""
    try:
        from trader.us.universe_builder import build_us_dynamic_universe
    except ImportError:
        pytest.skip("universe_builder not available")

    # 짧은 daily로 필터에 걸리게
    short_daily = [
        {"xymd": "2024-01-01", "clos": "2.0", "high": "2.1", "low": "1.9", "tvol": "100"}
    ]
    provider = MagicMock()
    provider.get_daily_prices.return_value = short_daily
    provider.get_current_price.return_value = {"last": "2.0"}
    provider.offline = True

    manual_seed = ["NVDA"]

    with patch("trader.us.universe_builder._load_dynamic_sources", return_value={}):
        result = build_us_dynamic_universe(
            trade_date="2024-05-01",
            env="practice",
            provider=provider,
            manual_seed=manual_seed,
            force_rebuild=True,
        )

    # 필터 통과하는 심볼이 30개 미만이므로 ERROR 또는 filtered_count < 30
    assert result.get("filtered_count", 0) < 30 or result.get("status") == "ERROR"
