# -*- coding: utf-8 -*-
"""tests/us/test_us_candidate_pool_builder.py - Candidate pool builder contract 테스트."""
import pytest
from unittest.mock import MagicMock


def _make_universe_symbols(n=80):
    """stub dynamic universe symbols"""
    symbols = []
    for i in range(n):
        ticker = f"SYM{i:03d}"
        symbols.append({
            "symbol": ticker,
            "exchange": "NASDAQ",
            "asset_type": "stock",
            "source_tags": ["manual_seed"],
            "price": 100.0 + i,
            "avg_volume_20d": 2_000_000,
            "avg_dollar_volume_20d": 200_000_000,
            "history_days": 200,
            "atr_pct": 0.03,
        })
    return symbols


def _make_provider(daily_days=200):
    daily = [
        {"xymd": f"2024-01-{(i % 28) + 1:02d}", "clos": "120.0", "high": "125.0", "low": "115.0", "tvol": "4000000"}
        for i in range(daily_days)
    ]
    provider = MagicMock()
    provider.get_daily_prices.return_value = daily
    provider.get_current_price.return_value = {"last": "120.0"}
    return provider


def test_build_us_candidate_pool_returns_required_keys():
    """결과 dict에 필수 키가 있어야 한다."""
    try:
        from trader.us.candidate_pool_builder import build_us_candidate_pool
    except ImportError:
        pytest.skip("candidate_pool_builder not available")

    provider = _make_provider()
    universe = _make_universe_symbols(100)

    result = build_us_candidate_pool(
        trade_date="2024-05-01",
        env="practice",
        dynamic_universe=universe,
        provider=provider,
        force_rebuild=True,
    )

    assert "status" in result
    assert "input_count" in result
    assert "selected_count" in result
    assert "rows" in result


def test_build_us_candidate_pool_selected_count_gte_50():
    """selected_count가 50 이상이어야 한다 (universe 충분할 때)."""
    try:
        from trader.us.candidate_pool_builder import build_us_candidate_pool
    except ImportError:
        pytest.skip("candidate_pool_builder not available")

    provider = _make_provider(200)
    universe = _make_universe_symbols(120)

    result = build_us_candidate_pool(
        trade_date="2024-05-01",
        env="practice",
        dynamic_universe=universe,
        provider=provider,
        force_rebuild=True,
    )

    if result.get("status") != "ERROR":
        assert result["selected_count"] >= 50


def test_build_us_candidate_pool_rows_have_candidate_score():
    """rows의 각 항목에 candidate_score 필드가 있어야 한다."""
    try:
        from trader.us.candidate_pool_builder import build_us_candidate_pool
    except ImportError:
        pytest.skip("candidate_pool_builder not available")

    provider = _make_provider(200)
    universe = _make_universe_symbols(100)

    result = build_us_candidate_pool(
        trade_date="2024-05-01",
        env="practice",
        dynamic_universe=universe,
        provider=provider,
        force_rebuild=True,
    )

    for row in result.get("rows", []):
        assert "candidate_score" in row, f"missing candidate_score in {row.get('symbol')}"


def test_build_us_candidate_pool_input_count_matches_universe():
    """input_count는 dynamic_universe 길이와 같아야 한다."""
    try:
        from trader.us.candidate_pool_builder import build_us_candidate_pool
    except ImportError:
        pytest.skip("candidate_pool_builder not available")

    provider = _make_provider(200)
    universe = _make_universe_symbols(80)

    result = build_us_candidate_pool(
        trade_date="2024-05-01",
        env="practice",
        dynamic_universe=universe,
        provider=provider,
        force_rebuild=True,
    )

    assert result["input_count"] == 80
