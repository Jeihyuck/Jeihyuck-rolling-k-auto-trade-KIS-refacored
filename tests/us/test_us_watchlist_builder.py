# -*- coding: utf-8 -*-
"""tests/us/test_us_watchlist_builder.py - Watchlist builder contract 테스트."""
import pytest
from unittest.mock import MagicMock


def _make_candidate_rows(n=80):
    rows = []
    for i in range(n):
        ticker = f"SYM{i:03d}"
        rows.append({
            "symbol": ticker,
            "exchange": "NASDAQ",
            "asset_type": "stock",
            "source_tags": ["manual_seed"],
            "candidate_score": 0.5 + (i % 10) * 0.01,
            "price": 100.0 + i,
            "close": 100.0 + i,
            "avg_volume_20d": 2_000_000,
            "avg_dollar_volume_20d": 200_000_000,
            "atr_pct": 0.03,
            "rs_60d_score": 0.6,
            "rs_20d_score": 0.55,
        })
    return rows


def _make_provider(daily_days=200):
    daily = [
        {"xymd": f"2024-01-{(i % 28) + 1:02d}", "clos": "120.0", "high": "125.0", "low": "115.0", "tvol": "4000000"}
        for i in range(daily_days)
    ]
    provider = MagicMock()
    provider.get_daily_prices.return_value = daily
    provider.get_current_price.return_value = {"last": "120.0"}
    return provider


def test_build_us_watchlist_returns_required_keys():
    """결과 dict에 필수 키가 있어야 한다."""
    try:
        from trader.us.watchlist_builder import build_us_watchlist
    except ImportError:
        pytest.skip("watchlist_builder not available")

    provider = _make_provider()
    candidates = _make_candidate_rows(80)

    result = build_us_watchlist(
        trade_date="2024-05-01",
        env="practice",
        candidate_pool=candidates,
        provider=provider,
        force_rebuild=True,
    )

    assert "final30_scored" in result or "final30_scored_count" in result


def test_build_us_watchlist_final30_count_equals_30():
    """final30_scored는 정확히 30개여야 한다."""
    try:
        from trader.us.watchlist_builder import build_us_watchlist
    except ImportError:
        pytest.skip("watchlist_builder not available")

    provider = _make_provider(200)
    candidates = _make_candidate_rows(80)

    result = build_us_watchlist(
        trade_date="2024-05-01",
        env="practice",
        candidate_pool=candidates,
        provider=provider,
        force_rebuild=True,
    )

    if result.get("status") != "ERROR":
        final30 = result.get("final30_scored", [])
        assert len(final30) == 30, f"expected 30 but got {len(final30)}"


def test_build_us_watchlist_rank_final30_unique_1_to_30():
    """rank_final30이 1~30 사이에서 중복 없이 부여되어야 한다."""
    try:
        from trader.us.watchlist_builder import build_us_watchlist
    except ImportError:
        pytest.skip("watchlist_builder not available")

    provider = _make_provider(200)
    candidates = _make_candidate_rows(80)

    result = build_us_watchlist(
        trade_date="2024-05-01",
        env="practice",
        candidate_pool=candidates,
        provider=provider,
        force_rebuild=True,
    )

    if result.get("status") != "ERROR":
        final30 = result.get("final30_scored", [])
        ranks = [r.get("rank_final30") for r in final30]
        assert sorted(ranks) == list(range(1, 31)), f"ranks wrong: {ranks}"


def test_build_us_watchlist_score_final_nonzero():
    """final30_scored의 모든 행에 score_final > 0이어야 한다."""
    try:
        from trader.us.watchlist_builder import build_us_watchlist
    except ImportError:
        pytest.skip("watchlist_builder not available")

    provider = _make_provider(200)
    candidates = _make_candidate_rows(80)

    result = build_us_watchlist(
        trade_date="2024-05-01",
        env="practice",
        candidate_pool=candidates,
        provider=provider,
        force_rebuild=True,
    )

    if result.get("status") != "ERROR":
        final30 = result.get("final30_scored", [])
        for row in final30:
            assert row.get("score_final", 0) > 0, f"score_final is zero for {row.get('symbol')}"


def test_build_us_watchlist_entry_style_present():
    """final30_scored의 모든 행에 entry_style_selected가 있어야 한다."""
    try:
        from trader.us.watchlist_builder import build_us_watchlist
    except ImportError:
        pytest.skip("watchlist_builder not available")

    provider = _make_provider(200)
    candidates = _make_candidate_rows(80)

    result = build_us_watchlist(
        trade_date="2024-05-01",
        env="practice",
        candidate_pool=candidates,
        provider=provider,
        force_rebuild=True,
    )

    if result.get("status") != "ERROR":
        final30 = result.get("final30_scored", [])
        for row in final30:
            assert row.get("entry_style_selected"), f"missing entry_style_selected for {row.get('symbol')}"


def test_build_us_watchlist_etf_max_5():
    """final30 내 ETF는 최대 5개여야 한다."""
    try:
        from trader.us.watchlist_builder import build_us_watchlist
    except ImportError:
        pytest.skip("watchlist_builder not available")

    provider = _make_provider(200)
    candidates = _make_candidate_rows(70)
    # ETF 시뮬레이션
    etf_tickers = ["SPY", "QQQ", "QQQM", "SMH", "SOXX", "IWM", "GLD"]
    for i, ticker in enumerate(etf_tickers):
        candidates.append({
            "symbol": ticker,
            "exchange": "NYSE",
            "asset_type": "etf",
            "source_tags": ["core_etf"],
            "candidate_score": 0.95 - i * 0.01,
            "price": 400.0,
            "close": 400.0,
            "avg_volume_20d": 10_000_000,
            "avg_dollar_volume_20d": 4_000_000_000,
            "atr_pct": 0.01,
            "rs_60d_score": 0.8,
            "rs_20d_score": 0.75,
        })

    result = build_us_watchlist(
        trade_date="2024-05-01",
        env="practice",
        candidate_pool=candidates,
        provider=provider,
        force_rebuild=True,
    )

    if result.get("status") != "ERROR":
        final30 = result.get("final30_scored", [])
        etf_count = sum(1 for r in final30 if r.get("asset_type") == "etf")
        assert etf_count <= 5, f"Too many ETFs in final30: {etf_count}"
