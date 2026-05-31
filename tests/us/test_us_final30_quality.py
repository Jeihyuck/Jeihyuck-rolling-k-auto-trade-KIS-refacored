# -*- coding: utf-8 -*-
"""tests/us/test_us_final30_quality.py - Final30 contract validator 테스트."""
import pytest


def _make_valid_rows(n=30, start_rank=1):
    rows = []
    for i in range(n):
        ticker = f"SYM{i + 100:04d}"
        rows.append({
            "symbol": ticker,
            "exchange": "NASDAQ",
            "asset_type": "stock",
            "rank_final30": start_rank + i,
            "score_final": 0.5 + i * 0.01,
            "agent_a_score": 0.4 + i * 0.01,
            "agent_b_score": 0.45 + i * 0.01,
            "candidate_score": 0.55,
            "tech_score": 0.6,
            "rs_20d": 1.05,
            "rs_60d": 1.10,
            "rs_120d": 1.08,
            "rs_percentile": 0.75,
            "trend_score": 0.65,
            "breakout_score": 0.5,
            "pullback_score": 0.6,
            "momentum_score": 0.55,
            "vcp_score": 0.4,
            "volume_accel_score": 0.7,
            "liquidity_score": 0.8,
            "risk_score": 0.3,
            "atr_pct": 0.03,
            "close": 120.0,
            "ma20": 115.0,
            "ma50": 110.0,
            "ma150": 105.0,
            "pullback_pct": 0.05,
            "entry_style_selected": "pb1_pullback",
            "source_tags": ["manual_seed"],
            "reason_json": {"agent_a": "RS+", "agent_b": "pullback"},
            "trade_date": "2024-05-01",
        })
    return rows


def test_valid_30_rows_passes():
    """정상 30행은 ok=True여야 한다."""
    try:
        from trader.us.final30_quality import verify_us_final30_scored_rows
    except ImportError:
        pytest.skip("final30_quality not available")

    rows = _make_valid_rows(30)
    result = verify_us_final30_scored_rows(rows, required_rows=30, source="test")
    assert result["ok"] is True, f"errors: {result.get('errors')}"


def test_29_rows_fails():
    """29행이면 ok=False여야 한다."""
    try:
        from trader.us.final30_quality import verify_us_final30_scored_rows
    except ImportError:
        pytest.skip("final30_quality not available")

    rows = _make_valid_rows(29)
    result = verify_us_final30_scored_rows(rows, required_rows=30, source="test")
    assert result["ok"] is False


def test_31_rows_fails():
    """31행이면 ok=False여야 한다."""
    try:
        from trader.us.final30_quality import verify_us_final30_scored_rows
    except ImportError:
        pytest.skip("final30_quality not available")

    rows = _make_valid_rows(31)
    result = verify_us_final30_scored_rows(rows, required_rows=30, source="test")
    assert result["ok"] is False


def test_duplicate_symbol_fails():
    """중복 symbol이 있으면 ok=False여야 한다."""
    try:
        from trader.us.final30_quality import verify_us_final30_scored_rows
    except ImportError:
        pytest.skip("final30_quality not available")

    rows = _make_valid_rows(30)
    rows[0]["symbol"] = rows[1]["symbol"]  # 중복
    result = verify_us_final30_scored_rows(rows, required_rows=30, source="test")
    assert result["ok"] is False


def test_duplicate_rank_fails():
    """중복 rank_final30이 있으면 ok=False여야 한다."""
    try:
        from trader.us.final30_quality import verify_us_final30_scored_rows
    except ImportError:
        pytest.skip("final30_quality not available")

    rows = _make_valid_rows(30)
    rows[0]["rank_final30"] = rows[1]["rank_final30"]  # 중복 rank
    result = verify_us_final30_scored_rows(rows, required_rows=30, source="test")
    assert result["ok"] is False


def test_zero_score_final_fails():
    """score_final == 0인 행이 있으면 ok=False여야 한다."""
    try:
        from trader.us.final30_quality import verify_us_final30_scored_rows
    except ImportError:
        pytest.skip("final30_quality not available")

    rows = _make_valid_rows(30)
    rows[5]["score_final"] = 0.0
    result = verify_us_final30_scored_rows(rows, required_rows=30, source="test")
    assert result["ok"] is False


def test_missing_entry_style_fails():
    """entry_style_selected가 없으면 ok=False여야 한다."""
    try:
        from trader.us.final30_quality import verify_us_final30_scored_rows
    except ImportError:
        pytest.skip("final30_quality not available")

    rows = _make_valid_rows(30)
    rows[10]["entry_style_selected"] = ""
    result = verify_us_final30_scored_rows(rows, required_rows=30, source="test")
    assert result["ok"] is False


def test_etf_over_5_fails():
    """ETF가 6개 이상이면 ok=False여야 한다."""
    try:
        from trader.us.final30_quality import verify_us_final30_scored_rows
    except ImportError:
        pytest.skip("final30_quality not available")

    rows = _make_valid_rows(24)
    etf_tickers = ["SPY", "QQQ", "QQQM", "SMH", "SOXX", "IWM"]
    for i, ticker in enumerate(etf_tickers):
        rows.append({
            "symbol": ticker,
            "exchange": "NYSE",
            "asset_type": "etf",
            "rank_final30": 25 + i,
            "score_final": 0.8 + i * 0.01,
            "agent_a_score": 0.7,
            "agent_b_score": 0.75,
            "candidate_score": 0.9,
            "tech_score": 0.8,
            "rs_20d": 1.05,
            "rs_60d": 1.10,
            "rs_120d": 1.08,
            "rs_percentile": 0.85,
            "trend_score": 0.75,
            "breakout_score": 0.6,
            "pullback_score": 0.5,
            "momentum_score": 0.65,
            "vcp_score": 0.3,
            "volume_accel_score": 0.8,
            "liquidity_score": 0.9,
            "risk_score": 0.2,
            "close": 400.0,
            "ma20": 395.0,
            "ma50": 390.0,
            "ma150": 380.0,
            "atr_pct": 0.01,
            "pullback_pct": 0.02,
            "entry_style_selected": "etf_trend",
            "source_tags": ["core_etf"],
            "reason_json": {},
            "trade_date": "2024-05-01",
        })

    result = verify_us_final30_scored_rows(rows, required_rows=30, source="test")
    assert result["ok"] is False
