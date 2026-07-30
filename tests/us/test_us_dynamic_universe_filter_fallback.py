# -*- coding: utf-8 -*-
"""tests/us/test_us_dynamic_universe_filter_fallback.py

dynamic_universe strict/relaxed/fallback 3단계 필터 + price fallback 테스트.
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# 헬퍼
# ---------------------------------------------------------------------------

def _make_daily(count: int, close: float = 120.0, volume: int = 2_000_000) -> list[dict]:
    """정규화된 형태의 stub daily rows (normalize_daily_rows 이후 포맷)."""
    from datetime import date, timedelta
    rows = []
    today = date(2026, 5, 29)
    for i in range(count):
        d = today - timedelta(days=count - i - 1)
        rows.append({
            "xymd": d.strftime("%Y%m%d"),
            "date": d.strftime("%Y%m%d"),
            "clos": str(close),
            "close": close,
            "open": close * 0.99,
            "high": close * 1.01,
            "low": close * 0.98,
            "tvol": str(volume),
            "volume": volume,
        })
    return rows


def _make_provider(
    current_price: float | None = 120.0,
    daily_count: int = 70,
    daily_close: float = 120.0,
    raise_current: bool = False,
    raise_daily: bool = False,
) -> MagicMock:
    provider = MagicMock()

    if raise_current:
        provider.get_current_price.side_effect = Exception("price_fetch_error")
    else:
        last_val = str(current_price) if current_price is not None else None
        provider.get_current_price.return_value = {"last": last_val, "symbol": "TEST"}

    if raise_daily:
        provider.get_daily_prices.side_effect = Exception("daily_fetch_error")
    else:
        provider.get_daily_prices.return_value = _make_daily(daily_count, close=daily_close)

    return provider


_PATCH_DYNAMIC = patch(
    "trader.us.universe_builder._load_dynamic_sources",
    return_value={},
)


# ---------------------------------------------------------------------------
# 테스트 1: current_price=None → daily_close fallback
# ---------------------------------------------------------------------------

def test_price_fallback_from_daily_close():
    """QQQ daily close가 있는데 current price가 None이면 daily close로 price fallback되어야 한다."""
    try:
        from trader.us.universe_builder import build_us_dynamic_universe
    except ImportError:
        pytest.skip("universe_builder not available")

    # current price는 반환하지만 last=None
    provider = MagicMock()
    provider.get_current_price.return_value = {"last": None, "symbol": "QQQ"}
    provider.get_daily_prices.return_value = _make_daily(70, close=450.0)

    with _PATCH_DYNAMIC:
        result = build_us_dynamic_universe(
            trade_date="2026-05-29",
            env="practice",
            provider=provider,
            manual_seed=["QQQ"],
            force_rebuild=True,
        )

    # QQQ는 core seed이므로 fallback_seed_price_only 또는 relaxed/strict 로 통과해야 함
    passed_symbols = [s["symbol"] for s in result.get("symbols", [])]
    assert "QQQ" in passed_symbols, (
        f"QQQ should pass via daily_close fallback, got: {result}"
    )
    # price는 daily close 값이어야 함
    qqq_entry = next(s for s in result["symbols"] if s["symbol"] == "QQQ")
    assert abs(qqq_entry["price"] - 450.0) < 1.0, f"price should be ~450.0, got {qqq_entry['price']}"


# ---------------------------------------------------------------------------
# 테스트 2: history_days=35 → strict 실패, relaxed 통과
# ---------------------------------------------------------------------------

def test_relaxed_filter_35_days():
    """history_days=35이면 strict는 실패하지만 relaxed는 통과해야 한다."""
    try:
        from trader.us.universe_builder import build_us_dynamic_universe
    except ImportError:
        pytest.skip("universe_builder not available")

    # 35일치 데이터 (strict_history_days=60 미만, relaxed_history_days=30 이상)
    provider = _make_provider(current_price=200.0, daily_count=35)

    with _PATCH_DYNAMIC:
        result = build_us_dynamic_universe(
            trade_date="2026-05-29",
            env="practice",
            provider=provider,
            manual_seed=["NVDA"],
            force_rebuild=True,
        )

    passed_symbols = [s["symbol"] for s in result.get("symbols", [])]
    assert "NVDA" in passed_symbols, (
        f"NVDA with 35 days should pass relaxed filter, got: {result}"
    )
    nvda_entry = next(s for s in result["symbols"] if s["symbol"] == "NVDA")
    # relaxed 또는 fallback 모드여야 함
    assert nvda_entry.get("filter_mode") in ("relaxed", "fallback_seed_price_only"), (
        f"Expected relaxed or fallback, got: {nvda_entry.get('filter_mode')}"
    )


# ---------------------------------------------------------------------------
# 테스트 3: manual_seed → price 있고 history 부족해도 fallback 통과
# ---------------------------------------------------------------------------

def test_manual_seed_fallback_insufficient_history():
    """manual_seed 종목은 price가 있고 history가 부족해도 fallback으로 통과 가능해야 한다."""
    try:
        from trader.us.universe_builder import build_us_dynamic_universe
    except ImportError:
        pytest.skip("universe_builder not available")

    # 10일치 데이터 (relaxed_history_days=30 미만 → strict/relaxed 모두 실패)
    # AAPL은 core seed이므로 fallback 허용
    provider = _make_provider(current_price=180.0, daily_count=10)

    with _PATCH_DYNAMIC:
        result = build_us_dynamic_universe(
            trade_date="2026-05-29",
            env="practice",
            provider=provider,
            manual_seed=["AAPL"],
            force_rebuild=True,
        )

    passed_symbols = [s["symbol"] for s in result.get("symbols", [])]
    assert "AAPL" in passed_symbols, (
        f"AAPL (manual_seed/core) should pass via fallback even with 10 days, got: {result}"
    )
    aapl_entry = next(s for s in result["symbols"] if s["symbol"] == "AAPL")
    assert aapl_entry.get("filter_mode") == "fallback_seed_price_only", (
        f"Expected fallback_seed_price_only, got: {aapl_entry.get('filter_mode')}"
    )
    assert aapl_entry.get("warning") == "insufficient_history_but_seed_allowed"


# ---------------------------------------------------------------------------
# 테스트 4: 101개 input 중 mock daily close가 있는 40개는 selected_count >= 30
# ---------------------------------------------------------------------------

def test_selected_count_ge_30_with_40_valid():
    """101개 input 중 mock daily close가 있는 40개는 selected_count >= 30이 되어야 한다."""
    try:
        from trader.us.universe_builder import build_us_dynamic_universe
    except ImportError:
        pytest.skip("universe_builder not available")

    # 40개 유효 종목 (daily 70일, close 100)
    from trader.us.symbols import list_known_symbols
    known_symbols = list_known_symbols()
    valid_symbols = known_symbols[:40]
    # 61개 실패 종목 (daily 0일, price None)
    invalid_symbols = known_symbols[40:101]
    all_symbols = valid_symbols + invalid_symbols

    def _side_effect_daily(symbol, exchange, **kwargs):
        if symbol in valid_symbols:
            return _make_daily(70, close=50.0)
        return []  # 데이터 없음

    def _side_effect_price(symbol, exchange):
        if symbol in valid_symbols:
            return {"last": "50.0", "symbol": symbol}
        return {"last": None, "symbol": symbol}

    provider = MagicMock()
    provider.get_daily_prices.side_effect = _side_effect_daily
    provider.get_current_price.side_effect = _side_effect_price

    dynamic_mock = {
        "test_category": invalid_symbols,
    }

    with patch("trader.us.universe_builder._load_dynamic_sources", return_value=dynamic_mock):
        result = build_us_dynamic_universe(
            trade_date="2026-05-29",
            env="practice",
            provider=provider,
            manual_seed=valid_symbols,
            force_rebuild=True,
        )

    assert result["filtered_count"] >= 30, (
        f"Expected >= 30, got filtered_count={result['filtered_count']}, "
        f"status={result['status']}, errors={result['errors']}"
    )
    assert result["status"] != "ERROR", (
        f"Status should not be ERROR: {result['status']}, errors={result['errors']}"
    )


# ---------------------------------------------------------------------------
# 테스트 5: normalize_daily_row - KIS raw 필드 변환
# ---------------------------------------------------------------------------

def test_normalize_daily_row_kis_fields():
    """KIS raw 필드가 표준 OHLCV로 변환되어야 한다."""
    try:
        from trader.us.data_provider import normalize_daily_row
    except ImportError:
        pytest.skip("normalize_daily_row not available")

    # KIS 해외지수 필드 스타일
    raw = {
        "xymd": "20260529",
        "ovrs_nmix_prpr": "450.25",  # close
        "ovrs_nmix_oprc": "448.00",  # open
        "ovrs_nmix_hgpr": "451.00",  # high
        "ovrs_nmix_lwpr": "447.50",  # low
        "acml_vol": "1,234,567",     # volume (콤마 포함)
    }
    row = normalize_daily_row(raw)
    assert row["close"] == pytest.approx(450.25)
    assert row["open"] == pytest.approx(448.00)
    assert row["high"] == pytest.approx(451.00)
    assert row["low"] == pytest.approx(447.50)
    assert row["volume"] == 1234567


def test_normalize_daily_row_stck_fields():
    """KIS price/date fields convert, but stck_sdpr must not be treated as volume."""
    try:
        from trader.us.data_provider import normalize_daily_row
    except ImportError:
        pytest.skip("normalize_daily_row not available")

    raw = {
        "stck_bsop_date": "20260529",
        "stck_clpr": "180.50",
        "stck_oprc": "179.00",
        "stck_hgpr": "181.00",
        "stck_lwpr": "178.50",
        "stck_sdpr": "999,999",
    }
    row = normalize_daily_row(raw)
    assert row["close"] == pytest.approx(180.50)
    assert row["volume"] == 0


# ---------------------------------------------------------------------------
# 테스트 6: FILTER 로그에 strict/relaxed/fallback 카운트 포함 (smoke)
# ---------------------------------------------------------------------------

def test_filter_log_strict_relaxed_fallback(caplog):
    """필터 결과 로그에 STRICT/RELAXED/FALLBACK 카운트가 남아야 한다."""
    try:
        from trader.us.universe_builder import build_us_dynamic_universe
    except ImportError:
        pytest.skip("universe_builder not available")

    import logging
    provider = _make_provider(current_price=100.0, daily_count=70)

    with caplog.at_level(logging.INFO, logger="trader.us.universe_builder"):
        with _PATCH_DYNAMIC:
            build_us_dynamic_universe(
                trade_date="2026-05-29",
                env="practice",
                provider=provider,
                manual_seed=["SPY"],
                force_rebuild=True,
            )

    log_text = caplog.text
    assert "[US_UNIVERSE_BUILDER][FILTER][STRICT]" in log_text
    assert "[US_UNIVERSE_BUILDER][FILTER][RELAXED]" in log_text
    assert "[US_UNIVERSE_BUILDER][FILTER][FALLBACK]" in log_text


def test_volume_missing_provider_fallback_avoids_hard_fail():
    """260 bars with close history but zero provider volume should pass >=30 as warning fallback."""
    from trader.us.universe_builder import build_us_dynamic_universe

    from trader.us.symbols import list_known_symbols
    symbols = list_known_symbols()[:50]
    provider = MagicMock()
    provider.get_current_price.return_value = {"last": "120"}
    provider.get_daily_prices.return_value = _make_daily(260, close=120.0, volume=0)

    with _PATCH_DYNAMIC:
        result = build_us_dynamic_universe(
            trade_date="2026-05-29",
            env="practice",
            provider=provider,
            manual_seed=symbols,
            force_rebuild=True,
        )

    assert result["status"] == "OK_WITH_WARNINGS"
    assert result["filtered_count"] >= 30
    assert result["volume_missing_fallback_used"] is True
    assert result["filter_counts"].get("failed_volume", 0) == 0
    assert all(s["filter_mode"] == "fallback_price_history_only" for s in result["symbols"])


def test_regression_filtered_20_hard_fail_becomes_warning_with_volume_fallback_candidates():
    """Regression: previous 20 strict pass + many zero-volume history candidates should not ERROR."""
    from trader.us.universe_builder import build_us_dynamic_universe

    from trader.us.symbols import list_known_symbols
    symbols = list_known_symbols()[:50]
    symbol_index = {symbol: idx for idx, symbol in enumerate(symbols)}
    provider = MagicMock()
    provider.get_current_price.return_value = {"last": "120"}

    def daily_for(symbol, exchange, as_of_date=None):
        idx = symbol_index[symbol]
        return _make_daily(260, close=120.0, volume=(2_000_000 if idx < 20 else 0))

    provider.get_daily_prices.side_effect = daily_for
    with _PATCH_DYNAMIC:
        result = build_us_dynamic_universe(
            trade_date="2026-05-29",
            env="practice",
            provider=provider,
            manual_seed=symbols,
            force_rebuild=True,
        )

    assert result["status"] == "OK_WITH_WARNINGS"
    assert result["filtered_count"] >= 30
    assert result["volume_missing_fallback_count"] >= 30


def test_value_or_price_fields_do_not_disable_volume_missing_fallback():
    """Value/price-only pseudo-volume fields must not make volume fallback disappear."""
    from trader.us.data_provider import normalize_daily_rows
    from trader.us.universe_builder import build_us_dynamic_universe

    from trader.us.symbols import list_known_symbols
    symbols = list_known_symbols()[:50]
    provider = MagicMock()
    provider.get_current_price.return_value = {"last": "120"}
    raw_daily = []
    from datetime import date, timedelta
    end = date(2026, 5, 29)
    for i in range(260):
        d = end - timedelta(days=260 - i - 1)
        raw_daily.append({
            "xymd": d.strftime("%Y%m%d"),
            "clos": "120", "high": "121", "low": "119", "open": "120",
            "acml_tr_pbmn": "999,999,999",
            "stck_sdpr": "100",
        })
    provider.get_daily_prices.return_value = normalize_daily_rows("V000", raw_daily)

    with _PATCH_DYNAMIC:
        result = build_us_dynamic_universe(
            trade_date="2026-05-29", env="practice", provider=provider,
            manual_seed=symbols, force_rebuild=True,
        )

    assert result["volume_missing_fallback_used"] is True
    assert result["volume_missing_fallback_count"] >= 30
    assert result["status"] == "OK_WITH_WARNINGS"
