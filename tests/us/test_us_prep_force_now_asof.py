# -*- coding: utf-8 -*-
"""Tests: force_now → as_of_date → KIS dailyprice BYMD propagation.

Verifies that when force_now is supplied to run_prep(), the resulting
as_of_date is correctly derived and threaded all the way through to the
KIS dailyprice BYMD parameter — ensuring historical back-dated price data
is requested rather than today's actual date.
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# 1. resolve_us_dailyprice_bymd unit tests
# ---------------------------------------------------------------------------

def test_resolve_us_dailyprice_bymd_with_date():
    """as_of_date 'YYYY-MM-DD' → 'YYYYMMDD'."""
    from trader.us.execution.kis_us_client import resolve_us_dailyprice_bymd

    assert resolve_us_dailyprice_bymd("2026-05-29") == "20260529"


def test_resolve_us_dailyprice_bymd_compact_format():
    """as_of_date 이미 'YYYYMMDD' 형식이면 그대로 반환."""
    from trader.us.execution.kis_us_client import resolve_us_dailyprice_bymd

    assert resolve_us_dailyprice_bymd("20260529") == "20260529"


def test_resolve_us_dailyprice_bymd_returns_string():
    """반환값은 항상 str 타입."""
    from trader.us.execution.kis_us_client import resolve_us_dailyprice_bymd

    result = resolve_us_dailyprice_bymd("2026-05-29")
    assert isinstance(result, str)
    assert len(result) == 8


def test_dailyprice_bymd_does_not_use_today_when_as_of_date_given():
    """as_of_date=2026-05-29 → BYMD != '20260531' (오늘 날짜와 달라야)."""
    from trader.us.execution.kis_us_client import resolve_us_dailyprice_bymd

    bymd = resolve_us_dailyprice_bymd("2026-05-29")
    assert bymd == "20260529"
    assert bymd != "20260531"  # 오늘이 2026-05-31이더라도 다른 값


def test_resolve_us_dailyprice_bymd_none_returns_today():
    """as_of_date=None이면 오늘 NY 기준 날짜를 8자리로 반환."""
    import re
    from trader.us.execution.kis_us_client import resolve_us_dailyprice_bymd

    result = resolve_us_dailyprice_bymd(None)
    assert re.match(r"^\d{8}$", result), f"expected 8-digit date, got {result!r}"


# ---------------------------------------------------------------------------
# 2. data_provider.get_daily_prices as_of_date 전달 확인
# ---------------------------------------------------------------------------

class _FakeKISClient:
    """KIS 클라이언트 스텁 — 호출 기록 저장."""

    def __init__(self):
        self.calls: list[dict] = []

    def get_us_daily_price(self, symbol: str, exchange: str, count: int = 120, as_of_date: str | None = None):
        self.calls.append({"symbol": symbol, "exchange": exchange, "count": count, "as_of_date": as_of_date})
        return []  # 빈 일봉 리스트 반환


def test_data_provider_passes_as_of_date_to_kis_client(monkeypatch):
    """USDataProvider.get_daily_prices()가 as_of_date를 KIS 클라이언트에 전달."""
    from trader.us.data_provider import USDataProvider

    provider = USDataProvider(offline=False, cache_enabled=False)
    fake_client = _FakeKISClient()
    provider._client = fake_client  # type: ignore[attr-defined]

    # _get_client()가 fake_client를 반환하도록 패치
    monkeypatch.setattr(provider, "_get_client", lambda: fake_client)

    provider.get_daily_prices("NVDA", "NASDAQ", count=5, as_of_date="2026-05-29")

    assert len(fake_client.calls) == 1
    call = fake_client.calls[0]
    assert call["as_of_date"] == "2026-05-29"
    assert call["symbol"] == "NVDA"


# ---------------------------------------------------------------------------
# 3. universe_builder as_of_date 전달 확인
# ---------------------------------------------------------------------------

class _FakeProvider:
    """USDataProvider 스텁 — get_daily_prices 호출 기록."""

    def __init__(self):
        self.calls: list[dict] = []

    def get_daily_prices(self, symbol: str, exchange: str, count: int = 120, as_of_date: str | None = None):
        self.calls.append({"symbol": symbol, "exchange": exchange, "as_of_date": as_of_date})
        # 120개 일봉 스텁
        return [
            {"xymd": f"20260{i:03d}", "clos": "100", "open": "99", "high": "101", "low": "98", "tvol": "1000000"}
            for i in range(1, 121)
        ]

    def get_current_price(self, symbol: str, exchange: str):
        return {"last": "100.0", "symbol": symbol, "exchange": exchange}


def _make_fake_provider() -> _FakeProvider:
    return _FakeProvider()


def test_universe_builder_passes_as_of_date_to_provider(monkeypatch):
    """build_us_dynamic_universe()가 as_of_date를 provider.get_daily_prices에 전달."""
    from trader.us.universe_builder import build_us_dynamic_universe

    fake_provider = _make_fake_provider()

    # save/load 파일 I/O 우회
    monkeypatch.setattr(
        "trader.us.universe_builder._load_dynamic_sources",
        lambda: {},  # 빈 dict → dynamic_sources.values() = []
    )

    result = build_us_dynamic_universe(
        trade_date="2026-05-29",
        as_of_date="2026-05-29",
        env="practice",
        provider=fake_provider,
        manual_seed=["NVDA", "AAPL"],
        force_rebuild=True,
    )

    # 호출 기록 확인
    asof_values = {c["as_of_date"] for c in fake_provider.calls if c["as_of_date"] is not None}
    assert "2026-05-29" in asof_values, (
        f"Expected as_of_date='2026-05-29' in provider calls, got: {fake_provider.calls}"
    )


def test_candidate_pool_builder_passes_as_of_date_to_provider():
    """build_us_candidate_pool()가 as_of_date를 provider.get_daily_prices에 전달."""
    from trader.us.candidate_pool_builder import build_us_candidate_pool

    fake_provider = _make_fake_provider()
    fake_universe = [
        {"symbol": "NVDA", "exchange": "NASDAQ", "avg_volume_20d": 5_000_000, "avg_dollar_volume_20d": 500_000_000, "atr_pct": 2.0},
    ]

    result = build_us_candidate_pool(
        trade_date="2026-05-29",
        as_of_date="2026-05-29",
        env="practice",
        dynamic_universe=fake_universe,
        provider=fake_provider,
        force_rebuild=True,
    )

    asof_values = {c["as_of_date"] for c in fake_provider.calls if c["as_of_date"] is not None}
    assert "2026-05-29" in asof_values, (
        f"Expected as_of_date='2026-05-29' in provider calls, got: {fake_provider.calls}"
    )


def test_watchlist_builder_passes_as_of_date_to_provider():
    """build_us_watchlist()가 as_of_date를 provider.get_daily_prices에 전달."""
    from trader.us.watchlist_builder import build_us_watchlist

    fake_provider = _make_fake_provider()
    fake_pool = [
        {"symbol": "NVDA", "exchange": "NASDAQ", "trend_score": 0.8, "rs_60d_score": 0.7, "near_high_score": 0.9,
         "liquidity_score": 0.9, "atr_pct": 2.0, "avg_volume_20d": 5_000_000, "avg_dollar_volume_20d": 500_000_000,
         "rs_20d": 0.8, "rs_60d": 0.7, "rs_120d": 0.6, "score_candidate": 0.8},
    ]

    build_us_watchlist(
        trade_date="2026-05-29",
        as_of_date="2026-05-29",
        env="practice",
        candidate_pool=fake_pool,
        provider=fake_provider,
        force_rebuild=True,
    )

    asof_values = {c["as_of_date"] for c in fake_provider.calls if c["as_of_date"] is not None}
    assert "2026-05-29" in asof_values, (
        f"Expected as_of_date='2026-05-29' in provider calls, got: {fake_provider.calls}"
    )
