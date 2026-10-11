"""VCP cannot BUY on WebSocket's tvol=0 or incomplete PREP volume proof.

All tests are offline: fake REST only, no KIS HTTP and no real orders.
"""
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from trader.us.pb1.us_entry_engine import (
    _verify_us_vcp_live_breakout, generate_entry_intents,
)
from trader.us.data_provider import USDataProvider


def _proof():
    return {
        "vcp_pass": True,
        "trend_template_pass": True,
        "pivot_price": 100.0,
        "vcp_daily_avg_volume20": 100000.0,
        "vcp_evidence_source": "completed_daily_ohlcv",
    }


def _quote(**overrides):
    return {
        "last": "101.00", "tvol": "180000", "source": "KIS_LIVE",
        "quality": "fresh", "stale": False, "age_sec": 0.0,
        **overrides,
    }


@pytest.mark.parametrize(
    ("quote_override", "proof_override", "ok", "expected_reason"),
    [
        ({}, {}, True, ""),
        ({"last": "100.20"}, {}, False, "vcp_pivot_volume_or_chase_failed"),
        ({"last": "106.00"}, {}, False, "vcp_pivot_volume_or_chase_failed"),
        ({"tvol": "140000"}, {}, False, "vcp_pivot_volume_or_chase_failed"),
        ({"tvol": "0", "source": "KIS_WEBSOCKET"}, {}, False, "vcp_live_quote_unverified_source"),
        ({"tvol": "0"}, {}, False, "vcp_live_price_volume_missing"),
        ({"age_sec": 30}, {}, False, "vcp_live_quote_age_invalid"),
        ({"quality": "stale", "stale": True}, {}, False, "vcp_live_quote_stale"),
        ({"source": "DB_STALE"}, {}, False, "vcp_live_quote_unverified_source"),
        ({}, {"vcp_daily_avg_volume20": None}, False, "vcp_live_price_volume_missing"),
        ({"last": "nan"}, {}, False, "vcp_live_price_volume_missing"),
    ],
)
def test_minervini_original_trigger_policy_and_fresh_broker_volume(
    quote_override, proof_override, ok, expected_reason,
):
    actual_ok, reason, evidence = _verify_us_vcp_live_breakout(
        {**_proof(), **proof_override}, _quote(**quote_override)
    )
    assert actual_ok is ok
    assert reason == expected_reason
    if ok:
        assert evidence["vol_ok"] is True
        assert evidence["pivot"] == 100
        assert evidence["tvol"] == 180000


def test_provider_returns_no_fake_volume_when_offline():
    p = object.__new__(USDataProvider)
    p._offline = True
    p._stage_cancelled = lambda: False
    p._tick_context = None
    quote = p.get_current_price_with_volume("AAPL", "NASDAQ")
    assert quote["stale"] is True
    assert quote["tvol"] == "0"


def test_provider_reads_rest_volume_without_websocket_placeholder():
    p = object.__new__(USDataProvider)
    p._offline = False
    p._stage_cancelled = lambda: False
    p._tick_context = None

    class Client:
        def get_us_price(self, symbol, exchange):
            assert (symbol, exchange) == ("AAPL", "NASDAQ")
            return {
                "output": {"last": "101", "tvol": "180000"},
                "_quote_quality": "FRESH",
                "_quote_source": "KIS_LIVE",
                "_quote_age_sec": 0.0,
            }

    p._get_client = lambda: Client()
    quote = p.get_current_price_with_volume("AAPL", "NASDAQ")
    assert quote["last"] == "101"
    assert quote["tvol"] == "180000"
    assert quote["source"] == "KIS_LIVE"
    assert quote["stale"] is False


@pytest.mark.parametrize(
    ("response", "expected_new_buy", "expected_http"),
    [
        (_quote(), True, 1),
        (_quote(tvol="0"), False, 1),
        (_quote(last="108.0"), False, 1),
        (_quote(age_sec=50), False, 1),
    ],
)
def test_locked_prep_vcp_requires_verified_rest_volume_before_intent(
    monkeypatch, response, expected_new_buy, expected_http,
):
    from trader.us.db import repos
    from trader.us.db.repos import _merge_us_daily_metrics_meta
    from trader.us.score_columns import canonicalize_us_watchlist_row

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    for key, value in {
        "KIS_ENV": "practice",
        "US_MINERVINI_VCP_PROOF_ENABLED": "1",
        "US_VCP_LIVE_REST_MAX_PER_TICK": "1",
        "US_MIN_ENTRY_SCORE": ".01",
        "US_MAX_NEW_ENTRIES_PER_TICK": "1",
        "US_MAX_ORDER_USD": "5000",
        "US_MAX_POSITION_WEIGHT": "1",
        "US_MIN_CASH_BUFFER_USD": "0",
        "US_MAX_DAILY_NOTIONAL_USD": "50000",
    }.items():
        monkeypatch.setenv(key, value)

    row = {
        "symbol": "AAPL", "exchange": "NASDAQ",
        "entry_style_selected": "vcp", "entry_style_raw": "vcp",
        "score_final": .90, "score": .90, "vcp_score": .95,
        "pullback_score": .30, "momentum_score": .50, "breakout_score": .35,
        "rank_final30": 1, "reasons": ["ENTRY_VCP"],
        "filters_passed": ["volume", "trend"],
        "score_breakdown": {"vcp": .95},
        **_proof(),
    }
    db_row = {
        "symbol": "AAPL", "exchange": "NASDAQ", "score": .90,
        "strategy": "vcp", "meta": _merge_us_daily_metrics_meta(row)
    }

    class Provider:
        calls = 0
        def get_current_price(self, symbol, exchange):
            return {"last": "101"}
        def get_current_price_with_volume(self, symbol, exchange):
            self.calls += 1
            return response

    provider = Provider()
    diagnostics = {}
    intents = generate_entry_intents(
        tickers=None, provider=provider, sold_today=set(),
        available_cash_usd=10000.0, position_count=0,
        capital_usd_cap=10000.0,
        now=datetime(2026, 10, 9, 10, 5, tzinfo=ZoneInfo("America/New_York")),
        max_new_entries=1, watchlist_entries=[canonicalize_us_watchlist_row(db_row)],
        current_position_symbols=set(), diagnostics=diagnostics,
    )
    assert provider.calls == expected_http
    assert bool(intents) is expected_new_buy, diagnostics
    if expected_new_buy:
        assert intents[0]["meta"]["vcp_live_breakout_verified"] is True
        assert intents[0]["meta"]["vcp_live_volume"] == 180000
        assert intents[0]["limit_price"] > 101
    else:
        assert any(
            item["reason"] == "VCP_LIVE_TRIGGER_INVALID"
            for item in diagnostics.get("blocked", [])
        ), diagnostics


def test_rest_budget_zero_fails_closed_without_http(monkeypatch):
    from trader.us.db import repos
    from trader.us.db.repos import _merge_us_daily_metrics_meta
    from trader.us.score_columns import canonicalize_us_watchlist_row
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    monkeypatch.setenv("US_VCP_LIVE_REST_MAX_PER_TICK", "0")
    monkeypatch.setenv("US_MIN_ENTRY_SCORE", "0.01")
    monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "1")
    monkeypatch.setenv("US_MAX_ORDER_USD", "5000")
    raw = {"symbol": "AAPL", "exchange": "NASDAQ", "score": .90,
           "score_final": .90, "entry_style_selected": "vcp",
           "entry_style_raw": "vcp", "vcp_score": .90, **_proof()}
    row = canonicalize_us_watchlist_row({
        "symbol": "AAPL", "exchange": "NASDAQ", "score": .90,
        "meta": _merge_us_daily_metrics_meta(raw),
    })
    class Provider:
        def get_current_price(self, symbol, exchange):
            return {"last": "101"}
        def get_current_price_with_volume(self, symbol, exchange):
            raise AssertionError("HTTP must not be requested after budget exhaustion")
    diagnostic = {}
    assert generate_entry_intents(
        tickers=None, provider=Provider(), sold_today=set(),
        available_cash_usd=10000, position_count=0, capital_usd_cap=10000,
        now=datetime(2026, 10, 9, 10, 5, tzinfo=ZoneInfo("America/New_York")),
        watchlist_entries=[row], current_position_symbols=set(),
        diagnostics=diagnostic,
    ) == []
    assert any(
        x["reason"] == "vcp_live_rest_budget_exhausted"
        for x in diagnostic.get("blocked", [])
    )
