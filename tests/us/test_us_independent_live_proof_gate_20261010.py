"""Prove that independent strategies never reach BUY on forged/missing proof."""
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from trader.us.pb1.us_entry_engine import _validate_us_independent_new_buy_proof


@pytest.mark.parametrize(
    ("style", "row", "good"),
    [
        ("ENTRY_MOMENTUM", {"independent_entry_contract_v1": True,
                            "entry_signal_proof_source": "completed_daily_ohlcv",
                            "momentum_pass": True, "standalone_momentum_score": .8}, True),
        ("ENTRY_MOMENTUM", {"independent_entry_contract_v1": True,
                            "entry_signal_proof_source": "completed_daily_ohlcv",
                            "momentum_pass": False, "standalone_momentum_score": .8}, False),
        ("ENTRY_MOMENTUM", {"momentum_pass": True, "standalone_momentum_score": .8}, False),
        ("ENTRY_BREAKOUT", {"independent_entry_contract_v1": True,
                            "entry_signal_proof_source": "completed_daily_ohlcv",
                            "breakout_pass": True, "breakout_pivot_price": 100}, True),
        ("ENTRY_BREAKOUT", {"independent_entry_contract_v1": True,
                            "entry_signal_proof_source": "completed_daily_ohlcv",
                            "breakout_pass": False, "breakout_pivot_price": 100}, False),
        ("ENTRY_BREAKOUT", {"independent_entry_contract_v1": True,
                            "entry_signal_proof_source": "completed_daily_ohlcv",
                            "breakout_pass": True}, False),
        ("ENTRY_VCP", {"vcp_pass": True, "trend_template_pass": True,
                       "pivot_price": 100, "vcp_evidence_source": "completed_daily_ohlcv"}, True),
        ("ENTRY_VCP", {"vcp_pass": True, "trend_template_pass": False,
                       "pivot_price": 100, "vcp_evidence_source": "completed_daily_ohlcv"}, False),
        ("ENTRY_VCP", {"vcp_pass": True, "trend_template_pass": True,
                       "pivot_price": 0, "vcp_evidence_source": "completed_daily_ohlcv"}, False),
    ],
)
def test_explicit_proof_required_per_family(monkeypatch, style, row, good):
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "1")
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    ok, reason = _validate_us_independent_new_buy_proof(row, style)
    assert ok is good, reason
    if not good:
        assert reason


def test_legacy_momentum_unaffected_when_feature_off(monkeypatch):
    monkeypatch.setenv("US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED", "0")
    assert _validate_us_independent_new_buy_proof(
        {"entry_style_selected": "momentum"},
        "ENTRY_MOMENTUM",
    ) == (True, "")


def test_verified_vcp_never_requires_pb1_pullback_conditions(monkeypatch):
    monkeypatch.setenv("US_MINERVINI_VCP_PROOF_ENABLED", "1")
    result = _validate_us_independent_new_buy_proof({
        "vcp_pass": True, "trend_template_pass": True,
        "pivot_price": 100, "vcp_evidence_source": "completed_daily_ohlcv",
        "pullback_score": 0,
    }, "ENTRY_VCP")
    assert result == (True, "")


@pytest.mark.parametrize(
    ("style", "proof"),
    [
        ("momentum", {
            "independent_entry_contract_v1": True,
            "momentum_pass": False, "standalone_momentum_score": .91,
            "entry_signal_proof_source": "completed_daily_ohlcv",
        }),
        ("breakout", {
            "independent_entry_contract_v1": True,
            "breakout_pass": False, "breakout_pivot_price": 99,
            "entry_signal_proof_source": "completed_daily_ohlcv",
        }),
        ("vcp", {
            "vcp_pass": False, "trend_template_pass": True,
            "pivot_price": 99, "vcp_evidence_source": "completed_daily_ohlcv",
        }),
    ],
)
def test_unproven_independent_style_is_blocked_before_order_intent(monkeypatch, style, proof):
    from trader.us.db import repos
    from trader.us.db.repos import _merge_us_daily_metrics_meta
    from trader.us.score_columns import canonicalize_us_watchlist_row
    from trader.us.pb1.us_entry_engine import generate_entry_intents

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    for key, value in {
        "US_INDEPENDENT_MOMENTUM_BREAKOUT_ENABLED": "1",
        "US_MINERVINI_VCP_PROOF_ENABLED": "1",
        "US_MIN_ENTRY_SCORE": ".01",
        "US_MAX_NEW_ENTRIES_PER_TICK": "1",
        "US_MAX_ORDER_USD": "5000",
        "US_MAX_POSITION_WEIGHT": "1",
        "US_MIN_CASH_BUFFER_USD": "0",
        "US_MAX_DAILY_NOTIONAL_USD": "50000",
        "KIS_ENV": "practice",
    }.items():
        monkeypatch.setenv(key, value)

    raw = {
        "symbol": "AAPL", "exchange": "NASDAQ",
        "entry_style_selected": style, "entry_style_raw": style,
        "score_final": .90, "score": .90,
        "momentum_score": .9, "breakout_score": .9,
        "pullback_score": .9, "vcp_score": .9,
        "rank_final30": 1,
        **proof,
    }
    db = {
        "symbol": "AAPL", "exchange": "NASDAQ", "score": .90,
        "strategy": style,
        "meta": _merge_us_daily_metrics_meta(raw),
    }
    class Provider:
        def get_current_price(self, symbol, exchange):
            return {"last": 100.0}

    diagnostics = {}
    intents = generate_entry_intents(
        tickers=None,
        provider=Provider(),
        sold_today=set(),
        available_cash_usd=10000.0,
        position_count=0,
        capital_usd_cap=10000.0,
        now=datetime(2026, 10, 9, 10, 5, tzinfo=ZoneInfo("America/New_York")),
        max_new_entries=1,
        watchlist_entries=[canonicalize_us_watchlist_row(db)],
        current_position_symbols=set(),
        diagnostics=diagnostics,
    )
    assert intents == []
    assert any(
        item.get("reason") == "ENTRY_SETUP_PROOF_INVALID"
        for item in diagnostics.get("blocked", [])
    ), diagnostics
