"""Four US_STANDARD entry styles must survive locked PREP → BUY → frozen exit contract.

Real strategy qualification stays with PREP; these tests prove that the *live*
execution boundary does not silently turn a valid Momentum/Breakout/VCP into
PB1 Pullback or GENERIC. TQQQ ownership is not changed.
"""
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from trader.us.strategy_ownership import owner_for_symbol


@pytest.mark.parametrize(
    ("raw_style", "expected_style", "expected_signal"),
    [
        ("pb1_pullback", "ENTRY_PULLBACK", "pullback"),
        ("momentum", "ENTRY_MOMENTUM", "momentum"),
        ("breakout", "ENTRY_BREAKOUT", "breakout"),
        ("vcp", "ENTRY_VCP", "vcp"),
    ],
)
def test_standard_style_prep_db_live_buy_frozen_contract(monkeypatch, raw_style, expected_style, expected_signal):
    from trader.us.db import repos
    from trader.us.score_columns import (
        canonicalize_us_watchlist_row,
        validate_us_entry_provenance_contract,
    )
    from trader.us.pb1.us_entry_engine import generate_entry_intents
    from trader.us.execution.order_router import route_order

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setenv("US_MIN_ENTRY_SCORE", "0.01")
    monkeypatch.setenv("US_MAX_NEW_ENTRIES_PER_TICK", "1")
    monkeypatch.setenv("US_MAX_ORDER_USD", "5000")
    monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "1")
    monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
    monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "50000")
    monkeypatch.setenv("KIS_ENV", "practice")

    prep = {
        "symbol": "AAPL",
        "exchange": "NASDAQ",
        "score": .92,
        "score_final": .92,
        "entry_style_selected": raw_style,
        "entry_style_raw": raw_style,
        "pullback_score": .55,
        "momentum_score": .80,
        "breakout_score": .75,
        "vcp_score": .92,
        "vcp_pass": raw_style == "vcp",
        "trend_template_pass": raw_style == "vcp",
        "pivot_price": 99.0 if raw_style == "vcp" else None,
        "reasons": [expected_style],
        "filters_passed": ["score", "liquidity"],
        "score_breakdown": {"momentum": .80},
        "rank_final30": 1,
        "theme_cluster": "TECH",
    }
    from trader.us.db.repos import _merge_us_daily_metrics_meta
    db_row = {
        "symbol": "AAPL", "exchange": "NASDAQ",
        "strategy": raw_style, "score": .92,
        "meta": _merge_us_daily_metrics_meta(prep),
        "prep_status": "OK", "run_id": "four-entry-families-test",
        "data_source": "completed_daily",
    }
    preflight = validate_us_entry_provenance_contract([db_row])
    assert preflight["ok"] is True, preflight

    canonical = canonicalize_us_watchlist_row(db_row)
    assert canonical["entry_style_selected"] == raw_style

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
        watchlist_entries=[canonical],
        current_position_symbols=set(),
        diagnostics=diagnostics,
    )
    assert len(intents) == 1, diagnostics
    intent = intents[0]
    assert intent["strategy"] == "us_pb1"  # shared safe US_STANDARD broker executor
    assert intent["entry_style_selected"] == expected_style
    assert intent["entry_signal_type"] == expected_signal
    assert intent["meta"]["entry_style_selected"] == expected_style

    routed = route_order(
        intent,
        signal_only=True,
        current_position_symbols=set(),
        allowed_symbols={"AAPL"},
    )
    assert routed["status"] == "SIGNAL_ONLY"
    contract = routed["intent"]["meta"]["entry_exit_contract"]
    assert contract["strategy_owner"] == "US_STANDARD"
    assert contract["entry_provenance"]["entry_style_selected"] == expected_style
    assert contract["entry_provenance"]["entry_style_raw"] == raw_style
    assert routed["intent"]["meta"]["entry_exit_contract_sha256"] == contract["sha256"]


def test_tqqq_still_belongs_to_its_own_infinite_sleeve():
    assert owner_for_symbol("TQQQ") == "TQQQ_INFINITE"
    assert owner_for_symbol("AAPL") == "US_STANDARD"


@pytest.mark.parametrize(
    ("style", "entry_reason"),
    [
        ("pb1_pullback", "ENTRY_PULLBACK"),
        ("momentum", "ENTRY_MOMENTUM"),
        ("breakout", "ENTRY_BREAKOUT"),
        ("vcp", "ENTRY_VCP"),
    ],
)
def test_each_family_restarts_with_its_original_frozen_stop_and_tp(
    monkeypatch, style, entry_reason,
):
    """Frozen BUY policy wins even if today's env and PREP change on restart."""
    from trader.us.entry_exit_contract import (
        build_us_entry_exit_contract, contract_profit_capture,
        verify_us_entry_exit_contract,
    )
    from trader.us.pb1.us_exit_engine import evaluate_exit

    monkeypatch.setenv("US_HARD_STOP_PCT", "0.08")
    monkeypatch.setenv("US_TP1_PCT", "0.03")
    monkeypatch.setenv("US_TP2_PCT", "0.05")
    monkeypatch.setenv("US_TP3_PCT", "0.08")

    frozen = build_us_entry_exit_contract({
        "symbol": "AAPL", "strategy_owner": "US_STANDARD",
        "entry_style_selected": entry_reason, "entry_reason": entry_reason,
        "entry_style_raw": style,
    })
    assert verify_us_entry_exit_contract(frozen)
    frozen_sha = frozen["sha256"]
    original_tp = contract_profit_capture({"meta": {"entry_exit_contract": frozen}})
    assert [round(stage["threshold_fraction"], 4) for stage in original_tp["stages"]] == [
        .03, .05, .08,
    ]

    # New session/changed global env must not overwrite the original
    # contract for any strategy owner under US_STANDARD.
    monkeypatch.setenv("US_HARD_STOP_PCT", "0.25")
    monkeypatch.setenv("US_TP1_PCT", "0.20")
    monkeypatch.setenv("US_TP2_PCT", "0.30")
    monkeypatch.setenv("US_TP3_PCT", "0.40")

    position = {
        "symbol": "AAPL", "exchange": "NASDAQ",
        "qty": 5, "orderable_qty": 5, "entry_price": 100,
        "max_price": 100,
        "meta": {"entry_exit_contract": frozen, "entry_exit_contract_sha256": frozen_sha},
    }
    signal = evaluate_exit(position=position, current_price=91.0)
    assert signal is not None
    assert signal["exit_type"] == "hard_stop_loss"
    assert signal["qty"] == 5
    assert frozen["sha256"] == frozen_sha
    restart_tp = contract_profit_capture(position)
    assert restart_tp == original_tp
