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
    # Exercise the production repository API across save/load, rather
    # than constructing a DB-shaped row with the helper being tested.
    repos.reset_memory_stores()
    prep["strategy"] = raw_style
    prep["data_source"] = "completed_daily"
    saved = repos.clear_and_save_locked_us_watchlist(
        entries=[prep],
        trade_date="2026-10-09",
        run_id="four-entry-families-test",
        prep_status="OK",
    )
    assert saved["saved_count"] == 1
    loaded = repos.load_locked_us_watchlist("2026-10-09", min_count=1)
    assert len(loaded) == 1
    db_row = loaded[0]
    assert db_row["run_id"] == "four-entry-families-test"
    assert db_row["meta"]["entry_style_selected"] == raw_style
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


def test_tqqq_cannot_inherit_any_us_standard_four_family_exit_contract():
    from trader.us.entry_exit_contract import (
        build_us_entry_exit_contract,
        verify_us_entry_exit_contract,
    )
    for style in ("ENTRY_PULLBACK", "ENTRY_MOMENTUM", "ENTRY_BREAKOUT", "ENTRY_VCP"):
        standard = build_us_entry_exit_contract({
            "symbol": "AAPL",
            "strategy_owner": "US_STANDARD",
            "entry_style_selected": style,
        })
        assert verify_us_entry_exit_contract(standard)
        # Real restart/reconciliation inputs may already carry a valid
        # standard contract.  The TQQQ owner fence must run *first*.
        for item in (
            {"symbol": "TQQQ", "strategy_owner": "TQQQ_INFINITE"},
            {"symbol": "TQQQ", "strategy_owner": "US_STANDARD"},
            {"symbol": "AAPL", "strategy_owner": "TQQQ_INFINITE"},
        ):
            assert build_us_entry_exit_contract({
                **item,
                "entry_style_selected": style,
                "meta": {"entry_exit_contract": standard},
            }) == {}


@pytest.mark.skipif(
    not __import__("os").getenv("PBCORE_TEST_POSTGRES_URL"),
    reason="real isolated PostgreSQL integration URL not configured",
)
@pytest.mark.parametrize(
    ("style", "entry_reason"),
    [
        ("pb1_pullback", "ENTRY_PULLBACK"),
        ("momentum", "ENTRY_MOMENTUM"),
        ("breakout", "ENTRY_BREAKOUT"),
        ("vcp", "ENTRY_VCP"),
    ],
)
def test_four_family_actual_postgres_locked_save_load_contract(monkeypatch, style, entry_reason):
    """Verify the actual SQL writer AND reader, not a synthetic meta mapping."""
    import os
    from pathlib import Path
    from sqlalchemy import create_engine, text
    from trader.us.db import repos
    from trader.us.entry_exit_contract import (
        build_us_entry_exit_contract,
        verify_us_entry_exit_contract,
    )

    url = os.environ["PBCORE_TEST_POSTGRES_URL"]
    schema = "us_four_family_contract_20261011"
    admin = create_engine(url, future=True)
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    admin.dispose()

    engine = create_engine(
        url, future=True,
        connect_args={"options": f"-csearch_path={schema},public"},
    )
    try:
        with engine.begin() as conn:
            for migration in (
                "migrations/0038_us_agent_tables.sql",
                "migrations/0039_us_prep_locked_watchlist_contract.sql",
            ):
                conn.exec_driver_sql(Path(migration).read_text(encoding="utf-8"))

        monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
        entry = {
            "symbol": "AAPL",
            "exchange": "NASDAQ",
            "strategy": "us_pb1",
            "score": 0.92,
            "score_final": 0.92,
            "entry_style_selected": style,
            "entry_style_raw": style,
            "entry_reason": entry_reason,
            "pullback_score": 0.70,
            "momentum_score": 0.80,
            "breakout_score": 0.75,
            "vcp_score": 0.90,
            "vcp_pass": style == "vcp",
            "trend_template_pass": style == "vcp",
            "pivot_price": 99.0 if style == "vcp" else None,
            "reasons": [entry_reason],
            "filters_passed": ["score", "liquidity"],
            "rank_final30": 1,
        }
        saved = repos.clear_and_save_locked_us_watchlist(
            [entry], "2026-10-09", f"four-families-pg-{style}", "OK"
        )
        assert saved["saved_count"] == 1
        persisted = repos.load_locked_us_watchlist(
            trade_date="2026-10-09", min_count=1,
        )
        assert len(persisted) == 1
        row = persisted[0]
        assert row["entry_style_selected"] == style
        assert row["entry_reason"] == entry_reason
        frozen = build_us_entry_exit_contract(row)
        assert verify_us_entry_exit_contract(frozen)
        assert frozen["strategy_owner"] == "US_STANDARD"
        assert frozen["entry_provenance"]["entry_style_selected"] == style
    finally:
        engine.dispose()
        admin = create_engine(url, future=True)
        with admin.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        admin.dispose()
