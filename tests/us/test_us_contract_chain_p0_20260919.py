from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest


def _simulate_postgres_watchlist_row():
    from trader.us.db.repos import _merge_us_daily_metrics_meta

    prep_row = {
        "symbol": "AAPL",
        "exchange": "NASDAQ",
        "score": 0.92,
        "score_final": 0.92,
        "entry_style_selected": "pb1_pullback",
        "entry_style_raw": "pb1_pullback",
        "pullback_score": 0.81,
        "breakout_score": 0.12,
        "momentum_score": 0.43,
        "reasons": ["ENTRY_PULLBACK"],
        "filters_passed": ["score", "risk"],
        "score_breakdown": {"pullback": 0.81},
        "explanation_quality": "FULL",
        "rank_final30": 1,
        "theme_cluster": "TECH",
        "market_regime": "GROWTH_LEADERSHIP",
        "meta": {},
    }
    meta = _merge_us_daily_metrics_meta(prep_row)
    # This is the actual PostgreSQL load shape: provenance lives in meta JSON,
    # not as dedicated us_watchlist columns.
    return {
        "symbol": "AAPL",
        "exchange": "NASDAQ",
        "strategy": "pb1_pullback",
        "score": 0.92,
        "meta": meta,
        "prep_status": "OK",
        "run_id": "p0-contract-chain",
        "data_source": "kis",
    }


def test_us_final30_db_reload_preserves_provenance_and_creates_live_buy(monkeypatch):
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

    db_row = _simulate_postgres_watchlist_row()
    canonical = canonicalize_us_watchlist_row(db_row)

    assert canonical["entry_style_selected"] == "pb1_pullback"
    assert canonical["entry_style_raw"] == "pb1_pullback"
    assert canonical["pullback_score"] == 0.81
    assert canonical["reasons"] == ["ENTRY_PULLBACK"]
    assert validate_us_entry_provenance_contract([db_row])["ok"] is True

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
        now=datetime(2026, 9, 18, 10, 5, tzinfo=ZoneInfo("America/New_York")),
        max_new_entries=1,
        watchlist_entries=[canonical],
        current_position_symbols=set(),
        diagnostics=diagnostics,
    )

    assert len(intents) == 1
    assert intents[0]["symbol"] == "AAPL"
    assert intents[0]["entry_style_selected"] == "ENTRY_PULLBACK"
    assert not [
        item for item in diagnostics.get("blocked", [])
        if item.get("reason") == "ENTRY_EXPLAIN_CONTRACT_ERROR"
    ]

    routed = route_order(
        intents[0],
        signal_only=True,
        current_position_symbols=set(),
        allowed_symbols={"AAPL"},
    )
    assert routed["status"] == "SIGNAL_ONLY"
    contract = routed["intent"]["meta"]["entry_exit_contract"]
    assert contract["entry_provenance"]["entry_style_selected"] == "ENTRY_PULLBACK"
    assert routed["intent"]["meta"]["entry_exit_contract_sha256"] == contract["sha256"]


def test_us_momentum_pullback_db_to_buy_contract_preserves_raw_subtype(monkeypatch):
    from trader.us.db import repos
    from trader.us.score_columns import canonicalize_us_watchlist_row, validate_us_entry_provenance_contract
    from trader.us.pb1.us_entry_engine import generate_entry_intents
    from trader.us.execution.order_router import route_order

    _entry_test_env(monkeypatch)
    db_row = _simulate_postgres_watchlist_row()
    db_row["meta"]["entry_style_selected"] = "momentum_pullback"
    db_row["meta"]["entry_style_raw"] = "momentum_pullback"
    db_row["meta"]["momentum_score"] = 0.82
    db_row["meta"]["pullback_score"] = 0.78

    assert validate_us_entry_provenance_contract([db_row])["ok"] is True
    canonical = canonicalize_us_watchlist_row(db_row)
    assert canonical["entry_style_selected"] == "momentum_pullback"
    assert canonical["entry_style_raw"] == "momentum_pullback"

    diagnostics = {}
    intents = generate_entry_intents(
        tickers=None,
        provider=_EntryProvider(),
        sold_today=set(),
        available_cash_usd=10000.0,
        position_count=0,
        capital_usd_cap=10000.0,
        now=datetime(2026, 9, 18, 10, 5, tzinfo=ZoneInfo("America/New_York")),
        max_new_entries=1,
        watchlist_entries=[canonical],
        current_position_symbols=set(),
        diagnostics=diagnostics,
    )
    assert len(intents) == 1
    assert intents[0]["entry_style_raw"] == "momentum_pullback"
    assert intents[0]["entry_style_selected"] == "ENTRY_PULLBACK"
    assert intents[0]["entry_signal_type"] == "pullback"

    routed = route_order(
        intents[0],
        signal_only=True,
        current_position_symbols=set(),
        allowed_symbols={"AAPL"},
    )
    assert routed["status"] == "SIGNAL_ONLY"
    contract = routed["intent"]["meta"]["entry_exit_contract"]
    assert contract["entry_provenance"]["entry_style_raw"] == "momentum_pullback"
    assert contract["entry_provenance"]["entry_style_selected"] == "ENTRY_PULLBACK"
    assert contract["strategy_owner"] == "US_STANDARD"


def test_us_live_provenance_validator_rejects_db_row_without_style():
    from trader.us.score_columns import validate_us_entry_provenance_contract

    broken = _simulate_postgres_watchlist_row()
    broken["meta"].pop("entry_style_selected", None)
    result = validate_us_entry_provenance_contract([broken])
    assert result["ok"] is False
    assert result["invalid_count"] == 1
    assert result["invalid"][0]["symbol"] == "AAPL"


def test_us_persisted_position_exposure_never_collapses_to_zero():
    from trader.us.portfolio_cluster_guard import resolve_position_market_value_usd

    persisted = {"symbol": "MSFT", "qty": 10, "current_px": 500.0, "avg_cost": 490.0}
    assert resolve_position_market_value_usd(persisted) == 5000.0

    cost_only = {"symbol": "MSFT", "qty": 10, "avg_cost": 490.0}
    assert resolve_position_market_value_usd(cost_only) == 4900.0


def test_us_health_and_preflight_have_contract_integrity_gates():
    tick = Path("trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    preflight = Path("scripts/wsl/check-us-prep-before-am.sh").read_text(encoding="utf-8")

    assert "[US_ENTRY][CONTRACT_INTEGRITY_FAIL]" in tick
    assert 'entry_degraded_reason = "entry_contract_integrity_fail"' in tick
    assert "validate_us_entry_provenance_contract" in preflight
    assert "DB_LIVE_CONTRACT_FAIL" in preflight


def test_us_intentional_balance_interval_skip_is_not_a_failure_marker():
    tick = Path("trader/us/runner/trade_tick_runner.py").read_text(encoding="utf-8")
    assert 'intentional_balance_skip = str(recon.get("status") or "").upper() == "SKIPPED_BALANCE_RECONCILE"' in tick
    assert "balance_fetch_failed = bool(\n        not intentional_balance_skip" in tick


@pytest.mark.skipif(not __import__("os").getenv("PBCORE_TEST_POSTGRES_URL"), reason="real PostgreSQL integration URL not configured")
def test_us_continuous_postgres_prep_to_sell_uses_one_original_contract(monkeypatch):
    """One continuous production-shaped chain from PREP DB row to SELL.

    The test never injects an entry_exit_contract into fill or position data.
    The contract is created once from the DB-reloaded PREP decision at BUY route
    time. Every later stage must recover/reference that same immutable contract
    by its root SHA and canonical payload, even after current ENV is changed.
    """
    import json
    import os
    from sqlalchemy import create_engine, text

    import trader.us.db.repos as repos
    from trader.us.pb1.us_entry_engine import generate_entry_intents
    from trader.us.execution.order_router import route_order
    from trader.us.pb1.us_exit_engine import generate_exit_intents
    from trader.us.entry_exit_contract import verify_us_entry_exit_contract

    admin_url = os.environ["PBCORE_TEST_POSTGRES_URL"]
    schema_name = "us_contract_chain_e2e_20260919"
    admin = create_engine(admin_url, future=True)
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {schema_name}"))
    admin.dispose()

    engine = create_engine(
        admin_url,
        future=True,
        connect_args={"options": f"-csearch_path={schema_name},public"},
    )
    try:
        with engine.begin() as conn:
            for migration in (
                "migrations/0038_us_agent_tables.sql",
                "migrations/0039_us_prep_locked_watchlist_contract.sql",
                "migrations/0043_us_fills_idempotency_and_order_reconcile_fix.sql",
                "migrations/0046_us_orders_committed_notional.sql",
                "migrations/0047_us_order_events_profit_lifecycle.sql",
            ):
                conn.exec_driver_sql(open(migration, encoding="utf-8").read())

        monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
        monkeypatch.setenv("KIS_ENV", "practice")
        monkeypatch.setenv("US_MIN_ENTRY_SCORE", "0.01")
        monkeypatch.setenv("US_MAX_NEW_ENTRIES_PER_TICK", "1")
        monkeypatch.setenv("US_MAX_ORDER_USD", "5000")
        monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "1")
        monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
        monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "50000")
        monkeypatch.setenv("US_HARD_STOP_PCT", "0.08")

        # 1) PREP decision -> real PostgreSQL locked watchlist.
        prep_row = {
            "symbol": "AAPL",
            "exchange": "NASDAQ",
            "strategy": "us_pb1",
            "score": 0.92,
            "score_final": 0.92,
            "entry_style_selected": "pb1_pullback",
            "entry_reason": "ENTRY_PULLBACK",
            "pullback_score": 0.81,
            "breakout_score": 0.12,
            "momentum_score": 0.43,
            "reasons": ["ENTRY_PULLBACK", "ma_alignment"],
            "filters_passed": ["score", "risk"],
            "score_breakdown": {"pullback": 0.81},
            "explanation_quality": "FULL",
            "rank_final30": 1,
            "theme_cluster": "TECH",
            "market_regime": "GROWTH_LEADERSHIP",
            "meta": {"data_source": "kis"},
        }
        saved = repos.clear_and_save_locked_us_watchlist(
            [prep_row], "2026-09-18", "prep-run-contract-chain", "OK"
        )
        assert saved["saved_count"] == 1

        # 2) New DB read boundary -> live canonical row -> actual PB1 BUY intent.
        live_rows = repos.load_locked_us_watchlist(
            trade_date="2026-09-18", min_count=1, allow_degraded=True
        )
        assert len(live_rows) == 1
        live_row = live_rows[0]
        assert live_row["entry_style_selected"] == "pb1_pullback"
        assert live_row["entry_reason"] == "ENTRY_PULLBACK"
        assert live_row["reasons"][0] == "ENTRY_PULLBACK"

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
            now=datetime(2026, 9, 18, 10, 5, tzinfo=ZoneInfo("America/New_York")),
            max_new_entries=1,
            watchlist_entries=live_rows,
            current_position_symbols=set(),
            diagnostics=diagnostics,
        )
        assert len(intents) == 1
        assert not [
            item for item in diagnostics.get("blocked", [])
            if item.get("reason") == "ENTRY_EXPLAIN_CONTRACT_ERROR"
        ]

        # 3) BUY route creates the original immutable contract exactly once.
        routed = route_order(
            intents[0],
            signal_only=True,
            current_position_symbols=set(),
            allowed_symbols={"AAPL"},
        )
        assert routed["status"] == "SIGNAL_ONLY"
        routed_intent = routed["intent"]
        original_contract = routed_intent["meta"]["entry_exit_contract"]
        original_sha = original_contract["sha256"]
        assert verify_us_entry_exit_contract(original_contract)
        assert original_contract["entry_provenance"]["entry_reason"] == "ENTRY_PULLBACK"
        assert original_contract["entry_provenance"]["entry_style_selected"] == "ENTRY_PULLBACK"
        assert original_contract["management"]["swing"]["hard_stop"] == pytest.approx(0.08)
        original_canonical = json.dumps(
            original_contract, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )

        # Change today's policy AFTER the original contract exists. Any later
        # rebuild/re-derivation would change the contract and fail below.
        monkeypatch.setenv("US_HARD_STOP_PCT", "0.20")
        monkeypatch.setenv("US_TP1_PCT", "0.50")

        # 4) Persist BUY intent and ACK. The persisted contract must be byte-
        # equivalent canonically to the original contract, not reconstructed.
        assert repos.save_order_intent(routed_intent, trade_date="2026-09-18")
        with engine.connect() as conn:
            persisted_intent_meta = conn.execute(
                text("SELECT meta FROM us_order_intents WHERE client_order_key=:key"),
                {"key": routed_intent["client_order_key"]},
            ).scalar_one()
        assert persisted_intent_meta["entry_exit_contract_sha256"] == original_sha
        assert json.dumps(
            persisted_intent_meta["entry_exit_contract"],
            sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ) == original_canonical

        assert repos.save_order_ack({
            **routed_intent,
            "qty_requested": int(routed_intent["qty"]),
            "qty_filled": 0,
            "order_no": "AAPL-CONTINUOUS-E2E",
            "status": "ACK",
            "env": "practice",
            "meta": persisted_intent_meta,
        }, trade_date="2026-09-18")
        with engine.connect() as conn:
            order_meta = conn.execute(
                text("SELECT meta FROM us_orders WHERE client_order_key=:key"),
                {"key": routed_intent["client_order_key"]},
            ).scalar_one()
        assert order_meta["entry_exit_contract_sha256"] == original_sha
        assert json.dumps(
            order_meta["entry_exit_contract"],
            sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ) == original_canonical

        # 5) Broker-proven fill intentionally carries NO contract. save_fills
        # must recover the original contract from the persisted BUY order.
        broker_fill = {
            "symbol": "AAPL",
            "exchange": "NASDAQ",
            "side": "BUY",
            "qty": int(routed_intent["qty"]),
            "price_usd": 100.0,
            "order_no": "AAPL-CONTINUOUS-E2E",
            "client_order_key": routed_intent["client_order_key"],
            "filled_at": "2026-09-18T14:05:00+00:00",
            "cumulative_filled_qty": int(routed_intent["qty"]),
            "requested_qty": int(routed_intent["qty"]),
            "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
            "meta": {
                "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                "cumulative_filled_qty": int(routed_intent["qty"]),
                "requested_qty": int(routed_intent["qty"]),
            },
        }
        fill_result = repos.save_fills_with_result(
            [broker_fill], trade_date="2026-09-18"
        )
        assert fill_result["status"] == "OK"
        with engine.connect() as conn:
            fill_meta = conn.execute(
                text("SELECT meta FROM us_fills WHERE client_order_key=:key"),
                {"key": routed_intent["client_order_key"]},
            ).scalar_one()
        assert fill_meta["entry_exit_contract_sha256"] == original_sha
        assert json.dumps(
            fill_meta["entry_exit_contract"],
            sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ) == original_canonical

        # 6) Authoritative broker position also carries NO contract. Position
        # persistence must resolve the same original BUY contract from lineage.
        balance_position = {
            "symbol": "AAPL",
            "exchange": "NASDAQ",
            "qty": int(routed_intent["qty"]),
            "orderable_qty": int(routed_intent["qty"]),
            "avg_price_usd": 100.0,
            "current_price_usd": 91.0,
            "balance_source": "kis_balance_authoritative",
            "meta": {"balance_source": "kis_balance_authoritative"},
        }
        assert repos.save_position_snapshot(
            [balance_position],
            trade_date="2026-09-18",
            balance_fetch_status="OK",
            balance_parse_status="OK",
            authoritative_positions=True,
            preserve_previous_positions=False,
        ) == 1

        with engine.connect() as conn:
            position_meta = conn.execute(
                text("SELECT meta FROM us_positions WHERE symbol='AAPL' AND as_of='2026-09-18'")
            ).scalar_one()
        assert position_meta["entry_exit_contract_sha256"] == original_sha
        assert json.dumps(
            position_meta["entry_exit_contract"],
            sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ) == original_canonical

        # 7) Fresh DB load = restart boundary. SELL must cite the same root SHA
        # and original BUY reason despite today's ENV having changed.
        restarted = repos.load_us_positions_by_symbols(
            ["AAPL"], as_of="2026-09-18"
        )["AAPL"]
        assert restarted["meta"]["entry_exit_contract_sha256"] == original_sha
        assert json.dumps(
            restarted["meta"]["entry_exit_contract"],
            sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ) == original_canonical

        restarted["resolved_current_price"] = 91.0
        restarted["entry_price"] = 100.0
        restarted["max_price"] = 100.0
        sells = generate_exit_intents(
            [restarted],
            prepared_snapshots=[restarted],
            now=datetime.now(timezone.utc),
            include_trend_time=False,
        )
        assert sells and sells[0]["side"] == "SELL"
        assert sells[0]["source_entry_contract_sha256"] == original_sha
        assert sells[0]["meta"]["source_entry_contract_sha256"] == original_sha
        assert sells[0]["source_entry_reason"] == "ENTRY_PULLBACK"
        assert sells[0]["meta"]["hard_stop_threshold_pct"] == pytest.approx(0.08)
    finally:
        engine.dispose()
        cleanup = create_engine(admin_url, future=True)
        with cleanup.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
        cleanup.dispose()


def _entry_test_env(monkeypatch):
    from trader.us.db import repos
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setenv("US_MIN_ENTRY_SCORE", "0.01")
    monkeypatch.setenv("US_MAX_NEW_ENTRIES_PER_TICK", "1")
    monkeypatch.setenv("US_MAX_ORDER_USD", "5000")
    monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "1")
    monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
    monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "50000")
    monkeypatch.setenv("KIS_ENV", "practice")


class _EntryProvider:
    def get_current_price(self, symbol, exchange):
        return {"last": 100.0}


def test_us_corrupted_style_is_rejected_by_preflight_and_live_engine(monkeypatch):
    from trader.us.score_columns import canonicalize_us_watchlist_row, validate_us_entry_provenance_contract
    from trader.us.pb1.us_entry_engine import generate_entry_intents

    _entry_test_env(monkeypatch)
    broken = _simulate_postgres_watchlist_row()
    broken["meta"]["entry_style_selected"] = "CORRUPTED_STYLE"

    preflight = validate_us_entry_provenance_contract([broken])
    assert preflight["ok"] is False
    assert preflight["invalid"][0]["reason"] == "entry_style_invalid_source"

    canonical = canonicalize_us_watchlist_row(broken)
    diagnostics = {}
    intents = generate_entry_intents(
        tickers=None,
        provider=_EntryProvider(),
        sold_today=set(),
        available_cash_usd=10000.0,
        position_count=0,
        capital_usd_cap=10000.0,
        now=datetime(2026, 9, 18, 10, 5, tzinfo=ZoneInfo("America/New_York")),
        max_new_entries=1,
        watchlist_entries=[canonical],
        current_position_symbols=set(),
        diagnostics=diagnostics,
    )
    assert intents == []
    blocked = diagnostics.get("blocked") or []
    assert any(item.get("reason") == "ENTRY_EXPLAIN_CONTRACT_ERROR" for item in blocked)
    assert any(item.get("contract_reason") == "entry_style_invalid_source" for item in blocked)


def test_us_conflicting_raw_subtype_and_canonical_family_fails_closed():
    from trader.us.score_columns import validate_us_entry_provenance_contract

    conflict = _simulate_postgres_watchlist_row()
    conflict["meta"]["entry_style_selected"] = "pb1_pullback"
    conflict["meta"]["entry_style_raw"] = "breakout"

    result = validate_us_entry_provenance_contract([conflict])
    assert result["ok"] is False
    assert result["invalid"][0]["reason"] == "entry_style_conflict"
    assert sorted(result["invalid"][0]["conflicts"]) == ["ENTRY_BREAKOUT", "ENTRY_PULLBACK"]


def test_us_conflicting_top_and_meta_styles_fail_closed_in_preflight_and_live(monkeypatch):
    from trader.us.score_columns import canonicalize_us_watchlist_row, validate_us_entry_provenance_contract
    from trader.us.pb1.us_entry_engine import generate_entry_intents

    _entry_test_env(monkeypatch)
    conflict = _simulate_postgres_watchlist_row()
    conflict["entry_style_selected"] = "ENTRY_BREAKOUT"
    conflict["meta"]["entry_style_selected"] = "ENTRY_PULLBACK"

    preflight = validate_us_entry_provenance_contract([conflict])
    assert preflight["ok"] is False
    assert preflight["invalid"][0]["reason"] == "entry_style_conflict"
    assert sorted(preflight["invalid"][0]["conflicts"]) == ["ENTRY_BREAKOUT", "ENTRY_PULLBACK"]

    canonical = canonicalize_us_watchlist_row(conflict)
    diagnostics = {}
    intents = generate_entry_intents(
        tickers=None,
        provider=_EntryProvider(),
        sold_today=set(),
        available_cash_usd=10000.0,
        position_count=0,
        capital_usd_cap=10000.0,
        now=datetime(2026, 9, 18, 10, 5, tzinfo=ZoneInfo("America/New_York")),
        max_new_entries=1,
        watchlist_entries=[canonical],
        current_position_symbols=set(),
        diagnostics=diagnostics,
    )
    assert intents == []
    blocked = diagnostics.get("blocked") or []
    assert any(item.get("reason") == "ENTRY_EXPLAIN_CONTRACT_ERROR" for item in blocked)
    assert any(item.get("contract_reason") == "entry_style_conflict" for item in blocked)
