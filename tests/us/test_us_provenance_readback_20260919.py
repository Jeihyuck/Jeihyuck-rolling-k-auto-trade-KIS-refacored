"""Regression for the 2026-09-18 locked-watchlist metadata incident."""
import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from trader.us.score_columns import canonicalize_us_watchlist_row
from trader.us.pb1.us_entry_engine import generate_entry_intents


def incident_rows():
    return json.loads(Path(__file__).with_name("fixtures").joinpath("final30_provenance_20260918.json").read_text())


def db_row(row):
    # PostgreSQL returns selected columns, with provenance inside JSONB meta.
    return {"symbol": row["symbol"], "exchange": row["exchange"],
            "strategy": "us_pb1", "score": row["score_final"], "meta": dict(row)}


@pytest.fixture
def entry_setup(monkeypatch):
    from trader.us.db import repos
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    monkeypatch.setattr(repos, "has_pending_order_for_symbol_side", lambda **kw: False)
    monkeypatch.setattr(repos, "has_position", lambda symbol: False)
    monkeypatch.setattr(repos, "load_today_order_keys", lambda trade_date: set())
    monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
    monkeypatch.setenv("US_MAX_ORDER_USD", "2500")


def generate(rows, diagnostics=None):
    class Provider:
        def get_current_price(self, symbol, exchange):
            return {"last": 100.0}
    return generate_entry_intents(
        None, Provider(), set(), 10000, 0, 10000,
        now=datetime(2026, 9, 18, 10, tzinfo=ZoneInfo("America/New_York")),
        watchlist_entries=rows, current_position_symbols=set(),
        available_new_slots=30, max_new_entries=1, diagnostics=diagnostics,
    )


@pytest.mark.parametrize("row", incident_rows(), ids=lambda row: row["symbol"])
def test_incident_rows_generate_buy_after_db_shape_readback(row, entry_setup):
    intents = generate([canonicalize_us_watchlist_row(db_row(row))])
    assert len(intents) == 1
    assert intents[0]["entry_style_selected"] == "ENTRY_PULLBACK"
    assert intents[0]["meta"]["entry_reason"] == "ENTRY_PULLBACK"


@pytest.mark.parametrize("json_meta", [False, True])
def test_nested_reason_json_restores_style_without_inventing_it(json_meta):
    meta = {"reason_json": json.dumps({"entry_style_selected": "pb1_pullback"})}
    row = {"symbol": "CRM", "score": .65, "meta": json.dumps(meta) if json_meta else meta}
    restored = canonicalize_us_watchlist_row(row)
    assert restored["entry_style_selected"] == "ENTRY_PULLBACK"
    assert canonicalize_us_watchlist_row(restored)["entry_style_selected"] == "ENTRY_PULLBACK"


def test_conflicting_provenance_blocks_buy(entry_setup):
    row = db_row(incident_rows()[0])
    row["entry_style_selected"] = "ENTRY_BREAKOUT"
    diagnostics = {}
    assert generate([row], diagnostics) == []
    assert diagnostics["blocked"][0]["reason"] == "ENTRY_EXPLAIN_CONTRACT_ERROR"


def test_missing_provenance_still_blocks_buy(entry_setup):
    diagnostics = {}
    assert generate([{"symbol": "CRM", "exchange": "NYSE", "score": .65}], diagnostics) == []
    assert diagnostics["blocked"][0]["reason"] == "ENTRY_EXPLAIN_CONTRACT_ERROR"


def test_explicit_skip_is_not_overridden_by_nested_provenance(entry_setup):
    row = db_row(incident_rows()[0])
    row["entry_style_selected"] = "SKIP"
    assert generate([row]) == []


def test_recovered_tick_keeps_session_incident_evidence():
    from trader.us.runner.status_contract import summarize_entry_contract_errors, classify_tick_status
    result = summarize_entry_contract_errors([
        {"entry_contract_error_count": 18, "entry_contract_error_symbols": ["CRM", "AAPL"]},
        {"entry_contract_error_count": 18, "entry_contract_error_symbols": ["CRM"]},
        {"status": "OK"},
    ])
    assert result["entry_contract_error_count"] == 36
    assert result["entry_contract_error_symbols"] == ["AAPL", "CRM"]
    assert classify_tick_status({"status": "OK", **result}) == "warning"
    assert classify_tick_status({"status": "FAILED_DB", **result}) == "fatal"


def test_contract_failure_reports_degraded_and_still_routes_sell(monkeypatch):
    from .test_us_exit_first_routing import _patch_tick_basics
    from trader.us.runner.trade_tick_runner import run_trade_tick
    calls = []
    _patch_tick_basics(monkeypatch, calls)
    monkeypatch.setenv("US_WATCHLIST_LOAD_TIMEOUT_SEC", "10")
    monkeypatch.setattr("trader.us.db.repos.load_locked_us_watchlist", lambda *a, **kw: [db_row(row) for row in incident_rows()])

    class Engine:
        last_entry_diagnostics = {}
        def evaluate_exits(self, *a, **kw):
            return [{"symbol": "BE", "exchange": "NASDAQ", "side": "SELL", "qty": 1,
                     "available_qty": 1, "limit_price": 100, "notional_usd": 100,
                     "client_order_key": "sell-be", "trade_date": "2026-06-05"}]
        def evaluate_entries(self, *a, **kw):
            self.last_entry_diagnostics = {"blocked": [{"symbol": "CRM", "reason": "ENTRY_EXPLAIN_CONTRACT_ERROR", "block_stage": "intent_generation"}]}
            return []

    monkeypatch.setattr("trader.us.runner.trade_tick_runner._get_strategy_engine", lambda **kw: Engine())
    monkeypatch.setattr("trader.us.execution.order_router.route_order", lambda intent, **kw: {"status": "ACK", "side": intent["side"], "symbol": intent["symbol"], "intent": intent})
    result = run_trade_tick(session="am", env="practice", offline=False,
                            force_now="2026-06-05T10:00:00-04:00", kis_order_allowed=False)
    assert result["entry_contract_error_count"] == 1
    assert result["entry_contract_error_symbols"] == ["CRM"]
    assert result["entry_degraded"] == 1
    assert result["entry_error_type"] == "ENTRY_EXPLAIN_CONTRACT_ERROR"
    assert result["status"] != "OK_NO_TRADE"
    assert result["orders_ack"] == 1
    assert result["exit_routed_before_entry"] == 1


def test_close_report_preserves_am_and_afternoon_errors(tmp_path, monkeypatch):
    from trader.us.runner.daily_report_runner import run_daily_report
    monkeypatch.chdir(tmp_path)
    base = tmp_path / "reports/us_daily/2026-09-18"
    base.mkdir(parents=True)
    for session, count in (("am", 540), ("afternoon", 630)):
        (base / f"{session}_summary.json").write_text(json.dumps({
            "entry_contract_error_count": count, "entry_contract_error_symbols": ["CRM"],
        }))
    result = run_daily_report(env="practice", session="close", trade_date="2026-09-18", offline=True)
    saved = json.loads((base / "close/us_daily_report.json").read_text())
    assert saved["entry_contract_error_count"] == 1170
    assert saved["entry_contract_error_symbols"] == ["CRM"]
    assert "ENTRY_EXPLAIN_CONTRACT_ERROR" in saved["warnings"]
    assert saved["status"] != "OK"
    assert "1170" in (base / "close/us_daily_report.md").read_text()


@pytest.mark.skipif(not __import__("os").getenv("PBCORE_TEST_POSTGRES_URL"), reason="real PostgreSQL integration URL not configured")
def test_postgres_final30_to_buy_fill_restart_sell(monkeypatch):
    import os
    from sqlalchemy import create_engine, text
    import trader.us.db.repos as repos
    from trader.us.pb1.us_exit_engine import generate_exit_intents

    from uuid import uuid4
    from trader.us.pb1.us_entry_engine import generate_entry_intents
    schema = "provenance_" + uuid4().hex
    admin = create_engine(os.environ["PBCORE_TEST_POSTGRES_URL"], future=True)
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(os.environ["PBCORE_TEST_POSTGRES_URL"], future=True,
                           connect_args={"options": f"-csearch_path={schema}"})
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(open("migrations/0038_us_agent_tables.sql", encoding="utf-8").read())
            conn.exec_driver_sql(open("migrations/0039_us_prep_locked_watchlist_contract.sql", encoding="utf-8").read())
            conn.exec_driver_sql(open("migrations/0043_us_fills_idempotency_and_order_reconcile_fix.sql", encoding="utf-8").read())
            conn.exec_driver_sql(open("migrations/0046_us_orders_committed_notional.sql", encoding="utf-8").read())
            conn.exec_driver_sql(open("migrations/0047_us_order_events_profit_lifecycle.sql", encoding="utf-8").read())

        monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
        monkeypatch.setenv("KIS_ENV", "practice")
        monkeypatch.setenv("US_HARD_STOP_PCT", "0.08")

        monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
        monkeypatch.setenv("US_MAX_ORDER_USD", "2500")
        row = next(row for row in incident_rows() if row["symbol"] == "AAPL")
        saved = repos.clear_and_save_locked_us_watchlist([row], "2026-09-18", "incident-replay", "OK")
        assert saved["saved_count"] == 1
        # No test-side flattening: use the actual PostgreSQL loader and engine.
        loaded = repos.load_locked_us_watchlist("2026-09-18")
        intents = generate(loaded)
        assert len(intents) == 1
        intent = intents[0]
        qty = intent["qty"]
        assert intent["meta"]["entry_reason"] == "ENTRY_PULLBACK"
        assert repos.save_order_intent(intent, trade_date="2026-09-18")
        # Verify the real PostgreSQL persistence boundary instead of relying on
        # caller-dict mutation.
        with engine.connect() as conn:
            persisted_intent_meta = conn.execute(
                text("SELECT meta FROM us_order_intents WHERE client_order_key=:key"),
                {"key": intent["client_order_key"]},
            ).scalar_one()
        contract = persisted_intent_meta["entry_exit_contract"]
        root_sha = contract["sha256"]
        assert persisted_intent_meta["entry_exit_contract_sha256"] == root_sha

        assert repos.save_order_ack({
            **intent,
            "qty_requested": qty,
            "qty_filled": 0,
            "order_no": "AAPL-CONTRACT-E2E",
            "status": "ACK",
            "env": "practice",
            "meta": persisted_intent_meta,
        }, trade_date="2026-09-18")

        actual_fill = {
            "symbol": "AAPL", "exchange": "NASDAQ", "side": "BUY",
            "qty": qty, "price_usd": 100.0,
            "order_no": "AAPL-CONTRACT-E2E",
            "client_order_key": intent["client_order_key"],
            "filled_at": "2026-09-18T13:35:00+00:00",
            "cumulative_filled_qty": qty,
            "requested_qty": qty,
            "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
            "meta": {
                "fill_evidence_type": "KIS_ORDER_CUMULATIVE_ACTUAL",
                "cumulative_filled_qty": qty,
                "requested_qty": qty,
                "entry_exit_contract": contract,
                "entry_exit_contract_sha256": root_sha,
                "entry_exit_contract_version": contract["version"],
            },
        }
        fill_result = repos.save_fills_with_result([actual_fill], trade_date="2026-09-18")
        assert fill_result["status"] == "OK"
        with engine.connect() as conn:
            fill_meta = conn.execute(
                text("SELECT meta FROM us_fills WHERE client_order_key=:key"),
                {"key": intent["client_order_key"]},
            ).scalar_one()
        assert fill_meta["entry_exit_contract_sha256"] == root_sha
        assert fill_meta["entry_exit_contract"]["sha256"] == root_sha

        balance_row = {
            "symbol": "AAPL", "exchange": "NASDAQ", "qty": qty,
            "avg_price_usd": 100.0, "current_price_usd": 91.0,
            "balance_source": "kis_balance_authoritative",
            "meta": {"balance_source": "kis_balance_authoritative"},
        }
        assert repos.save_position_snapshot([balance_row], trade_date="2026-09-18") == 1

        with engine.connect() as conn:
            stored_meta = conn.execute(
                text("SELECT meta FROM us_positions WHERE symbol='AAPL' AND as_of='2026-09-18'")
            ).scalar_one()
        assert stored_meta["entry_exit_contract_sha256"] == root_sha
        assert stored_meta["entry_exit_contract"]["sha256"] == root_sha

        # Fresh DB load simulates the next process/tick.
        reloaded = repos.load_us_positions_by_symbols(["AAPL"], as_of="2026-09-18")["AAPL"]
        assert reloaded["meta"]["entry_exit_contract_sha256"] == root_sha

        # Mutating today's ENV must not alter the already-filled lifecycle.
        monkeypatch.setenv("US_HARD_STOP_PCT", "0.20")
        reloaded["resolved_current_price"] = 91.0
        reloaded["entry_price"] = 100.0
        reloaded["max_price"] = 100.0
        sells = generate_exit_intents(
            [reloaded], prepared_snapshots=[reloaded],
            now=datetime.now(timezone.utc), include_trend_time=False,
        )
        assert sells and sells[0]["side"] == "SELL"
        assert sells[0]["meta"]["hard_stop_threshold_pct"] == pytest.approx(0.08)
        assert sells[0]["source_entry_contract_sha256"] == root_sha
        assert sells[0]["meta"]["source_entry_contract_sha256"] == root_sha
        assert sells[0]["source_entry_reason"] == "ENTRY_PULLBACK"
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()
