import json
import logging
from pathlib import Path

import pytest


class _Provider:
    def __init__(self, price=110.0):
        self.price = price
    def get_current_price(self, symbol, exchange):
        return {"last": str(self.price)}
    def get_daily_prices(self, symbol, exchange, count=120):
        return [{"clos": "100", "tvol": "100000"}] * count


def _patch_entry_db(monkeypatch, pos):
    monkeypatch.setattr("trader.us.db.repos.has_pending_order_for_symbol_side", lambda **kwargs: False)
    monkeypatch.setattr("trader.us.db.repos.has_position", lambda symbol: symbol in pos)
    monkeypatch.setattr("trader.us.db.repos.load_today_order_keys", lambda trade_date=None: set())
    monkeypatch.setattr("trader.us.db.repos.load_us_positions_by_symbols", lambda symbols: {s: pos[s] for s in symbols if s in pos})


def _run_entry(monkeypatch, pos, price=110.0):
    from trader.us.pb1.us_entry_engine import generate_entry_intents
    monkeypatch.setenv("US_MIN_ENTRY_SCORE", "0.01")
    monkeypatch.setenv("US_MAX_NEW_ENTRIES_PER_TICK", "3")
    monkeypatch.setenv("US_ALLOW_ADD_TO_EXISTING", "1")
    monkeypatch.setenv("US_ALLOW_AVERAGING_DOWN", "0")
    monkeypatch.setenv("US_ADD_MIN_PNL_PCT", "0.0")
    monkeypatch.setenv("US_TARGET_POSITION_WEIGHT", "0.10")
    monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "0.20")
    monkeypatch.setenv("US_MAX_ORDER_USD", "10000")
    _patch_entry_db(monkeypatch, pos)
    return generate_entry_intents(
        tickers=None,
        provider=_Provider(price),
        sold_today=set(),
        available_cash_usd=10000.0,
        position_count=len(pos),
        capital_usd_cap=100000.0,
        watchlist_entries=[{"symbol": "AMD", "exchange": "NASDAQ", "score": 0.9}],
        current_position_symbols={"AMD"},
    )


def test_held_missing_pnl_skips_add_unknown(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    intents = _run_entry(monkeypatch, {"AMD": {"symbol": "AMD", "qty": 10, "market_value": 5000, "current_weight": 0.05}})
    assert intents == []
    assert "SKIP_ADD_PNL_UNKNOWN" in caplog.text


def test_held_negative_pnl_no_averaging_down(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    pos = {"AMD": {"symbol": "AMD", "qty": 10, "avg_price": 120, "current_price": 110, "market_value": 5000, "current_weight": 0.05}}
    intents = _run_entry(monkeypatch, pos, price=110)
    assert intents == []
    assert "SKIP_NO_AVERAGING_DOWN" in caplog.text


def test_held_positive_under_target_add_buy(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    pos = {"AMD": {"symbol": "AMD", "qty": 10, "avg_price": 100, "current_price": 110, "market_value": 5000, "current_weight": 0.05}}
    intents = _run_entry(monkeypatch, pos, price=110)
    assert len(intents) == 1
    assert intents[0]["symbol"] == "AMD"
    assert "ADD_BUY" in caplog.text


def test_held_at_max_weight_skips_full_weight(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    pos = {"AMD": {"symbol": "AMD", "qty": 10, "avg_price": 100, "current_price": 110, "market_value": 20000, "current_weight": 0.20}}
    intents = _run_entry(monkeypatch, pos, price=110)
    assert intents == []
    assert "SKIP_FULL_WEIGHT" in caplog.text


def test_held_max_add_count_reached_skips(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    monkeypatch.setenv("US_MAX_ADD_COUNT_PER_SYMBOL", "2")
    pos = {"AMD": {"symbol": "AMD", "qty": 10, "avg_price": 100, "current_price": 110, "market_value": 5000, "current_weight": 0.05, "add_count": 2}}
    intents = _run_entry(monkeypatch, pos, price=110)
    assert intents == []
    assert "SKIP_MAX_ADD_COUNT" in caplog.text


def test_no_balance_reject_without_recent_ack_remains_fatal():
    from trader.us.runner.status_contract import classify_tick_status
    result = {
        "status": "FAILED_ALL_EXIT_ORDERS_REJECTED",
        "primary_reject_reason": "모의투자 잔고내역이 없습니다",
        "no_balance_sell_reject_count": 1,
        "recent_sell_ack_exists": False,
        "balance_qty_zero": True,
    }
    assert classify_tick_status(result) == "fatal"


def test_no_balance_reject_with_recent_ack_and_qty_zero_is_warning():
    from trader.us.runner.status_contract import classify_tick_status
    result = {
        "status": "FAILED_ALL_EXIT_ORDERS_REJECTED",
        "primary_reject_reason": "모의투자 잔고내역이 없습니다",
        "no_balance_sell_reject_count": 1,
        "recent_sell_ack_exists": True,
        "balance_qty_zero": True,
    }
    assert classify_tick_status(result) == "warning"


def test_validator_sleep_start_without_done_fatal(tmp_path, monkeypatch):
    from scripts.validate_us_daily_report import validate_report
    monkeypatch.chdir(tmp_path)
    Path("artifacts").mkdir()
    Path("artifacts/us-trade-am.log").write_text("[US_TICK_LOOP][SLEEP_START]\n", encoding="utf-8")
    report = {
        "trade_date": "2026-06-18", "run_id": "1", "session": "am", "env": "practice",
        "dry_run": True, "final_status": "OK", "last_stage": "tick_1", "expected_to_trade": 1,
        "trade_runner_started": 1, "trade_status": "OK", "expected_min_ticks": 2, "ticks_total": 2,
    }
    Path("report.json").write_text(json.dumps(report), encoding="utf-8")
    code, fatals, _warnings = validate_report("report.json", "2026-06-18", "1", True, "am")
    assert code == 1
    assert "sleep_start_without_sleep_done" in fatals


def test_synthetic_reconcile_buy_not_real_broker_buy(monkeypatch):
    # Mirror tick classification: balance_reconcile_buy/synthetic sources are internal only.
    fill = {"side": "BUY", "source": "balance_reconcile_buy"}
    source = fill["source"].lower()
    real_broker_buys = 0 if "balance_reconcile" in source or "synthetic" in source else 1
    synthetic_reconcile_buys = 1 if "balance_reconcile" in source or "synthetic" in source else 0
    assert real_broker_buys == 0
    assert synthetic_reconcile_buys == 1



def test_order_router_no_balance_recent_ack_qty_zero_returns_closed(monkeypatch):
    from trader.us.db import repos
    from trader.us.execution.order_router import route_order, clear_sent_order_keys

    clear_sent_order_keys()
    repos.save_order_ack({
        "client_order_key": "old-sell-ack", "symbol": "AAOI", "exchange": "NASDAQ",
        "side": "SELL", "qty": 1, "order_no": "S1", "status": "ACK",
    }, trade_date="2026-06-18")
    _risk_env(monkeypatch)
    monkeypatch.setattr("trader.us.db.repos.load_us_positions_by_symbols", lambda symbols: {"AAOI": {"symbol": "AAOI", "qty": 0, "orderable_qty": 0}})
    monkeypatch.setattr("trader.us.db.repos.has_pending_order_for_symbol_side", lambda **kwargs: False)

    class _Kis:
        def place_us_sell_order(self, *args, **kwargs):
            raise RuntimeError("모의투자 잔고내역이 없습니다")

    result = route_order(
        {
            "symbol": "AAOI", "exchange": "NASDAQ", "side": "SELL", "qty": 1,
            "available_qty": 1, "orderable_qty": 1, "limit_price": 10.0, "notional_usd": 10.0,
            "client_order_key": "new-sell-no-balance", "trade_date": "2026-06-18",
        },
        kis_client=_Kis(),
        allowed_symbols={"AAOI"},
        current_position_symbols={"AAOI"},
    )
    assert result["status"] == "OK_EXIT_POSITION_CLOSED"


def _risk_env(monkeypatch):
    monkeypatch.setenv("US_AGENT_ENABLED", "1")
    monkeypatch.setenv("TRADING_REGION", "US")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("RUN_MODE", "TRADE")
    monkeypatch.setenv("STRATEGY_MODE", "TRADE")
    monkeypatch.setenv("SIGNAL_ONLY", "0")
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "0")
    monkeypatch.setenv("DISABLE_REAL_TRADING", "0")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("US_LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("US_ORDER_ARMED", "1")
    monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1")
    monkeypatch.setenv("ALLOW_REAL_ORDER", "1")
    monkeypatch.setenv("US_MAX_ORDER_USD", "10000")
    monkeypatch.setenv("US_MAX_DAILY_NOTIONAL_USD", "999999")
    monkeypatch.setenv("US_MAX_POSITIONS", "100")
    monkeypatch.setenv("US_MAX_POSITION_WEIGHT", "1.0")
    monkeypatch.setenv("US_MIN_CASH_BUFFER_USD", "0")
    monkeypatch.setenv("US_BLOCK_NEW_ENTRY_AFTER_ET", "")
    monkeypatch.setenv("US_BLOCK_REBUY_AFTER_SELL_SAME_DAY", "false")


def test_sell_pending_hard_gate_ignores_us_order_accepted_env(monkeypatch):
    from trader.us.execution.risk_gate import RiskGateBlocked, assert_order_allowed
    monkeypatch.setenv("US_ORDER_ACCEPTED_IS_NOT_FILLED", "0")
    _risk_env(monkeypatch)
    monkeypatch.setattr("trader.us.db.repos.has_pending_order_for_symbol_side", lambda **kwargs: kwargs.get("side") == "SELL")
    with pytest.raises(RiskGateBlocked, match="pending_sell_order_exists"):
        assert_order_allowed(
            {"symbol": "AAOI", "exchange": "NASDAQ", "side": "SELL", "qty": 1, "available_qty": 1, "notional_usd": 10, "client_order_key": "s1"},
            allowed_symbols={"AAOI"}, current_position_symbols={"AAOI"}, trade_date="2026-06-18",
        )


def test_buy_pending_can_follow_existing_env_policy(monkeypatch):
    from trader.us.execution.risk_gate import assert_order_allowed
    monkeypatch.setenv("US_ORDER_ACCEPTED_IS_NOT_FILLED", "0")
    _risk_env(monkeypatch)
    monkeypatch.setattr("trader.us.db.repos.has_pending_order_for_symbol_side", lambda **kwargs: True)
    assert_order_allowed(
        {"symbol": "AAOI", "exchange": "NASDAQ", "side": "BUY", "qty": 1, "notional_usd": 10, "client_order_key": "b1"},
        allowed_symbols={"AAOI"}, current_position_symbols=set(), trade_date="2026-06-18",
    )


def test_no_balance_warning_requires_same_symbol_qty_zero():
    from trader.us.runner.status_contract import classify_tick_status
    result = {
        "status": "FAILED_ALL_EXIT_ORDERS_REJECTED",
        "no_balance_sell_symbols": ["AAOI"],
        "recent_sell_ack_symbols": ["AAOI"],
        "balance_qty_zero_symbols": ["TSLA"],
        "orderable_qty_zero_symbols": [],
        "position_absent_symbols": [],
    }
    assert classify_tick_status(result) == "fatal"


def test_no_balance_same_symbol_recent_ack_qty_zero_is_warning():
    from trader.us.runner.status_contract import classify_tick_status
    result = {
        "status": "FAILED_ALL_EXIT_ORDERS_REJECTED",
        "no_balance_sell_symbols": ["AAOI"],
        "recent_sell_ack_symbols": ["AAOI"],
        "balance_qty_zero_symbols": ["AAOI"],
    }
    assert classify_tick_status(result) == "warning"


def test_no_balance_without_recent_ack_is_fatal_symbol_level():
    from trader.us.runner.status_contract import classify_tick_status
    result = {
        "status": "FAILED_ALL_EXIT_ORDERS_REJECTED",
        "no_balance_sell_symbols": ["AAOI"],
        "recent_sell_ack_symbols": [],
        "balance_qty_zero_symbols": ["AAOI"],
    }
    assert classify_tick_status(result) == "fatal"


def test_sell_no_orderable_qty_after_recent_ack_returns_position_closed(monkeypatch):
    from trader.us.execution.order_router import route_order, clear_sent_order_keys
    clear_sent_order_keys()
    _risk_env(monkeypatch)
    monkeypatch.setenv("US_ORDER_ACCEPTED_IS_NOT_FILLED", "0")
    monkeypatch.setattr("trader.us.db.repos.load_today_order_keys", lambda trade_date=None: set())
    monkeypatch.setattr("trader.us.db.repos.find_recent_sell_ack", lambda symbol, trade_date=None: {"order_no": "S1", "symbol": symbol, "side": "SELL"})
    monkeypatch.setattr("trader.us.db.repos.has_pending_order_for_symbol_side", lambda **kwargs: False)
    monkeypatch.setattr("trader.us.execution.us_sell_qty_guard.resolve_sell_qty", lambda intent, pos: (0, {"holding_qty": 1, "orderable_qty": 0}))
    called = {"save": 0}
    monkeypatch.setattr("trader.us.db.repos.save_order_intent", lambda intent: called.__setitem__("save", called["save"] + 1) or True)
    result = route_order(
        {"symbol": "AAOI", "exchange": "NASDAQ", "side": "SELL", "qty": 1, "available_qty": 1, "orderable_qty": 0, "limit_price": 10, "notional_usd": 10, "client_order_key": "no-orderable", "trade_date": "2026-06-18"},
        allowed_symbols={"AAOI"}, current_position_symbols={"AAOI"}, kis_client=object(),
    )
    assert result["status"] == "OK_EXIT_POSITION_CLOSED"


def test_sell_no_orderable_qty_without_recent_ack_remains_blocked(monkeypatch):
    from trader.us.execution.order_router import route_order, clear_sent_order_keys
    clear_sent_order_keys()
    _risk_env(monkeypatch)
    monkeypatch.setenv("US_ORDER_ACCEPTED_IS_NOT_FILLED", "0")
    monkeypatch.setattr("trader.us.db.repos.load_today_order_keys", lambda trade_date=None: set())
    monkeypatch.setattr("trader.us.db.repos.find_recent_sell_ack", lambda symbol, trade_date=None: None)
    monkeypatch.setattr("trader.us.db.repos.has_pending_order_for_symbol_side", lambda **kwargs: False)
    monkeypatch.setattr("trader.us.execution.us_sell_qty_guard.resolve_sell_qty", lambda intent, pos: (0, {"holding_qty": 1, "orderable_qty": 0}))
    result = route_order(
        {"symbol": "AAOI", "exchange": "NASDAQ", "side": "SELL", "qty": 1, "available_qty": 1, "orderable_qty": 0, "limit_price": 10, "notional_usd": 10, "client_order_key": "no-orderable-2", "trade_date": "2026-06-18"},
        allowed_symbols={"AAOI"}, current_position_symbols={"AAOI"}, kis_client=object(),
    )
    assert result["status"] == "BLOCKED"
    assert result["reason"] == "no_orderable_qty"


def test_duplicate_sell_precheck_skips_before_save_intent(monkeypatch):
    from trader.us.execution.order_router import route_order, clear_sent_order_keys
    clear_sent_order_keys()
    _risk_env(monkeypatch)
    monkeypatch.setattr("trader.us.db.repos.load_today_order_keys", lambda trade_date=None: {"dup-sell"})
    def _save(_intent):
        raise AssertionError("save_order_intent should not be called")
    monkeypatch.setattr("trader.us.db.repos.save_order_intent", _save)
    result = route_order(
        {"symbol": "AAOI", "exchange": "NASDAQ", "side": "SELL", "qty": 1, "available_qty": 1, "limit_price": 10, "notional_usd": 10, "client_order_key": "dup-sell", "trade_date": "2026-06-18"},
        allowed_symbols={"AAOI"}, current_position_symbols={"AAOI"}, kis_client=object(),
    )
    assert result["status"] == "WARN_DUPLICATE_EXIT_BLOCKED"
    assert result["duplicate_blocked"] is True


def test_exit_engine_skips_intent_when_pending_sell_exists(monkeypatch, caplog):
    from trader.us.pb1.us_exit_engine import evaluate_exit
    caplog.set_level(logging.INFO)
    monkeypatch.setattr("trader.us.db.repos.find_recent_sell_ack", lambda symbol, trade_date=None: {"order_no": "S1", "symbol": symbol, "side": "SELL"})
    monkeypatch.setattr("trader.us.db.repos.has_pending_order_for_symbol_side", lambda **kwargs: True)
    pos = {"symbol": "AAOI", "exchange": "NASDAQ", "qty": 1, "entry_price": 100, "max_price": 100}
    intent = evaluate_exit(pos, current_price=80.0)
    assert intent is None
    assert "[US_EXIT][SKIP_PENDING_SELL]" in caplog.text


def test_session_warning_does_not_increment_consecutive_errors(monkeypatch, caplog):
    from trader.us.runner.trade_session_runner import run_trade_session
    caplog.set_level(logging.INFO)
    results = iter([
        {"status": "OK_EXIT_ORDERS_SENT", "last_stage": "order_route"},
        {"status": "WARN_DUPLICATE_EXIT_BLOCKED", "reason": "duplicate_exit_blocked", "last_stage": "order_route"},
        {"status": "WARN_SELL_REJECT_RECONCILE_PENDING", "reason": "no_balance_after_recent_sell_ack", "last_stage": "order_route"},
    ])
    monkeypatch.setattr("trader.us.runner.trade_tick_runner.run_trade_tick", lambda **kwargs: next(results))
    monkeypatch.setattr("trader.us.utils.session_guard.check_us_session_file_guard", lambda trade_date, session: {"already_ran": False, "guard_status": "OK", "payload": {}})
    monkeypatch.setattr("trader.us.utils.session_guard.write_us_session_done_file", lambda **kwargs: None)
    monkeypatch.setattr("trader.us.runner.trade_session_runner._write_us_session_report", lambda payload, session: None)
    monkeypatch.setattr("trader.us.runner.trade_session_runner._write_us_schedule_health", lambda payload, session: None)
    out = run_trade_session(session="afternoon", offline=True, force_now="2026-06-18T13:00:00-04:00", max_ticks=3, interval_sec=1)
    assert out["status"] in {"OK", "OK_WITH_WARNINGS"}
    assert "reason=consecutive_errors" not in caplog.text
    assert "class=warning" in caplog.text


def test_all_exit_blocked_no_orderable_without_recent_ack_is_fatal():
    from trader.us.runner.status_contract import classify_tick_status
    result = {
        "status": "FAILED_ALL_EXIT_ORDERS_BLOCKED",
        "block_reasons": {"no_orderable_qty": 1},
        "recent_sell_ack_symbols": [],
        "balance_qty_zero_symbols": ["AAOI"],
        "orderable_qty_zero_symbols": ["AAOI"],
        "position_absent_symbols": [],
    }
    assert classify_tick_status(result) == "fatal"


def test_all_exit_blocked_duplicate_exit_is_warning():
    from trader.us.runner.status_contract import classify_tick_status
    result = {
        "status": "FAILED_ALL_EXIT_ORDERS_BLOCKED",
        "duplicate_exit_blocked": True,
        "block_reasons": {"duplicate_sell_client_order_key": 1},
    }
    assert classify_tick_status(result) == "warning"


def test_all_exit_blocked_pending_sell_is_warning():
    from trader.us.runner.status_contract import classify_tick_status
    result = {"status": "FAILED_ALL_EXIT_ORDERS_BLOCKED", "block_reasons": {"pending_sell_order_exists": 1}}
    assert classify_tick_status(result) == "warning"


def test_all_exit_blocked_no_orderable_recent_ack_same_symbol_closed_is_warning():
    from trader.us.runner.status_contract import classify_tick_status
    result = {
        "status": "FAILED_ALL_EXIT_ORDERS_BLOCKED",
        "block_reasons": {"no_orderable_qty": 1},
        "recent_sell_ack_symbols": ["AAOI"],
        "orderable_qty_zero_symbols": ["AAOI"],
    }
    assert classify_tick_status(result) == "warning"


def test_no_balance_reject_requires_same_symbol_closed_final():
    from trader.us.runner.status_contract import classify_tick_status
    result = {
        "status": "FAILED_ALL_EXIT_ORDERS_REJECTED",
        "no_balance_sell_symbols": ["AAOI"],
        "recent_sell_ack_symbols": ["AAOI"],
        "balance_qty_zero_symbols": ["TSLA"],
    }
    assert classify_tick_status(result) == "fatal"


def test_route_order_ack_db_failed_when_save_order_ack_returns_false(monkeypatch):
    from trader.us.execution.order_router import route_order, clear_sent_order_keys
    clear_sent_order_keys()
    _risk_env(monkeypatch)
    monkeypatch.setenv("US_ORDER_ACCEPTED_IS_NOT_FILLED", "0")
    monkeypatch.setattr("trader.us.db.repos.load_today_order_keys", lambda trade_date=None: set())
    monkeypatch.setattr("trader.us.db.repos.has_pending_order_for_symbol_side", lambda **kwargs: False)
    monkeypatch.setattr("trader.us.db.repos.save_order_ack", lambda _ack: False)

    class _Kis:
        def place_us_sell_order(self, *args, **kwargs):
            return {"output": {"ODNO": "ACK1"}}

    result = route_order(
        {"symbol": "AAOI", "exchange": "NASDAQ", "side": "SELL", "qty": 1, "available_qty": 1, "orderable_qty": 1, "limit_price": 10, "notional_usd": 10, "client_order_key": "ack-false", "trade_date": "2026-06-18"},
        allowed_symbols={"AAOI"}, current_position_symbols={"AAOI"}, kis_client=_Kis(),
    )
    assert result["status"] == "ACK_DB_FAILED"
    assert result["requires_reconcile"] is True


def test_buy_client_order_key_uses_new_york_trade_date_when_now_none(monkeypatch):
    import hashlib
    import trader.us.pb1.us_entry_engine as entry_engine
    from trader.us.pb1.us_entry_engine import generate_entry_intents

    class _FakeDateTime:
        @classmethod
        def now(cls, tz=None):
            from datetime import datetime
            from zoneinfo import ZoneInfo
            return datetime(2026, 6, 17, 23, 30, tzinfo=ZoneInfo("America/New_York")).astimezone(tz) if tz else datetime(2026, 6, 18, 12, 30)

    monkeypatch.setattr(entry_engine, "datetime", _FakeDateTime)
    monkeypatch.setenv("US_MIN_ENTRY_SCORE", "0.01")
    monkeypatch.setenv("US_MAX_NEW_ENTRIES_PER_TICK", "1")
    monkeypatch.setattr("trader.us.db.repos.has_pending_order_for_symbol_side", lambda **kwargs: False)
    monkeypatch.setattr("trader.us.db.repos.has_position", lambda symbol: False)
    monkeypatch.setattr("trader.us.db.repos.load_today_order_keys", lambda trade_date=None: set())
    intents = generate_entry_intents(
        tickers=None,
        provider=_Provider(110),
        sold_today=set(),
        available_cash_usd=10000,
        position_count=0,
        capital_usd_cap=100000,
        watchlist_entries=[{"symbol": "AMD", "exchange": "NASDAQ", "score": 0.9}],
        current_position_symbols=set(),
    )
    expected = hashlib.sha256("AMD_20260617_BUY".encode()).hexdigest()[:24]
    assert intents[0]["client_order_key"] == expected
    assert intents[0]["trade_date"] == "2026-06-17"


def test_warning_classification_does_not_increment_consecutive_errors():
    from trader.us.runner.status_contract import classify_tick_status
    consecutive_errors = 0
    result = {"status": "FAILED_ALL_EXIT_ORDERS_BLOCKED", "duplicate_exit_blocked": True, "block_reasons": {"duplicate_sell_client_order_key": 1}}
    if classify_tick_status(result) == "warning":
        consecutive_errors = 0
    else:
        consecutive_errors += 1
    assert consecutive_errors == 0
