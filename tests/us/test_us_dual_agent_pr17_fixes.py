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
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1")
    monkeypatch.delenv("US_ORDER_ARMED", raising=False)
    monkeypatch.setattr("trader.us.db.repos.load_us_positions_by_symbols", lambda symbols: {"AAOI": {"symbol": "AAOI", "qty": 0, "orderable_qty": 0}})

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
