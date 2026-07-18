from __future__ import annotations


def test_ack_reconcile_passes_trade_date_to_provider(monkeypatch):
    from trader.us.execution import reconcile

    monkeypatch.setattr("trader.us.db.repos.load_pending_ack_orders", lambda trade_date, env="practice": [{
        "symbol": "AMD",
        "side": "SELL",
        "order_no": "O1",
        "client_order_key": "CK1",
        "qty_requested": 7,
        "pre_order_position_qty": 7,
        "limit_price": 100,
    }])
    monkeypatch.setattr("trader.us.db.repos.mark_order_filled_by_reconcile", lambda **kwargs: {"status": "OK"})
    calls = []

    class Provider:
        def get_balance(self):
            return {"positions": []}

        def get_fills_by_order_no(self, *, order_no, symbol, trade_date):
            calls.append((order_no, symbol, trade_date))
            return {"filled_qty": 7, "avg_price": 100, "symbol": "AMD", "side": "SELL", "order_no": "O1"}

    result = reconcile.reconcile_ack_orders_with_balance(provider=Provider(), trade_date="2026-07-17")

    assert result["confirmed_count"] == 1
    assert calls == [("O1", "AMD", "2026-07-17")]


def test_ack_reconcile_signature_typeerror_is_failed_not_confirmed(monkeypatch):
    from trader.us.execution import reconcile

    monkeypatch.setattr("trader.us.db.repos.load_pending_ack_orders", lambda trade_date, env="practice": [{
        "symbol": "AMD",
        "side": "SELL",
        "order_no": "O1",
        "client_order_key": "CK1",
        "qty_requested": 7,
        "pre_order_position_qty": 7,
        "limit_price": 100,
    }])

    class BadProvider:
        def get_balance(self):
            return {"positions": []}

        def get_fills_by_order_no(self, order_no, symbol):
            return {"filled_qty": 7}

    result = reconcile.reconcile_ack_orders_with_balance(provider=BadProvider(), trade_date="2026-07-17")

    assert result["confirmed_count"] == 0
    assert result["failed_count"] == 1
    assert result["unresolved_count"] == 1


def test_provider_uses_max_cumulative_snapshot_not_sum(monkeypatch):
    from trader.us.data_provider import USDataProvider

    provider = USDataProvider(offline=False)
    provider._offline = False
    monkeypatch.setattr(provider, "get_today_orders", lambda trade_date: [
        {"order_no": "O1", "symbol": "AMD", "side": "SELL", "cumulative_filled_qty": 3, "avg_price": 100, "observed_at": "2026-07-17T14:00:00Z"},
        {"order_no": "O1", "symbol": "AMD", "side": "SELL", "cumulative_filled_qty": 7, "avg_price": 101, "observed_at": "2026-07-17T14:01:00Z"},
    ])

    result = provider.get_fills_by_order_no(order_no="O1", symbol="AMD", trade_date="2026-07-17")

    assert result["filled_qty"] == 7
    assert result["cumulative_filled_qty"] == 7
    assert result["avg_price"] == 101


def test_close_db_fill_save_error_fails_close(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("US_CLOSE_ENTRY_ENABLED", "0")

    class Provider:
        def get_balance(self):
            return {"positions": [], "balance_parse_status": "OK"}

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: Provider())
    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", lambda **kwargs: {"status": "OK", "fills": [{"symbol": "AMD"}]})
    monkeypatch.setattr("trader.us.db.repos.save_fills_with_result", lambda fills, trade_date=None: {"status": "DB_ERROR", "error": "boom", "inserted_count": 0, "updated_count": 0, "unchanged_count": 0, "regression_count": 0})
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", lambda provider, trade_date: {"status": "OK", "balance_fetch_status": "OK", "authoritative_positions": True, "preserve_previous_positions": False, "positions": []})
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda *args, **kwargs: None)
    monkeypatch.setattr("trader.us.execution.reconcile.classify_ack_orders_with_final_balance", lambda **kwargs: {"status": "OK", "orders": [], "counts": {}, "pending_order_count": 0})
    monkeypatch.setattr("trader.us.runner.daily_report_runner.run_daily_report", lambda **kwargs: {"status": "OK", "report": {"report_consistency": "OK"}})

    from trader.us.runner.trade_close_runner import run_trade_close

    result = run_trade_close(env="practice", offline=False, force_now="2026-07-17T16:05:00-04:00")

    assert result["status"] == "ERROR"
    assert result["fills_status"] == "DB_ERROR"
    assert result["report_consistency"] == "FAILED"


def test_close_persists_explicit_us_trade_date(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    saved = {"fills_trade_date": None, "positions_trade_date": None, "log_trade_date": None, "reconcile_trade_date": None}

    class Provider:
        def get_balance(self):
            return {"positions": [], "balance_parse_status": "OK"}

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: Provider())
    monkeypatch.setattr("trader.us.execution.fills.get_fills_today", lambda **kwargs: {"status": "OK", "fills": [{"symbol": "AMD"}]})

    def save_fills_with_result(fills, trade_date=None):
        saved["fills_trade_date"] = trade_date
        return {"status": "OK", "inserted_count": 1, "updated_count": 0, "unchanged_count": 0, "regression_count": 0}

    def reconcile_positions(provider, trade_date):
        saved["reconcile_trade_date"] = trade_date
        return {"status": "OK", "balance_fetch_status": "OK", "authoritative_positions": True, "preserve_previous_positions": False, "positions": []}

    def save_position_snapshot(positions, **kwargs):
        saved["positions_trade_date"] = kwargs.get("trade_date")

    def save_reconcile_log(payload, trade_date=None):
        saved["log_trade_date"] = trade_date

    monkeypatch.setattr("trader.us.db.repos.save_fills_with_result", save_fills_with_result)
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", reconcile_positions)
    monkeypatch.setattr("trader.us.db.repos.save_position_snapshot", save_position_snapshot)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", save_reconcile_log)
    monkeypatch.setattr("trader.us.execution.reconcile.classify_ack_orders_with_final_balance", lambda **kwargs: {"status": "OK", "orders": [], "counts": {}, "pending_order_count": 0})
    monkeypatch.setattr("trader.us.runner.daily_report_runner.run_daily_report", lambda **kwargs: {"status": "OK", "report": {"report_consistency": "OK"}})

    from trader.us.runner.trade_close_runner import run_trade_close

    result = run_trade_close(env="practice", offline=False, force_now="2026-07-17T16:05:00-04:00")

    assert result["status"] == "OK"
    assert saved == {
        "fills_trade_date": "2026-07-17",
        "positions_trade_date": "2026-07-17",
        "log_trade_date": "2026-07-17",
        "reconcile_trade_date": "2026-07-17",
    }
