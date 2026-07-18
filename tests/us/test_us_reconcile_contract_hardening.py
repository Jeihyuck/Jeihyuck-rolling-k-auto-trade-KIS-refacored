from __future__ import annotations


def _pending_sell_order():
    return {
        "symbol": "AMD",
        "side": "SELL",
        "order_no": "O1",
        "client_order_key": "CK1",
        "qty_requested": 7,
        "pre_order_position_qty": 7,
        "limit_price": 100,
    }


def test_ack_reconcile_non_dict_mark_result_is_failure(monkeypatch):
    from trader.us.execution import reconcile

    monkeypatch.setattr(
        "trader.us.db.repos.load_pending_ack_orders",
        lambda trade_date, env="practice": [_pending_sell_order()],
    )
    monkeypatch.setattr(
        "trader.us.db.repos.mark_order_filled_by_reconcile",
        lambda **kwargs: None,
    )

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": []}

        def get_fills_by_order_no(self, *, order_no, symbol, trade_date):
            return {
                "status": "OK",
                "filled_qty": 7,
                "avg_price": 100,
                "symbol": "AMD",
                "side": "SELL",
                "order_no": "O1",
            }

    result = reconcile.reconcile_ack_orders_with_balance(
        provider=Provider(), trade_date="2026-07-17"
    )

    assert result["status"] == "ERROR"
    assert result["confirmed_count"] == 0
    assert result["failed_count"] == 1
    assert result["unresolved_count"] == 1


def test_ack_reconcile_rejects_regressed_provider_snapshot(monkeypatch):
    from trader.us.execution import reconcile

    monkeypatch.setattr(
        "trader.us.db.repos.load_pending_ack_orders",
        lambda trade_date, env="practice": [_pending_sell_order()],
    )
    calls = []
    monkeypatch.setattr(
        "trader.us.db.repos.mark_order_filled_by_reconcile",
        lambda **kwargs: calls.append(kwargs) or {"status": "OK"},
    )

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": []}

        def get_fills_by_order_no(self, *, order_no, symbol, trade_date):
            return {
                "status": "EVIDENCE_QUANTITY_REGRESSION",
                "filled_qty": 3,
                "avg_price": 100,
                "symbol": "AMD",
                "side": "SELL",
                "order_no": "O1",
            }

    result = reconcile.reconcile_ack_orders_with_balance(
        provider=Provider(), trade_date="2026-07-17"
    )

    assert result["status"] == "ERROR"
    assert result["confirmed_count"] == 0
    assert result["failed_count"] == 1
    assert calls == []


def test_close_does_not_retry_reconcile_without_trade_date(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    calls = []

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": [], "balance_parse_status": "OK"}

    def broken_reconcile(provider, *, trade_date):
        calls.append(trade_date)
        raise TypeError("internal reconcile bug")

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: Provider())
    monkeypatch.setattr(
        "trader.us.execution.fills.get_fills_today",
        lambda **kwargs: {"status": "OK", "fills": []},
    )
    monkeypatch.setattr("trader.us.execution.reconcile.reconcile_positions", broken_reconcile)
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        "trader.us.execution.reconcile.classify_ack_orders_with_final_balance",
        lambda **kwargs: {"status": "OK", "orders": [], "counts": {}, "pending_order_count": 0},
    )
    monkeypatch.setattr(
        "trader.us.runner.daily_report_runner.run_daily_report",
        lambda **kwargs: {"status": "OK", "report": {"report_consistency": "OK"}},
    )

    from trader.us.runner.trade_close_runner import run_trade_close

    result = run_trade_close(
        env="practice", offline=False, force_now="2026-07-17T16:05:00-04:00"
    )

    assert calls == ["2026-07-17"]
    assert result["status"] == "ERROR"
    assert result["reconcile_status"] == "CONTRACT_ERROR"


def test_close_position_snapshot_failure_is_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": [], "balance_parse_status": "OK"}

    monkeypatch.setattr("trader.us.data_provider.USDataProvider", lambda offline=False: Provider())
    monkeypatch.setattr(
        "trader.us.execution.fills.get_fills_today",
        lambda **kwargs: {"status": "OK", "fills": []},
    )
    monkeypatch.setattr(
        "trader.us.execution.reconcile.reconcile_positions",
        lambda provider, trade_date: {
            "status": "OK",
            "balance_fetch_status": "OK",
            "balance_parse_status": "OK",
            "authoritative_positions": True,
            "preserve_previous_positions": False,
            "positions": [],
        },
    )
    monkeypatch.setattr(
        "trader.us.db.repos.save_position_snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("db down")),
    )
    monkeypatch.setattr("trader.us.db.repos.save_reconcile_log", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        "trader.us.execution.reconcile.classify_ack_orders_with_final_balance",
        lambda **kwargs: {"status": "OK", "orders": [], "counts": {}, "pending_order_count": 0},
    )
    monkeypatch.setattr(
        "trader.us.runner.daily_report_runner.run_daily_report",
        lambda **kwargs: {"status": "OK", "report": {"report_consistency": "OK"}},
    )

    from trader.us.runner.trade_close_runner import run_trade_close

    result = run_trade_close(
        env="practice", offline=False, force_now="2026-07-17T16:05:00-04:00"
    )

    assert result["status"] == "ERROR"
    assert result["report_consistency"] == "FAILED"
    assert result["position_snapshot_error"] == "db down"
