import json


def _intent(symbol="AAPL", side="BUY", qty=2, price=100.0):
    return {
        "trade_date": "2026-07-07",
        "client_order_key": f"{side}-{symbol}",
        "symbol": symbol,
        "exchange": "NASDAQ",
        "side": side,
        "qty": qty,
        "limit_price": price,
        "notional_usd": qty * price,
        "strategy": "test",
    }


class CashZeroClient:
    def __init__(self):
        self.buy_calls = 0
    def get_orderable_cash(self, **kwargs):
        return 0.0
    def place_us_buy_order(self, *args, **kwargs):
        self.buy_calls += 1
        return {"rt_cd": "0", "output": {"ODNO": "1"}}


class CashRejectClient:
    def __init__(self):
        self.buy_calls = 0
    def get_orderable_cash(self, **kwargs):
        return 100000.0
    def place_us_buy_order(self, *args, **kwargs):
        self.buy_calls += 1
        raise RuntimeError("모의투자 주문가능금액 부족")


class NoQtyClient:
    def __init__(self):
        self.sell_calls = 0
    def get_balance(self, force_refresh=False):
        return {"positions": [{"symbol": "AAPL", "qty": 1, "orderable_qty": 0, "currency": "USD"}]}
    def place_us_sell_order(self, *args, **kwargs):
        self.sell_calls += 1
        return {"rt_cd": "0", "output": {"ODNO": "S1"}}


class NoBalanceRejectClient:
    def get_balance(self, force_refresh=False):
        return {"positions": [{"symbol": "AAPL", "qty": 1, "orderable_qty": 1, "currency": "USD"}]}
    def place_us_sell_order(self, *args, **kwargs):
        raise RuntimeError("모의투자 잔고내역이 없습니다")


def test_internal_available_ignored_when_broker_cash_zero(monkeypatch):
    from trader.us.db import repos
    from trader.us.execution import order_router
    repos.reset_memory_stores(); order_router._CASH_EXHAUSTED_TICKS.clear()
    monkeypatch.setenv("DRY_RUN", "0"); monkeypatch.setenv("KIS_ENV", "practice"); monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1"); monkeypatch.setenv("LIVE_TRADING_ENABLED", "1"); monkeypatch.setenv("US_LIVE_TRADING_ENABLED", "1"); monkeypatch.setenv("US_ORDER_ARMED", "1"); monkeypatch.setenv("DISABLE_LIVE_TRADING", "0"); monkeypatch.setenv("STRATEGY_ENV", "practice"); monkeypatch.setenv("US_SESSION_WINDOW_VALID", "1"); monkeypatch.setenv("US_PREP_CONTRACT_OK", "1"); monkeypatch.setenv("US_BALANCE_AVAILABLE", "1"); monkeypatch.setenv("US_MAX_ORDER_USD", "100000")
    client = CashZeroClient()
    result = order_router.route_order(_intent(), available_cash_usd=999999, total_portfolio_usd=100000, kis_client=client)
    assert result["status"] == "BLOCKED"
    assert result["reason"] == "broker_orderable_cash_insufficient"
    assert client.buy_calls == 0


def test_cash_reject_stops_remaining_buy_same_tick(monkeypatch):
    from trader.us.db import repos
    from trader.us.execution import order_router
    repos.reset_memory_stores(); order_router._CASH_EXHAUSTED_TICKS.clear()
    monkeypatch.setenv("DRY_RUN", "0"); monkeypatch.setenv("KIS_ENV", "practice"); monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1"); monkeypatch.setenv("LIVE_TRADING_ENABLED", "1"); monkeypatch.setenv("US_LIVE_TRADING_ENABLED", "1"); monkeypatch.setenv("US_ORDER_ARMED", "1"); monkeypatch.setenv("DISABLE_LIVE_TRADING", "0"); monkeypatch.setenv("STRATEGY_ENV", "practice"); monkeypatch.setenv("US_SESSION_WINDOW_VALID", "1"); monkeypatch.setenv("US_PREP_CONTRACT_OK", "1"); monkeypatch.setenv("US_BALANCE_AVAILABLE", "1"); monkeypatch.setenv("US_MAX_ORDER_USD", "100000")
    client = CashRejectClient()
    first = order_router.route_order(_intent("META"), total_portfolio_usd=100000, kis_client=client)
    second = order_router.route_order(_intent("MSFT"), total_portfolio_usd=100000, kis_client=client)
    assert first["status"] == "REJECT"
    assert first["cash_exhausted"] is True
    assert second["status"] == "BLOCKED"
    assert client.buy_calls == 1


def test_db_position_ignored_when_broker_orderable_qty_zero(monkeypatch):
    from trader.us.db import repos
    from trader.us.execution import order_router
    repos.reset_memory_stores(); order_router._CASH_EXHAUSTED_TICKS.clear()
    monkeypatch.setenv("DRY_RUN", "0"); monkeypatch.setenv("KIS_ENV", "practice"); monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1"); monkeypatch.setenv("LIVE_TRADING_ENABLED", "1"); monkeypatch.setenv("US_LIVE_TRADING_ENABLED", "1"); monkeypatch.setenv("US_ORDER_ARMED", "1"); monkeypatch.setenv("DISABLE_LIVE_TRADING", "0"); monkeypatch.setenv("STRATEGY_ENV", "practice"); monkeypatch.setenv("US_SESSION_WINDOW_VALID", "1"); monkeypatch.setenv("US_PREP_CONTRACT_OK", "1"); monkeypatch.setenv("US_BALANCE_AVAILABLE", "1"); monkeypatch.setenv("US_MAX_ORDER_USD", "100000")
    client = NoQtyClient()
    result = order_router.route_order(_intent("AAPL", "SELL", 1, 90), total_portfolio_usd=100000, kis_client=client)
    assert result["status"] == "BLOCKED"
    assert result["reason"] == "broker_orderable_qty_zero"
    assert client.sell_calls == 0


def test_no_balance_reject_marks_stale_broker_mismatch(monkeypatch):
    from trader.us.db import repos
    from trader.us.execution import order_router
    repos.reset_memory_stores(); order_router._CASH_EXHAUSTED_TICKS.clear()
    monkeypatch.setenv("DRY_RUN", "0"); monkeypatch.setenv("KIS_ENV", "practice"); monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1"); monkeypatch.setenv("LIVE_TRADING_ENABLED", "1"); monkeypatch.setenv("US_LIVE_TRADING_ENABLED", "1"); monkeypatch.setenv("US_ORDER_ARMED", "1"); monkeypatch.setenv("DISABLE_LIVE_TRADING", "0"); monkeypatch.setenv("STRATEGY_ENV", "practice"); monkeypatch.setenv("US_SESSION_WINDOW_VALID", "1"); monkeypatch.setenv("US_PREP_CONTRACT_OK", "1"); monkeypatch.setenv("US_BALANCE_AVAILABLE", "1"); monkeypatch.setenv("US_MAX_ORDER_USD", "100000")
    result = order_router.route_order(_intent("AAPL", "SELL", 1, 90), total_portfolio_usd=100000, kis_client=NoBalanceRejectClient())
    assert result["status"] in {"REJECT", "WARN_SELL_REJECT_RECONCILE_PENDING"}
    state = repos.load_us_position_risk_state("AAPL", "2026-07-07")
    assert state["stale_broker_mismatch"] is True


def test_ack_only_not_counted_as_fill_balance_delta_confirmed():
    from trader.us.execution.reconcile import classify_ack_orders_with_final_balance
    class Provider:
        def get_balance(self, force_refresh=False):
            return {"positions": []}
    orders = [
        {"symbol": "APH", "side": "SELL", "qty_requested": 1, "status": "ACK", "order_no": "1", "meta": {"pre_order_position_qty": 1}},
        {"symbol": "U1", "side": "BUY", "qty_requested": 1, "status": "ACK", "order_no": "2", "meta": {"pre_order_position_qty": 0}},
    ]
    out = classify_ack_orders_with_final_balance(provider=Provider(), trade_date="2026-07-07", env="practice", orders=orders)
    by_symbol = {o["symbol"]: o for o in out["orders"]}
    assert by_symbol["APH"]["final_status"] == "balance_delta_confirmed"
    assert by_symbol["U1"]["final_status"] == "ack_unresolved_error"
    assert out["counts"].get("broker_fill_confirmed", 0) == 0


def test_latest_report_trade_date_mismatch_fails(tmp_path):
    from scripts.validate_us_daily_report import validate_report
    report = tmp_path / "latest_us_daily_report.json"
    report.write_text(json.dumps({"trade_date": "2026-07-06", "run_id": "1", "dry_run": False}))
    code, fatals, _ = validate_report(str(report), "2026-07-07", "1", False, "close")
    assert code == 1
    assert any("trade_date_mismatch" in f for f in fatals)


def test_daily_report_source_mismatch_status(monkeypatch, tmp_path):
    from trader.us.runner import daily_report_runner as drr
    monkeypatch.chdir(tmp_path)
    import trader.us.db.repos as repos
    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda td: [{"status": "ACK", "side": "BUY", "symbol": "A", "meta": {}}])
    monkeypatch.setattr(drr, "load_us_fills_breakdown", lambda td: {"fills_count": 0})
    monkeypatch.setattr(drr, "load_balance_confirmed_count", lambda td: 0)
    monkeypatch.setattr(drr, "load_router_summary_ack_count", lambda td, session=None: 2)
    monkeypatch.setattr(repos, "load_positions", lambda as_of=None: [])
    out = drr.run_daily_report(session="am", trade_date="2026-07-07", offline=False)
    report = out["report"]
    assert report["status"] == "WARNING_RECONCILE_MISMATCH"
    assert "SOURCE_MISMATCH" in report["warnings"]


def test_cash_exhausted_key_is_tick_scoped(monkeypatch):
    from trader.us.execution import order_router
    monkeypatch.setenv("PB1_SESSION", "am")
    monkeypatch.setenv("GITHUB_RUN_ID", "run-1")
    k1 = order_router._cash_tick_key("2026-07-07", {"meta": {"tick_seq": 1}})
    k2 = order_router._cash_tick_key("2026-07-07", {"meta": {"tick_seq": 2}})
    assert k1 != k2
    assert "2026-07-07" in k1 and "am" in k1 and "run-1" in k1


def test_orderable_cash_parser_handles_kis_psamount_sample(caplog):
    from trader.us.execution.order_router import _parse_orderable_cash_output
    sample = {"ord_psbl_frcr_amt": "2432.55", "ord_psbl_qty": "4", "max_ord_psbl_qty": "4"}
    assert _parse_orderable_cash_output(sample, symbol="META", account_env="practice") == 2432.55
    assert _parse_orderable_cash_output({"unexpected": "1"}, symbol="META", account_env="practice") is None
    assert "[US_ORDER][BROKER_CASH_PARSE_WARN]" in caplog.text


def test_daily_report_counts_broker_blocked_reasons(monkeypatch, tmp_path):
    from trader.us.runner import daily_report_runner as drr
    import trader.us.db.repos as repos
    monkeypatch.chdir(tmp_path)
    rows = [
        {"status": "BLOCKED", "side": "BUY", "symbol": "A", "meta": {"reason": "broker_orderable_cash_insufficient"}},
        {"status": "BLOCKED", "side": "SELL", "symbol": "B", "meta": {"reason": "broker_orderable_qty_zero"}},
    ]
    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda td: rows)
    monkeypatch.setattr(repos, "load_positions", lambda as_of=None: [])
    monkeypatch.setattr(drr, "load_us_fills_breakdown", lambda td: {"fills_count": 0})
    monkeypatch.setattr(drr, "load_balance_confirmed_count", lambda td: 0)
    monkeypatch.setattr(drr, "load_router_summary_ack_count", lambda td, session=None: 0)
    out = drr.run_daily_report(session="close", trade_date="2026-07-07", offline=False)
    report = out["report"]
    assert report["orders_blocked"] == 2
    assert report["broker_orderable_cash_blocks"] == 1
    assert report["broker_orderable_qty_blocks"] == 1
    assert report["cash_exhausted"] is True


class CashUnparseableClient:
    def __init__(self):
        self.buy_calls = 0
    def get_us_orderable_cash(self, **kwargs):
        return {"output": {"unexpected_cash_key": "999999"}}
    def place_us_buy_order(self, *args, **kwargs):
        self.buy_calls += 1
        return {"rt_cd": "0", "output": {"ODNO": "BAD"}}


def test_unparseable_orderable_cash_blocks_live_buy(monkeypatch):
    from trader.us.db import repos
    from trader.us.execution import order_router
    repos.reset_memory_stores(); order_router._CASH_EXHAUSTED_TICKS.clear(); order_router._CASH_UNAVAILABLE_TICKS.clear()
    monkeypatch.setenv("DRY_RUN", "0"); monkeypatch.setenv("KIS_ENV", "practice"); monkeypatch.setenv("US_PAPER_TRADING_ENABLED", "1"); monkeypatch.setenv("LIVE_TRADING_ENABLED", "1"); monkeypatch.setenv("US_LIVE_TRADING_ENABLED", "1"); monkeypatch.setenv("US_ORDER_ARMED", "1"); monkeypatch.setenv("DISABLE_LIVE_TRADING", "0"); monkeypatch.setenv("STRATEGY_ENV", "practice"); monkeypatch.setenv("US_SESSION_WINDOW_VALID", "1"); monkeypatch.setenv("US_PREP_CONTRACT_OK", "1"); monkeypatch.setenv("US_BALANCE_AVAILABLE", "1"); monkeypatch.setenv("US_MAX_ORDER_USD", "100000")
    client = CashUnparseableClient()
    result = order_router.route_order(_intent("AAPL", "BUY", 1, 100), total_portfolio_usd=100000, kis_client=client)
    assert result["status"] == "BLOCKED"
    assert result["reason"] == "broker_orderable_cash_unavailable"
    assert result["cash_exhausted"] is False
    assert client.buy_calls == 0


def test_daily_report_counts_broker_cash_unavailable(monkeypatch, tmp_path):
    from trader.us.runner import daily_report_runner as drr
    import trader.us.db.repos as repos
    monkeypatch.chdir(tmp_path)
    rows = [{"status": "BLOCKED", "side": "BUY", "symbol": "A", "meta": {"reason": "broker_orderable_cash_unavailable"}}]
    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda td: rows)
    monkeypatch.setattr(repos, "load_positions", lambda as_of=None: [])
    monkeypatch.setattr(drr, "load_us_fills_breakdown", lambda td: {"fills_count": 0})
    monkeypatch.setattr(drr, "load_balance_confirmed_count", lambda td: 0)
    monkeypatch.setattr(drr, "load_router_summary_ack_count", lambda td, session=None: 0)
    out = drr.run_daily_report(session="close", trade_date="2026-07-07", offline=False)
    report = out["report"]
    assert report["orders_blocked"] == 1
    assert report["broker_orderable_cash_blocks"] == 1
    assert report["broker_orderable_cash_unknown_blocks"] == 1
    assert report["cash_exhausted"] is False
