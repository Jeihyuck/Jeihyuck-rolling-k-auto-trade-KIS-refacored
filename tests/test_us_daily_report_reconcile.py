from trader.us.runner import daily_report_runner as drr
from trader.us.db import repos

def test_daily_report_reconcile_sources():
    r=drr.reconcile_order_sources(db_orders=7, fills=4, balance_confirmed=7, router_summary=7)
    assert r['orders_ack']==7
    assert r['fill_api_count']==4
    assert r['balance_confirmed_count']==7
    assert 'FILL_API_LESS_THAN_ACK' in r['warnings']

def test_daily_report_uses_real_sources_without_env(monkeypatch, tmp_path):
    monkeypatch.delenv('US_DAILY_BALANCE_CONFIRMED_COUNT', raising=False)
    monkeypatch.delenv('US_DAILY_ROUTER_ACK_COUNT', raising=False)
    monkeypatch.setattr(repos, 'load_us_daily_orders_for_report', lambda td: [{'status':'ACK'} for _ in range(7)])
    monkeypatch.setattr(drr, 'load_us_fills_count', lambda td: 4)
    monkeypatch.setattr(drr, 'load_balance_confirmed_count', lambda td: 7)
    monkeypatch.setattr(drr, 'load_router_summary_ack_count', lambda td, session=None: 7)
    monkeypatch.setattr(drr, 'load_us_prep_status', lambda td: None, raising=False)
    monkeypatch.chdir(tmp_path)
    result=drr.run_daily_report(env='practice', session='close', trade_date='2026-06-16', offline=False)
    report=result['report']
    assert report['orders_ack']==7
    assert report['fill_api_count']==4
    assert report['balance_confirmed_count']==7
    assert 'FILL_API_LESS_THAN_ACK' in report['warnings']

def test_load_us_daily_orders_for_report_uses_timestamp_fallback(monkeypatch):
    calls=[]
    class Conn:
        def execution_options(self, **kwargs): return self
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, sql, params):
            text=str(sql)
            calls.append((text, params))
            if 'created_at >=' in text and 'us_orders' in text:
                return [{'status':'ACK','symbol':'AAPL'}]
            return []
    class Engine:
        def connect(self): return Conn()
    monkeypatch.setattr(repos, '_get_engine_or_none', lambda: Engine())
    rows=repos.load_us_daily_orders_for_report('2026-06-16')
    assert len(rows)==1
    assert any('created_at >=' in sql for sql,_ in calls)
    assert any('start_ts' in params and 'end_ts' in params for _,params in calls)

def test_daily_report_order_source_empty_warning(monkeypatch, tmp_path):
    monkeypatch.setattr(repos, 'load_us_daily_orders_for_report', lambda td: [])
    monkeypatch.setattr(drr, 'load_us_fills_count', lambda td: 0)
    monkeypatch.setattr(drr, 'load_balance_confirmed_count', lambda td: 0)
    monkeypatch.setattr(drr, 'load_router_summary_ack_count', lambda td, session=None: 0)
    monkeypatch.chdir(tmp_path)
    result=drr.run_daily_report(env='practice', session='close', trade_date='2026-06-16', offline=False)
    assert 'ORDER_SOURCE_EMPTY' in result['report']['warnings']

def test_daily_report_does_not_count_balance_confirmed_as_broker_sent(monkeypatch, tmp_path):
    monkeypatch.setattr(repos, 'load_us_daily_orders_for_report', lambda td: [
        {'status': 'ACK', 'side': 'BUY'},
        {'status': 'ACK', 'side': 'SELL'},
        {'status': 'BALANCE_CONFIRMED', 'side': 'BUY'},
        {'status': 'BALANCE_CONFIRMED', 'side': 'SELL'},
    ])
    monkeypatch.setattr(drr, 'load_us_fills_breakdown', lambda td: {
        'fills_count': 2,
        'real_broker_buys': 1,
        'real_broker_sells': 1,
        'synthetic_reconcile_buys': 0,
        'synthetic_reconcile_sells': 0,
    })
    monkeypatch.setattr(drr, 'load_balance_confirmed_count', lambda td: 2)
    monkeypatch.setattr(drr, 'load_router_summary_ack_count', lambda td, session=None: 2)
    monkeypatch.chdir(tmp_path)

    report = drr.run_daily_report(env='practice', session='close', trade_date='2026-06-29', offline=False)['report']

    assert report['orders_ack_total'] == 2
    assert report['orders_balance_confirmed_total'] == 2
    assert report['orders_sent_total'] == 2
    assert report['orders_ack'] == 2

def test_daily_report_fallback_counts_partial_cancel_in_raw_ack_population(monkeypatch, tmp_path):
    from trader.us.execution import order_journal

    partial_cancel = {
        "status": "CANCELLED",
        "side": "BUY",
        "symbol": "TQQQ",
        "qty_requested": 2,
        "qty_filled": 1,
        "order_no": "PARTIAL-CANCEL",
        "client_order_key": "PARTIAL-CANCEL-KEY",
        "meta": {
            "broker_raw_row": {
                "filled_qty_raw_present": True,
                "filled_qty_present": True,
                "filled_qty": 1,
                "cumulative_filled_qty": 1,
                "remaining_qty": 0,
            }
        },
    }
    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda _td: [partial_cancel])
    monkeypatch.setattr(order_journal, "aggregate_order_events", lambda _td: {
        "orders_sent_total": 0,
        "orders_ack_total": 0,
    })
    monkeypatch.setattr(drr, "load_us_fills_breakdown", lambda _td: {
        "fills_count": 1,
        "real_broker_buys": 1,
        "real_broker_sells": 0,
        "synthetic_reconcile_buys": 0,
        "synthetic_reconcile_sells": 0,
        "real_broker_buy_notional": 100.0,
        "real_broker_sell_notional": 0.0,
    })
    monkeypatch.setattr(drr, "load_balance_confirmed_count", lambda _td: 0)
    monkeypatch.setattr(drr, "load_router_summary_ack_count", lambda _td, session=None: 1)
    monkeypatch.chdir(tmp_path)

    report = drr.run_daily_report(
        env="practice", session="close", trade_date="2026-09-24", offline=False,
    )["report"]

    assert report["orders_ack_total"] == 1
    assert report["orders_cancelled_total"] == 1
    assert report["orders_partial_fill_cancelled_total"] == 1
    assert report["orders_zero_fill_cancelled_total"] == 0
    assert report["source_numbers"]["db_orders"] == 1
    assert report["source_numbers"]["db_orders_active"] == 1
    assert report["canonical_sources"]["source_counts"]["db_orders"] == 1
    assert report["canonical_sources"]["source_counts"]["kis_fills_inquire_ccnl"] == 1
    assert "db_orders_fills_mismatch" not in report["canonical_sources"]["inconsistencies"]

def test_close_session_router_count_not_reduced_by_am_zero_fill_cancel(monkeypatch, tmp_path):
    from trader.us.execution import order_journal

    rows = [
        {
            "status": "CANCELLED", "side": "BUY", "symbol": "A",
            "qty_filled": 0, "client_order_key": "AM-CANCEL",
            "meta": {"broker_raw_row": {
                "filled_qty_present": True, "filled_qty": 0, "remaining_qty": 0,
            }},
        },
        {"status": "FILLED", "side": "BUY", "symbol": "B", "qty_filled": 1, "client_order_key": "CLOSE-1", "meta": {}},
        {"status": "FILLED", "side": "SELL", "symbol": "C", "qty_filled": 1, "client_order_key": "CLOSE-2", "meta": {}},
    ]
    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda _td: rows)
    monkeypatch.setattr(order_journal, "aggregate_order_events", lambda _td: {
        "orders_sent_total": 0, "orders_ack_total": 0,
    })
    monkeypatch.setattr(drr, "load_us_fills_breakdown", lambda _td: {
        "fills_count": 2,
        "real_broker_buys": 1,
        "real_broker_sells": 1,
        "synthetic_reconcile_buys": 0,
        "synthetic_reconcile_sells": 0,
        "real_broker_buy_notional": 100.0,
        "real_broker_sell_notional": 100.0,
    })
    monkeypatch.setattr(drr, "load_balance_confirmed_count", lambda _td: 0)
    monkeypatch.setattr(
        drr, "load_router_summary_ack_count",
        lambda _td, session=None: drr._ScopedAckCount(2, scope="session", source_path="test-close.json"),
    )
    monkeypatch.chdir(tmp_path)

    report = drr.run_daily_report(
        env="practice", session="close", trade_date="2026-09-24", offline=False,
    )["report"]

    assert report["source_numbers"]["db_orders"] == 3
    assert report["source_numbers"]["db_orders_active"] == 2
    assert report["source_numbers"]["router_session_summary"] == 2
    assert report["source_numbers"]["router_session_summary_active"] == 2
    assert report["source_numbers"]["router_summary_scope"] == "session"
    assert report["source_numbers"]["router_summary_used_for_daily_compare"] == 0
    assert report["canonical_sources"]["source_counts"]["db_orders"] == 2
    assert report["canonical_sources"]["source_counts"]["kis_fills_inquire_ccnl"] == 2
    assert report["canonical_sources"]["source_counts"]["router_session_summary"] == 0
    assert "db_orders_fills_mismatch" not in report["canonical_sources"]["inconsistencies"]

def test_router_latest_fallback_is_daily_scoped_even_for_close_session(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "runtime" / "us" / "order_router_summary" / "2026-09-24"
    path.mkdir(parents=True)
    (path / "latest.json").write_text(
        '{"orders_ack": 3}',
        encoding="utf-8",
    )

    loaded = drr.load_router_summary_ack_count("2026-09-24", session="close")

    assert int(loaded) == 3
    assert loaded.scope == "daily"
    assert loaded.source_path.endswith("/2026-09-24/latest.json")


def test_router_exact_session_artifact_is_session_scoped(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "runtime" / "us" / "order_router_summary" / "2026-09-24"
    path.mkdir(parents=True)
    (path / "close.json").write_text(
        '{"orders_ack": 2}',
        encoding="utf-8",
    )

    loaded = drr.load_router_summary_ack_count("2026-09-24", session="close")

    assert int(loaded) == 2
    assert loaded.scope == "session"
    assert loaded.source_path.endswith("/2026-09-24/close.json")


def test_schedule_health_aggregate_fallback_reports_daily_scope(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "reports" / "us_schedule_health"
    path.mkdir(parents=True)
    (path / "2026-09-24.json").write_text(
        '{"aggregate":{"orders_ack":4}}',
        encoding="utf-8",
    )

    loaded = drr.load_schedule_health_fallback("2026-09-24", session="close")

    assert loaded["orders_ack"] == 4
    assert loaded["_scope"] == "daily"
    assert loaded["_source_key"] == "aggregate"


def test_close_report_uses_daily_latest_router_count_for_daily_compare(monkeypatch, tmp_path):
    from trader.us.execution import order_journal

    monkeypatch.chdir(tmp_path)
    router_path = tmp_path / "runtime" / "us" / "order_router_summary" / "2026-09-24"
    router_path.mkdir(parents=True)
    (router_path / "latest.json").write_text('{"orders_ack":1}', encoding="utf-8")

    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda _td: [
        {"status": "FILLED", "side": "BUY", "symbol": "AAPL", "qty_filled": 1, "meta": {}},
    ])
    monkeypatch.setattr(order_journal, "aggregate_order_events", lambda _td: {
        "orders_sent_total": 0, "orders_ack_total": 0,
    })
    monkeypatch.setattr(drr, "load_us_fills_breakdown", lambda _td: {
        "fills_count": 1,
        "real_broker_buys": 1,
        "real_broker_sells": 0,
        "synthetic_reconcile_buys": 0,
        "synthetic_reconcile_sells": 0,
        "real_broker_buy_notional": 100.0,
        "real_broker_sell_notional": 0.0,
    })
    monkeypatch.setattr(drr, "load_balance_confirmed_count", lambda _td: 0)

    report = drr.run_daily_report(
        env="practice", session="close", trade_date="2026-09-24", offline=False,
    )["report"]

    assert report["source_numbers"]["router_summary_scope"] == "daily"
    assert report["source_numbers"]["router_summary_used_for_daily_compare"] == 1
    assert report["canonical_sources"]["source_counts"]["router_session_summary"] == 1
    assert "db_orders_fills_mismatch" not in report["canonical_sources"]["inconsistencies"]

def test_daily_router_zero_fill_cancel_is_adjusted_before_daily_compare(monkeypatch, tmp_path):
    from trader.us.execution import order_journal

    monkeypatch.chdir(tmp_path)
    router_path = tmp_path / "runtime" / "us" / "order_router_summary" / "2026-09-24"
    router_path.mkdir(parents=True)
    (router_path / "latest.json").write_text('{"orders_ack":6}', encoding="utf-8")

    rows = [{
        "status": "CANCELLED",
        "side": "BUY",
        "symbol": "TQQQ",
        "qty_requested": 2,
        "qty_filled": 0,
        "client_order_key": "ZERO-CANCEL",
        "meta": {"broker_raw_row": {
            "filled_qty_present": True,
            "filled_qty": 0,
            "remaining_qty": 0,
        }},
    }]
    rows.extend({
        "status": "FILLED",
        "side": "BUY",
        "symbol": f"S{i}",
        "qty_requested": 1,
        "qty_filled": 1,
        "client_order_key": f"FILL-{i}",
        "meta": {},
    } for i in range(5))

    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda _td: rows)
    monkeypatch.setattr(order_journal, "aggregate_order_events", lambda _td: {
        "orders_sent_total": 0,
        "orders_ack_total": 0,
    })
    monkeypatch.setattr(drr, "load_us_fills_breakdown", lambda _td: {
        "fills_count": 5,
        "real_broker_buys": 5,
        "real_broker_sells": 0,
        "synthetic_reconcile_buys": 0,
        "synthetic_reconcile_sells": 0,
        "real_broker_buy_notional": 500.0,
        "real_broker_sell_notional": 0.0,
    })
    monkeypatch.setattr(drr, "load_balance_confirmed_count", lambda _td: 0)

    report = drr.run_daily_report(
        env="practice", session="close", trade_date="2026-09-24", offline=False,
    )["report"]

    assert report["source_numbers"]["router_summary_scope"] == "daily"
    assert report["source_numbers"]["router_session_summary"] == 6
    assert report["source_numbers"]["router_session_summary_active"] == 5
    assert report["source_numbers"]["router_summary_used_for_daily_compare"] == 5
    assert report["source_numbers"]["db_orders"] == 6
    assert report["source_numbers"]["db_orders_active"] == 5
    assert report["canonical_sources"]["source_counts"]["db_orders"] == 5
    assert report["canonical_sources"]["source_counts"]["router_session_summary"] == 5
    assert report["canonical_sources"]["source_counts"]["kis_fills_inquire_ccnl"] == 5
    assert "db_orders_router_summary_mismatch" not in report["canonical_sources"]["inconsistencies"]
    assert "db_orders_fills_mismatch" not in report["canonical_sources"]["inconsistencies"]

