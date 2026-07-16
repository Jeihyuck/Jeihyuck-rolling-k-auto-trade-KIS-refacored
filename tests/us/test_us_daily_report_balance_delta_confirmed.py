from __future__ import annotations


def test_close_balance_delta_confirmed_updates_reconcile_counts(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    from trader.us.runner import daily_report_runner as dr
    import trader.us.db.repos as repos
    import trader.us.execution.reconcile as reconcile
    import trader.us.data_provider as data_provider

    orders = [{"symbol": f"S{i}", "side": "SELL", "status": "ACK", "qty": 1} for i in range(3)]
    monkeypatch.setattr(repos, "load_locked_us_watchlist", lambda *a, **k: [])
    monkeypatch.setattr(repos, "load_us_prep_status", lambda *a, **k: None)
    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda *a, **k: orders)
    monkeypatch.setattr(repos, "load_positions", lambda as_of=None: [])
    monkeypatch.setattr(dr, "_load_close_final_balance_positions", lambda trade_date: [])
    monkeypatch.setattr(dr, "load_us_fills_breakdown", lambda trade_date: {"fills_count": 0})
    monkeypatch.setattr(dr, "load_balance_confirmed_count", lambda trade_date: 0)
    monkeypatch.setattr(dr, "load_router_summary_ack_count", lambda trade_date, session=None: 3)
    monkeypatch.setattr(dr, "load_schedule_health_fallback", lambda trade_date, session=None: {})
    monkeypatch.setattr(data_provider, "USDataProvider", lambda offline=False: object())
    monkeypatch.setattr(reconcile, "classify_ack_orders_with_final_balance", lambda **kwargs: {"orders": [], "counts": {"balance_delta_confirmed": 3}, "pending_order_count": 0})

    report = dr.run_daily_report(env="practice", session="close", trade_date="2026-07-15", offline=False)["report"]
    assert report["balance_delta_confirmed"] == 3
    assert report["balance_confirmed_count"] == 3
    assert report["ack_only_unresolved"] == 0
