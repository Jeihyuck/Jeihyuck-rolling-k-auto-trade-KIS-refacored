from __future__ import annotations


def test_kis_final_balance_overrides_stale_db_positions(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    from trader.us.runner import daily_report_runner as dr
    import trader.us.db.repos as repos

    kis_positions = [{"symbol": f"K{i:02d}", "qty": 1, "current_price": 10.0, "market_value_usd": 10.0} for i in range(30)]
    db_positions = [{"symbol": f"K{i:02d}", "qty": 1, "current_price": 10.0, "market_value_usd": 10.0} for i in range(30)] + [
        {"symbol": "APP", "qty": 1, "current_price": 10.0, "market_value_usd": 10.0},
        {"symbol": "BE", "qty": 1, "current_price": 10.0, "market_value_usd": 10.0},
        {"symbol": "IBM", "qty": 1, "current_price": 10.0, "market_value_usd": 10.0},
    ]

    monkeypatch.setattr(dr, "_load_close_final_balance_positions", lambda trade_date: kis_positions)
    monkeypatch.setattr(repos, "load_locked_us_watchlist", lambda *a, **k: [])
    monkeypatch.setattr(repos, "load_us_prep_status", lambda *a, **k: None)
    monkeypatch.setattr(repos, "load_us_daily_orders_for_report", lambda *a, **k: [])
    monkeypatch.setattr(repos, "load_positions", lambda as_of=None: db_positions)
    monkeypatch.setattr(dr, "load_us_fills_breakdown", lambda trade_date: {"fills_count": 0})
    monkeypatch.setattr(dr, "load_balance_confirmed_count", lambda trade_date: 0)
    monkeypatch.setattr(dr, "load_router_summary_ack_count", lambda trade_date, session=None: 0)
    monkeypatch.setattr(dr, "load_schedule_health_fallback", lambda trade_date, session=None: {})

    result = dr.run_daily_report(env="practice", session="close", trade_date="2026-07-15", offline=False)
    report = result["report"]
    assert report["canonical_position_source"] == "kis_final_balance"
    assert report["open_position_count"] == 30
    assert report["invested_market_value_usd"] > 0
    assert not {"APP", "BE", "IBM"} & set(report["open_position_symbols"])
