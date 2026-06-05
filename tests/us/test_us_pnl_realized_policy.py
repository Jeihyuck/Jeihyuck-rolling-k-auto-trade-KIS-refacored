from __future__ import annotations


def test_kis_raw_realized_not_used_as_strategy_realized(monkeypatch, tmp_path) -> None:
    from scripts.generate_us_portfolio_pnl_report import generate_us_pnl_report

    monkeypatch.setattr(
        "scripts.generate_us_portfolio_pnl_report._load_engine",
        lambda: object(),
    )
    monkeypatch.setattr(
        "scripts.generate_us_portfolio_pnl_report._load_db_positions",
        lambda engine, trade_date: [{
            "symbol": "CIEN",
            "qty": 1,
            "avg_price": 10.0,
            "last_price": 11.0,
            "market_value_usd": 11.0,
            "cost_usd": 10.0,
        }],
    )
    monkeypatch.setattr(
        "scripts.generate_us_portfolio_pnl_report._load_db_fills",
        lambda engine, trade_date: [{"symbol": "CIEN", "side": "SELL", "qty": 1, "price_usd": 11.0}],
    )
    monkeypatch.setattr(
        "scripts.generate_us_portfolio_pnl_report._load_kis_balance",
        lambda env: {"positions": [], "summary": {"ovrs_rlzt_pfls_amt": -305444.85}},
    )
    monkeypatch.setattr(
        "scripts.generate_us_portfolio_pnl_report._load_latest_daily_report",
        lambda path: {"orders_sent_total": 1},
    )

    result = generate_us_pnl_report(
        session="am",
        env="practice",
        trade_date="2026-06-05",
        output_dir=str(tmp_path / "pnl"),
    )

    assert result["kis_account_realized_pnl_raw"] == -305444.85
    assert result["realized_pnl_source"] == "unavailable"
    assert result["realized_pnl_usd"] is None
    assert result["total_pnl_usd"] == result["unrealized_pnl_usd"]
