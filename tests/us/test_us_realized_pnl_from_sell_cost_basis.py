from __future__ import annotations


def test_realized_pnl_from_sell_fill_cost_basis(monkeypatch, tmp_path) -> None:
    import scripts.generate_us_portfolio_pnl_report as pnl

    monkeypatch.setenv("PBCORE_DB_URL", "postgresql://example/test")
    monkeypatch.setattr(pnl, "_load_engine", lambda: object())
    monkeypatch.setattr(pnl, "_load_kis_balance", lambda env: None)
    monkeypatch.setattr(pnl, "_load_db_positions", lambda engine, trade_date: [{
        "symbol": "AMD", "qty": 1, "avg_price": 100.0, "last_price": 110.0,
    }])
    monkeypatch.setattr(pnl, "_load_db_fills", lambda engine, trade_date: [{
        "symbol": "META",
        "side": "SELL",
        "qty": 2,
        "filled_price": 586.7491,
        "meta": {"cost_basis_price_usd": 621.9260},
    }])

    result = pnl.generate_us_pnl_report(
        session="afternoon",
        env="practice",
        trade_date="2026-06-12",
        output_dir=str(tmp_path),
        latest_daily_report=None,
    )

    assert result["realized_pnl_available"] is True
    assert result["realized_pnl_source"] == "sell_fill_cost_basis"
    assert result["realized_pnl_usd"] == -70.35
