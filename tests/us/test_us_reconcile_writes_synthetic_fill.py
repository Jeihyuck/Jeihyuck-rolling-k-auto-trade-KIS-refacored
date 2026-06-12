from __future__ import annotations


def test_sell_reconcile_writes_idempotent_synthetic_fill(monkeypatch) -> None:
    import trader.us.db.repos as repos

    monkeypatch.delenv("PBCORE_DB_URL", raising=False)
    repos.reset_memory_stores()
    repos.save_order_ack({
        "trade_date": "2026-06-12",
        "symbol": "VRT",
        "exchange": "NYSE",
        "side": "SELL",
        "order_no": "S1",
        "client_order_key": "CKS1",
        "qty_requested": 6,
        "avg_price_usd": 303.6016,
        "status": "ACK",
        "meta": {"cost_basis_price_usd": 320.4960},
    }, trade_date="2026-06-12")

    for _ in range(2):
        repos.mark_order_filled_by_reconcile(
            order_no="S1",
            client_order_key="CKS1",
            symbol="VRT",
            side="SELL",
            filled_qty=6,
            avg_price_usd=303.6016,
            source="balance_reconcile_sell",
            trade_date="2026-06-12",
            meta={"cost_basis_price_usd": 320.4960},
        )

    fills = repos.load_today_fills("2026-06-12")
    assert len(fills) == 1
    assert fills[0]["symbol"] == "VRT"
    assert fills[0]["side"] == "SELL"
    assert fills[0]["qty"] == 6
    assert "VRT" in repos.load_today_symbols_sold("2026-06-12")
