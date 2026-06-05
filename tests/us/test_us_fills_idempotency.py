from __future__ import annotations


def test_save_fills_skips_duplicate_rows_in_memory(monkeypatch) -> None:
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_FILLS.clear()
    fills = [{
        "symbol": "CIEN",
        "exchange": "NYSE",
        "side": "SELL",
        "qty": 4,
        "price_usd": 500.6966,
        "order_no": "ORD1",
        "client_order_key": "CK1",
    }]

    inserted_first = repos.save_fills(fills, trade_date="2026-06-05")
    inserted_second = repos.save_fills(fills, trade_date="2026-06-05")

    assert inserted_first == 1
    assert inserted_second == 0
    assert len(repos._MEM_FILLS) == 1
