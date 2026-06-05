from __future__ import annotations

import inspect


def test_mark_order_filled_by_reconcile_updates_avg_price_usd_in_source() -> None:
    import trader.us.db.repos as repos

    source = inspect.getsource(repos.mark_order_filled_by_reconcile)
    assert "avg_price_usd = :price" in source
    assert "avg_fill_price = :price" not in source


def test_mark_order_filled_by_reconcile_in_memory_uses_avg_price_usd(monkeypatch) -> None:
    import trader.us.db.repos as repos

    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: None)
    repos._MEM_ORDERS.clear()
    repos._MEM_ORDERS.append({
        "trade_date": "2026-06-05",
        "order_no": "A1",
        "client_order_key": "CK1",
        "status": "ACK",
        "qty_filled": 0,
    })

    repos.mark_order_filled_by_reconcile(
        order_no="A1",
        client_order_key="CK1",
        filled_qty=4,
        avg_price_usd=123.45,
    )

    saved = repos._MEM_ORDERS[0]
    assert saved["qty_filled"] == 4
    assert saved["avg_price_usd"] == 123.45
    assert "avg_fill_price" not in saved
