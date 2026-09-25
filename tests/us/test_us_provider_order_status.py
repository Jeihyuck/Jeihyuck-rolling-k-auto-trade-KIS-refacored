from trader.us.data_provider import normalize_us_order_status_row, USDataProvider


def test_provider_normalizes_unfilled_ack_status():
    r=normalize_us_order_status_row({"odno":"O1","pdno":"AMD","sll_buy_dvsn_cd":"01","ord_qty":"10","ft_ccld_qty":"0","nccs_qty":"10"})
    assert r["status"] == "OPEN" and r["remaining_qty"] == 10


def test_provider_has_order_status_wrappers():
    p=USDataProvider(offline=True)
    assert hasattr(p,"get_today_orders") and hasattr(p,"get_fills_by_order_no")

def test_cancelled_row_preserves_missing_fill_provenance(monkeypatch):
    raw = {
        "odno": "O-MISSING",
        "pdno": "TQQQ",
        "sll_buy_dvsn_cd": "02",
        "ord_qty": "2",
        "nccs_qty": "0",
        "status": "CANCELLED",
    }
    normalized = normalize_us_order_status_row(raw)
    assert normalized["status"] == "CANCELLED"
    assert normalized["filled_qty"] is None
    assert normalized["filled_qty_present"] is False

    provider = USDataProvider(offline=False)
    monkeypatch.setattr(provider, "get_today_orders", lambda _trade_date: [normalized])
    exact = provider.get_fills_by_order_no("O-MISSING", "TQQQ", "2026-09-24")
    assert exact["filled_qty"] is None
    assert exact["cumulative_filled_qty"] is None
    assert exact["filled_qty_present"] is False


def test_cancelled_partial_fill_preserves_explicit_fill_provenance():
    normalized = normalize_us_order_status_row({
        "odno": "O-PARTIAL",
        "pdno": "TQQQ",
        "sll_buy_dvsn_cd": "02",
        "ord_qty": "2",
        "ft_ccld_qty": "1",
        "nccs_qty": "0",
        "status": "CANCELLED",
    })
    assert normalized["status"] == "CANCELLED"
    assert normalized["filled_qty_present"] is True
    assert normalized["filled_qty"] == 1
    assert normalized["remaining_qty"] == 0

