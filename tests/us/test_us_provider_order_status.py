from trader.us.data_provider import normalize_us_order_status_row, USDataProvider


def test_provider_normalizes_unfilled_ack_status():
    r=normalize_us_order_status_row({"odno":"O1","pdno":"AMD","sll_buy_dvsn_cd":"01","ord_qty":"10","ft_ccld_qty":"0","nccs_qty":"10"})
    assert r["status"] == "ACK" and r["remaining_qty"] == 10


def test_provider_has_order_status_wrappers():
    p=USDataProvider(offline=True)
    assert hasattr(p,"get_today_orders") and hasattr(p,"get_fills_by_order_no")
