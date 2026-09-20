import pytest

from trader.us.data_provider import normalize_us_order_status_row, USDataProvider


def test_provider_normalizes_unfilled_ack_status():
    r=normalize_us_order_status_row({"odno":"O1","pdno":"AMD","sll_buy_dvsn_cd":"01","ord_qty":"10","ft_ccld_qty":"0","nccs_qty":"10"})
    assert r["status"] == "OPEN" and r["remaining_qty"] == 10


def test_provider_has_order_status_wrappers():
    p=USDataProvider(offline=True)
    assert hasattr(p,"get_today_orders") and hasattr(p,"get_fills_by_order_no")


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("ord_qty", "10.5", "invalid_requested_qty"),
        ("ft_ccld_qty", "bad", "invalid_filled_qty"),
        ("nccs_qty", "bad", "invalid_remaining_qty"),
        ("nccs_qty", "0.5", "invalid_remaining_qty"),
    ],
)
def test_provider_quarantines_malformed_quantity_evidence(field, value, reason):
    raw = {
        "odno": "O1",
        "pdno": "AMD",
        "sll_buy_dvsn_cd": "01",
        "ord_qty": "10",
        "ft_ccld_qty": "0",
        "nccs_qty": "10",
    }
    raw[field] = value

    row = normalize_us_order_status_row(raw)

    assert row["normalization_result"] == "quarantined"
    assert row["filter_reason"] == reason
