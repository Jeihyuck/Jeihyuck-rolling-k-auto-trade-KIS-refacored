from trader.us.utils.order_no import normalize_us_order_no


def test_normalize_us_order_no():
    assert normalize_us_order_no("0000031806") == "31806"
    assert normalize_us_order_no("31806") == "31806"
    assert normalize_us_order_no("0000000000") == "0"
    assert normalize_us_order_no(None) == ""
