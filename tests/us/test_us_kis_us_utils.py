from trader.us.execution.kis_us_utils import endpoint_label, extract_input_field_name, resolve_us_dailyprice_bymd


def test_kis_us_utils_endpoint_label():
    assert endpoint_label("GET", "/foo/order") == "GET_order"
    assert endpoint_label("POST", "/foo/order") == "POST_order"
    assert endpoint_label("GET", "/foo/bar") == "GET_bar"


def test_kis_us_utils_extract_input_field_name():
    assert extract_input_field_name("something INPUT_FIELD_NAME 'FOO': missing") == "FOO"
    assert extract_input_field_name("no marker here") == ""


def test_kis_us_utils_resolve_us_dailyprice_bymd():
    assert resolve_us_dailyprice_bymd("2026-05-29") == "20260529"
    assert resolve_us_dailyprice_bymd("20260529") == "20260529"
