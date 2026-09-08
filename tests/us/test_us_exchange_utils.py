from trader.us.execution.exchange_utils import lookup_nested_exchange, normalize_exchange_code


def test_exchange_utils_normalize_exchange_code():
    assert normalize_exchange_code("nas") == "NASDAQ"
    assert normalize_exchange_code("NYS") == "NYSE"
    assert normalize_exchange_code("ase") == "AMEX"
    assert normalize_exchange_code("lse") == "LSE"


def test_exchange_utils_lookup_nested_exchange():
    assert lookup_nested_exchange({"exchange": "nas"}) == "NASDAQ"
    assert lookup_nested_exchange({"ovrs_excg_cd": "NYS"}) == "NYSE"
    assert lookup_nested_exchange({"market": "ase"}) == "AMEX"
    assert lookup_nested_exchange("nope") == ""
