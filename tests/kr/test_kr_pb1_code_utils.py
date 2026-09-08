from trader.kr.pb1.code_utils import _is_kis_balance_authoritative_empty, _is_kr_stock_code
from trader.pb1_engine import _is_kis_balance_authoritative_empty as facade_is_kis_balance_authoritative_empty
from trader.pb1_engine import _is_kr_stock_code as facade_is_kr_stock_code


def test_code_utils_module_matches_facade():
    for code, expected in [("028050", True), ("AAPL", False), ("12345", False), (None, False)]:
        assert _is_kr_stock_code(code) is expected
        assert facade_is_kr_stock_code(code) is expected


def test_balance_authoritative_empty_module_matches_facade():
    balance_snapshot = {"rt_cd": "0", "output1": [], "output2": {}}
    assert _is_kis_balance_authoritative_empty(balance_snapshot) is True
    assert facade_is_kis_balance_authoritative_empty(balance_snapshot) is True
    assert _is_kis_balance_authoritative_empty(None) is False
    assert facade_is_kis_balance_authoritative_empty(None) is False
