from trader.kr.pb1.ownership import enforce_kr_order_ownership
from trader.pb1_engine import enforce_kr_order_ownership as facade_enforce_kr_order_ownership


def test_reserved_symbol_fence_handles_a_prefix_and_facade():
    expected = (False, "KR_INF_OWNERSHIP_RESERVED")
    assert enforce_kr_order_ownership("A122630", "KR_STANDARD") == expected
    assert facade_enforce_kr_order_ownership("A122630", "KR_STANDARD") == expected


def test_infinite_owner_is_allowed_through_module_and_facade():
    expected = (True, None)
    assert enforce_kr_order_ownership("122630", "KR_INFINITE") == expected
    assert facade_enforce_kr_order_ownership("122630", "KR_INFINITE") == expected
