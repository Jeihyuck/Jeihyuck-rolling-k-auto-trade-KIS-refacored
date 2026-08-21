from trader.pb1_engine import enforce_kr_order_ownership


def test_standard_buy_and_sell_owners_are_rejected():
    for owner in ("KR_STANDARD", "PB1", "SWING_BOOK", ""):
        assert enforce_kr_order_ownership("122630", owner) == (False, "KR_INF_OWNERSHIP_RESERVED")


def test_infinite_owner_is_the_only_allowed_owner():
    assert enforce_kr_order_ownership("122630", "KR_INFINITE") == (True, None)
