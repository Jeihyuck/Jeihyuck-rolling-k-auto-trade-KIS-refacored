from trader.kr.forbidden_products import is_forbidden_kr_product, block_buy_if_forbidden

def test_forbidden_buy_names_blocked_sell_allowed():
    for name in ["KODEX 인버스", "선물인버스2X", "곱버스 ETN"]:
        assert block_buy_if_forbidden({"side":"BUY","name":name})["status"] == "BLOCKED"
    assert is_forbidden_kr_product(name="KODEX 200") is False
    assert "status" not in block_buy_if_forbidden({"side":"SELL","name":"KODEX 인버스"})
