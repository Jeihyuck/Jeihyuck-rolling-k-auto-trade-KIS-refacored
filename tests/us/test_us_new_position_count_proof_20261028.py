"""New-position counts must come from real broker fill + order join."""
from trader.us.runner.new_position_count_proof import verified_new_positions


def _audit(symbol, order, action="NEW_POSITION_BUY"):
    return {"symbol": symbol, "side": "BUY",
            "canonical_order_no": order, "position_action": action}


def _fill(symbol, order, qty=1, **extra):
    return {"symbol": symbol, "side": "BUY",
            "order_no": order, "qty": qty, **extra}


def test_20261027_eight_new_positions_are_eight_not_report_default_zero():
    symbols = ["DHR", "TMO", "GILD", "ABBV", "KO", "CL", "KR", "SYY"]
    orders = [_audit(s, f"{i:06d}") for i, s in enumerate(symbols, 1)]
    fills = [_fill(s, str(i)) for i, s in enumerate(symbols, 1)]
    proof = verified_new_positions(orders, fills)
    assert proof["actual_new_positions"] == 8
    assert proof["status"] == "VERIFIED"
    assert proof["kis_buy_fill_rows"] == 8


def test_missing_order_identity_is_partial_not_false_verified_zero():
    proof = verified_new_positions([], [_fill("SYY", "123", 41)])
    assert proof["actual_new_positions"] == 0
    assert proof["status"] == "PARTIAL_UNVERIFIED"
    assert proof["unmatched_buy_fill_rows"] == 1


def test_add_to_existing_does_not_count_as_new():
    proof = verified_new_positions(
        [_audit("DHR", "10", "ADD_TO_EXISTING_BUY")],
        [_fill("DHR", "00010")],
    )
    assert proof["actual_new_positions"] == 0
    assert proof["status"] == "VERIFIED"


def test_synthetic_unfilled_and_sell_are_not_counted():
    proof = verified_new_positions(
        [_audit("SYY", "20")],
        [_fill("SYY", "20", 0), _fill("SYY", "20", 41, is_synthetic=True),
         {"symbol": "SYY", "order_no": "20", "side": "SELL", "qty": 41}],
    )
    assert proof["actual_new_positions"] == 0
    assert proof["kis_buy_fill_rows"] == 0


def test_changed_order_number_does_not_match_by_symbol_alone():
    proof = verified_new_positions([_audit("SYY", "42")], [_fill("SYY", "41", 41)])
    assert proof["actual_new_positions"] == 0
    assert proof["status"] == "PARTIAL_UNVERIFIED"


def test_duplicate_audit_identity_is_unverified():
    proof = verified_new_positions(
        [_audit("SYY", "41"), _audit("SYY", "00041")], [_fill("SYY", "41", 41)]
    )
    assert proof["status"] == "PARTIAL_UNVERIFIED"


def test_nonfinite_quantity_is_not_confirmed():
    proof = verified_new_positions([_audit("SYY", "41")], [_fill("SYY", "41", float("nan"))])
    assert proof["kis_buy_fill_rows"] == 0
