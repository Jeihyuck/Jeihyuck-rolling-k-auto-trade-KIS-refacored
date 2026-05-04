from trader.pb1_engine import PB1Engine


def test_kis_holding_authoritative_gate_blocks_buy(monkeypatch):
    engine = PB1Engine.__new__(PB1Engine)
    engine._display_code = lambda code: code

    decision = PB1Engine._evaluate_unified_buyable_gate(
        engine,
        code="066970",
        gate_context={
            "holding_qty": 0,
            "kis_holding_qty": 7,
            "today_buy_exists": False,
            "today_submit_exists": False,
            "today_fill_exists": False,
            "today_sell_exists": False,
            "open_order_exists": False,
            "cooldown_active": False,
            "blocking_duplicate_exists": False,
        },
        allow_add_to_existing=False,
    )

    assert decision.ok is False
    assert "BUYABLE_EXISTING_HOLDING_KIS" in decision.reason_codes