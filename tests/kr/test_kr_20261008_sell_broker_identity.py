"""Regression for the 2026-10-08 LG전자 unknown-ACK false-fill incident."""
from trader.reconcile_kis import _kr_sell_holdings_promotion_unproven


def test_unknown_sell_submit_with_internal_client_key_cannot_promote_holdings_delta():
    order = {
        "side": "SELL",
        "code": "066570",
        "kis_odno": "practice:pb1_pullback_close:2026-10-08:066570:EXIT:SELL:exit:close:1:retry2",
        "broker_order_id": None,
        "client_order_key": "practice:pb1_pullback_close:2026-10-08:066570:EXIT:SELL:exit:close:1:retry2",
    }
    response = {
        "rt_cd": "UNRESOLVED_ACK",
        "msg_cd": "BROKER_SUBMIT_OUTCOME_UNKNOWN",
        "msg1": "KR_BALANCE_BUDGET_EXHAUSTED_BEFORE_FETCH",
        "pre_order_holding_qty": 5,
        "holding_qty": 0,
        "holding_delta": 5,
    }
    # The apparent holdings delta MUST NOT become a confirmed order fill.
    assert _kr_sell_holdings_promotion_unproven(order, response) is True

    # A genuinely identified broker order is eligible for subsequent broker-
    # matched reconciliation, though its fill quantity must still be verified.
    order["broker_order_id"] = "0000006722"
    assert _kr_sell_holdings_promotion_unproven(order, response) is False


def test_confirmed_broker_ack_does_not_change_existing_sell_promotion_policy():
    order = {"side": "SELL", "code": "010120", "kis_odno": "0000006722"}
    response = {"rt_cd": "0", "msg_cd": "40590000", "output": {"ODNO": "0000006722"}}
    assert _kr_sell_holdings_promotion_unproven(order, response) is False


def test_guard_does_not_interfere_with_pb1_buy_or_other_owner_decisions():
    order = {"side": "BUY", "code": "006400", "kis_odno": "internal-key"}
    response = {"rt_cd": "UNRESOLVED_ACK", "msg_cd": "BROKER_SUBMIT_OUTCOME_UNKNOWN"}
    assert _kr_sell_holdings_promotion_unproven(order, response) is False


def test_unknown_sell_stays_unproven_with_no_identifier_or_fake_identifier():
    response = {"msg_cd": "BROKER_SUBMIT_OUTCOME_UNKNOWN"}
    assert _kr_sell_holdings_promotion_unproven(
        {"side": "SELL", "kis_odno": None, "broker_order_id": None}, response,
    )
    assert _kr_sell_holdings_promotion_unproven(
        {"side": "SELL", "kis_odno": "practice:semantic:retry3"}, response,
    )
