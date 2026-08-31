from trader.execution_state import OrderState, reconcile_balance_delta


def test_buy_balance_delta_confirms_fill():
    result = reconcile_balance_delta(side="BUY", pre_qty=14, post_qty=21, submitted_qty=7)
    assert result.state is OrderState.FILLED
    assert result.confirmed_fill_qty == 7


def test_sell_balance_delta_confirms_fill_and_remaining_position():
    result = reconcile_balance_delta(side="SELL", pre_qty=21, post_qty=7, submitted_qty=14)
    assert result.state is OrderState.FILLED
    assert result.confirmed_fill_qty == 14
    assert 21 - result.confirmed_fill_qty == 7


def test_ack_is_not_fill_without_proven_delta():
    result = reconcile_balance_delta(side="BUY", pre_qty=14, post_qty=14, submitted_qty=7)
    assert result.state is OrderState.UNRESOLVED_ACK
    assert result.confirmed_fill_qty == 0


def test_impossible_delta_is_reconcile_error():
    result = reconcile_balance_delta(side="BUY", pre_qty=14, post_qty=22, submitted_qty=7)
    assert result.state is OrderState.RECONCILE_ERROR
