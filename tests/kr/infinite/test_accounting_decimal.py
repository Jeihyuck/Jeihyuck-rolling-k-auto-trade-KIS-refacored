from decimal import Decimal


def test_validate_invariants_accepts_decimal_capital():
    from trader.kr.infinite.accounting import validate_invariants
    from trader.kr.infinite.models import State

    state = State(allocated_capital_krw=Decimal("100.00"), core_filled_notional=Decimal("100.00"))
    validate_invariants(state, 1)


def test_order_fill_price_reconciliation_flags_one_percent_mismatch():
    from trader.kr.infinite.reconciliation import reconcile_order_fill_prices

    result = reconcile_order_fill_prices(order_price=110500, order_qty=1,
                                         broker_order_no="OID", fill_price=98000,
                                         fill_qty=1, broker_avg_after=98000,
                                         close_avg_price=98000)
    assert result["reconciliation_status"] == "KR_INF_ORDER_FILL_PRICE_MISMATCH"
    assert result["price_mismatch_pct"] >= 0.01
