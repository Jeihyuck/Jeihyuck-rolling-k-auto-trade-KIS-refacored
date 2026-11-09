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


def test_order_fill_reconciliation_uses_sell_fill_not_buy_cost_basis():
    from trader.kr.infinite.reconciliation import reconcile_order_fill_prices

    result = reconcile_order_fill_prices(
        order_price=72085, order_qty=6, broker_order_no="0000017474",
        fill_price=72090, fill_qty=6,
        broker_avg_after=67345.454, close_avg_price=67345.454,
    )
    assert result["reconciliation_status"] == "OK"
    assert result["warning"] is None
    assert result["price_comparison_basis"] == "PERSISTED_LIMIT_VS_KIS_EXECUTION"


def test_order_fill_reconciliation_without_priced_fill_is_unverified():
    from trader.kr.infinite.reconciliation import reconcile_order_fill_prices

    result = reconcile_order_fill_prices(
        order_price=72085, order_qty=6, broker_order_no="0000017474",
        fill_price=None, fill_qty=None,
        broker_avg_after=67345.454, close_avg_price=67345.454,
    )
    assert result["reconciliation_status"] == "KR_INF_ORDER_FILL_PRICE_UNVERIFIED"
    assert result["warning"] == "KR_INF_ORDER_FILL_PRICE_UNVERIFIED"


def test_infinite_intent_keeps_immutable_limit_price():
    from datetime import date
    from trader.kr.infinite.models import OrderIntent

    intent = OrderIntent(id=1, cycle_id="cycle", trade_date=date(2026, 10, 27),
                         side="SELL_ALL", idempotency_key="key", requested_qty=6,
                         limit_price=72085)
    assert intent.limit_price == 72085


def test_infinite_market_sell_does_not_compare_indicative_or_buy_cost_price():
    from trader.kr.infinite.reconciliation import reconcile_order_fill_prices

    result = reconcile_order_fill_prices(
        order_price=72085, order_qty=6, broker_order_no="0000017474",
        fill_price=67500, fill_qty=6,
        broker_avg_after=67345.454, close_avg_price=67345.454,
        order_side="SELL_ALL", order_is_market=True,
    )
    assert result["reconciliation_status"] == "OK"
    assert result["price_comparison_basis"] == "MARKET_ORDER_NATIVE_FILL_ONLY"
    assert result["price_mismatch_pct"] == 0.0


def test_infinite_market_sell_still_requires_native_fill_price():
    from trader.kr.infinite.reconciliation import reconcile_order_fill_prices

    result = reconcile_order_fill_prices(
        order_price=72085, order_qty=6, broker_order_no="0000017474",
        fill_price=None, fill_qty=6, order_side="SELL_PARTIAL", order_is_market=True,
    )
    assert result["reconciliation_status"] == "KR_INF_ORDER_FILL_PRICE_UNVERIFIED"


def test_infinite_limit_buy_favorable_execution_is_not_price_mismatch():
    from trader.kr.infinite.reconciliation import reconcile_order_fill_prices

    result = reconcile_order_fill_prices(
        order_price=72000, order_qty=11,
        fill_price=70000, fill_qty=11, order_side="BUY",
    )
    assert result["reconciliation_status"] == "OK"
    assert result["price_mismatch_pct"] == 0.0


def test_infinite_limit_buy_adverse_execution_is_price_mismatch():
    from trader.kr.infinite.reconciliation import reconcile_order_fill_prices

    result = reconcile_order_fill_prices(
        order_price=70000, order_qty=11,
        fill_price=71000, fill_qty=11, order_side="BUY",
    )
    assert result["reconciliation_status"] == "KR_INF_ORDER_FILL_PRICE_MISMATCH"
    assert result["price_mismatch_pct"] > 0.01
