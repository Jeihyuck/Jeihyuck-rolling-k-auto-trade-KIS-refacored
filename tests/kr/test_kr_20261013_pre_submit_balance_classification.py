"""No phantom UNRESOLVED_ACK when KR SELL is blocked by balance precheck.

This is a pre-HTTP-only classification. A lost ACK *after* the broker POST
remains ambiguous and must keep its durable execution fence.
"""
import inspect

import pytest

from trader.kis_wrapper import (
    KisAPI,
    KisBalanceUnavailable,
    KisOrderOutcomeUnknown,
    KisOrderPreSubmitBlocked,
    is_kr_order_submit_outcome_ambiguous,
)


def test_kr_sell_balance_budget_failure_is_proven_before_order_http():
    api = KisAPI.__new__(KisAPI)
    def precheck():
        raise KisBalanceUnavailable("KR_BALANCE_BUDGET_EXHAUSTED_BEFORE_FETCH")
    api.get_positions = precheck
    api._order_cash = lambda *_args, **_kwargs: pytest.fail("broker POST must not execute")

    with pytest.raises(
        KisOrderPreSubmitBlocked,
        match="KR_SELL_PRECHECK_PROVEN_BEFORE_ORDER_HTTP",
    ) as exc:
        api.sell_stock_market("036540", 140)
    assert "KR_BALANCE_BUDGET_EXHAUSTED_BEFORE_FETCH" in str(exc.value)
    assert is_kr_order_submit_outcome_ambiguous(exc.value) is False


def test_kr_post_http_lost_ack_still_remains_ambiguous():
    assert is_kr_order_submit_outcome_ambiguous(
        KisOrderOutcomeUnknown("broker order crossed HTTP, ACK lost")
    ) is True


def test_wrapper_has_proven_pre_submit_location_only():
    source = inspect.getsource(KisAPI.sell_stock_market)
    assert source.index("pos = self.get_positions()") < source.index("self._order_cash(")
    assert "except KisBalanceUnavailable as exc:" in source
    assert "raise KisOrderPreSubmitBlocked(" in source
