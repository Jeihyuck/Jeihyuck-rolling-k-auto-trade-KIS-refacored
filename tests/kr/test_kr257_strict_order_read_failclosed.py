"""P0: strict DB order reads in KR reconciliation must never fail open."""
from datetime import date, datetime
from zoneinfo import ZoneInfo
import inspect
import pytest

from trader import reconcile_kis


class ReadFailure:
    def __init__(self):
        self.reads = 0

    def get_open_orders(self, env, *, trade_date=None, fail_open=None):
        self.reads += 1
        assert env == "practice"
        assert fail_open is False
        raise TimeoutError("simulated DB read timeout")


def test_unresolved_activity_is_true_on_db_timeout_without_infinite_read():
    orders = ReadFailure()
    assert reconcile_kis._has_unresolved_broker_activity(
        engine=object(), orders_repo=orders, env="practice",
        trade_date=date(2026, 10, 26),
    ) is True
    assert orders.reads == 1


def test_holdings_promotion_aborts_before_mutation_when_order_read_fails():
    orders = ReadFailure()
    class NoWrites:
        def __getattr__(self, key):
            raise AssertionError(f"unexpected mutation after DB timeout: {key}")
    with pytest.raises(TimeoutError, match="simulated DB read timeout"):
        reconcile_kis._promote_open_buy_orders_from_holdings(
            env="practice", strategy="pb1_pullback_close", ctx_run_id=None,
            tick_ts=datetime(2026, 10, 26, tzinfo=ZoneInfo("Asia/Seoul")),
            holdings_rows=[], orders_repo=orders,
            fills_repo=NoWrites(), positions_repo=NoWrites(),
        )
    assert orders.reads == 1


def test_original_repo_api_keeps_general_read_policy_but_reconcile_explicitly_blocks():
    from trader.db.repos import OrdersRepo
    assert inspect.signature(OrdersRepo.get_open_orders).parameters["fail_open"].default is None
    assert "fail_open=False" in inspect.getsource(reconcile_kis._has_unresolved_broker_activity)
    assert "fail_open=False" in inspect.getsource(reconcile_kis._promote_open_buy_orders_from_holdings)
