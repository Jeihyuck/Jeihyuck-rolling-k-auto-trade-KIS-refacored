from datetime import datetime, timezone

from trader.us.infinite.integration import reconcile_tqqq_open_buy_ttl
from trader.us.infinite.models import PositionSnapshot


NOW = datetime(2026, 9, 15, 14, 0, tzinfo=timezone.utc)


def _order():
    return {
        "trade_date": "2026-09-14",
        "client_order_key": "TQQQ_INF_V3:cycle-x:2026-09-14:BUY",
        "order_no": "0000000018",
        "symbol": "TQQQ",
        "side": "BUY",
        "qty_requested": 3,
        "status": "OPEN",
        "meta": {
            "pre_order_holding_qty": 24,
            "pre_order_orderable_qty": 24,
            "pre_order_avg_price": 71.0,
            "pre_order_balance_source": "kis_balance_authoritative",
        },
    }


class Repo:
    def __init__(self):
        self.order = _order()
        self.balance_marks = []
        self.terminal = []

    def load_expired_open_buy_orders(self, **_kwargs):
        return [self.order]

    def apply_ttl_balance_delta(self, order, *, current_qty, current_avg_price, observed_at):
        self.balance_marks.append((current_qty, current_avg_price, observed_at))
        order["status"] = "FILLED"
        order["qty_filled"] = 3
        return {"status": "OK", "order_status": "FILLED", "qty_filled": 3}

    def apply_ttl_terminal_observation(self, order, observation):
        self.terminal.append(dict(observation))
        order["status"] = observation["status"]
        order["qty_filled"] = int(observation.get("filled_qty") or 0)
        return {"status": "OK"}


def _exact_open_zero_fill():
    return {
        # Deliberately unpadded to prove canonical order-number matching.
        "order_no": "18",
        "symbol": "TQQQ",
        "side": "BUY",
        "requested_qty": 3,
        "filled_qty": 0,
        "cumulative_filled_qty": 0,
        "remaining_qty": 3,
        "status": "OPEN",
    }


def test_exact_open_zero_fill_outranks_aggregate_holding_increase():
    repo = Repo()
    cancel_calls = []

    result = reconcile_tqqq_open_buy_ttl(
        repository=repo,
        now=NOW,
        ttl_seconds=120,
        cancel_order=lambda **kwargs: cancel_calls.append(kwargs) or {"status": "ACK"},
        query_order=lambda **_kwargs: _exact_open_zero_fill(),
        # Aggregate TQQQ holding increased by exactly requested qty, which could
        # be a manual/other purchase. It must not override exact order truth.
        broker_position=PositionSnapshot(
            qty=27, orderable_qty=27, average_price=72.0, price=72.0
        ),
        broker_position_authoritative=True,
    )

    assert repo.balance_marks == []
    assert repo.order["status"] == "OPEN"
    assert repo.order.get("qty_filled", 0) == 0
    assert result["balance_skipped_order_specific"] == 1
    assert result.get("balance_confirmed", 0) == 0
    assert result["cancel_requested"] == 1
    assert result["pending"] == 1
    assert len(cancel_calls) == 1


def test_missing_order_specific_quantities_allows_balance_delta_fallback():
    repo = Repo()

    result = reconcile_tqqq_open_buy_ttl(
        repository=repo,
        now=NOW,
        ttl_seconds=120,
        cancel_order=lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("full balance proof should resolve before cancel")
        ),
        query_order=lambda **_kwargs: {
            "order_no": "18", "symbol": "TQQQ", "side": "BUY", "status": "OPEN"
        },
        broker_position=PositionSnapshot(
            qty=27, orderable_qty=27, average_price=72.0, price=72.0
        ),
        broker_position_authoritative=True,
    )

    assert len(repo.balance_marks) == 1
    assert repo.order["status"] == "FILLED"
    assert repo.order["qty_filled"] == 3
    assert result["balance_confirmed"] == 1
    assert result["terminal"] == 1
