from trader.reconcile_kis import _record_execution_claim_observation


class RecordingOrdersRepo:
    def __init__(self):
        self.observations = []

    def record_execution_claim_for_order(self, client_order_key, **observation):
        self.observations.append((client_order_key, observation))


def test_reconcile_keeps_unknown_fill_unknown():
    repo = RecordingOrdersRepo()

    _record_execution_claim_observation(
        orders_repo=repo,
        source_order={"client_order_key": "order-1"},
        broker_status="PENDING",
        cumulative_filled_qty=None,
        requested_qty=10,
    )

    assert repo.observations == [
        (
            "order-1",
            {
                "state": "ACKED",
                "cumulative_filled_qty": None,
                "authoritative": False,
            },
        )
    ]


def test_reconcile_records_authoritative_partial_fill_and_zero_fill_cancel():
    repo = RecordingOrdersRepo()

    _record_execution_claim_observation(
        orders_repo=repo,
        source_order={"client_order_key": "partial-order"},
        broker_status="RECONCILED",
        cumulative_filled_qty=3,
        requested_qty=10,
    )
    _record_execution_claim_observation(
        orders_repo=repo,
        source_order={"client_order_key": "cancel-order"},
        broker_status="CANCELLED",
        cumulative_filled_qty=0,
        requested_qty=10,
    )

    assert repo.observations == [
        (
            "partial-order",
            {
                "state": "PARTIALLY_FILLED",
                "cumulative_filled_qty": 3,
                "authoritative": True,
            },
        ),
        (
            "cancel-order",
            {
                "state": "CANCELLED",
                "cumulative_filled_qty": 0,
                "authoritative": True,
            },
        ),
    ]
