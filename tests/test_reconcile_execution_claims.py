from trader.reconcile_kis import (
    _execution_claim_health,
    _record_execution_claim_observation,
    reconcile_today,
)
from trader.execution_claims import DurableExecutionClaimRepo
from trader.execution_state import SemanticActionIdentity
from trader.db.schema import schema_for_engine
from trader.run_context import RunContext
import sqlalchemy as sa
from datetime import date, datetime


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


def test_reconcile_cancel_without_fill_quantity_preserves_unknown_and_uncertainty():
    repo = RecordingOrdersRepo()

    _record_execution_claim_observation(
        orders_repo=repo,
        source_order={"client_order_key": "cancel-unknown"},
        broker_status="CANCELLED",
        cumulative_filled_qty=None,
        requested_qty=10,
    )

    assert repo.observations == [
        (
            "cancel-unknown",
            {
                "state": "CANCELLED",
                "cumulative_filled_qty": None,
                "authoritative": False,
            },
        )
    ]


def test_claim_ledger_failure_is_exposed_as_unavailable_health():
    class UnavailableOrdersRepo:
        def execution_claim_health(self):
            raise RuntimeError("ledger unavailable")

    assert _execution_claim_health(UnavailableOrdersRepo()) == {
        "available": False,
        "error": "RuntimeError",
    }


def test_reconcile_observation_persistence_failure_degrades_health_and_keeps_claim_fenced(
    monkeypatch,
):
    from trader import reconcile_kis

    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(
        engine, tables=[schema.execution_claims, schema.execution_attempts],
    )
    claims = DurableExecutionClaimRepo(
        engine, schema.execution_claims, schema.execution_attempts,
    )
    identity = SemanticActionIdentity(
        env="practice",
        account_id="test-account",
        market="KR",
        trading_epoch_id="test-epoch",
        strategy_owner="PB1",
        lifecycle_id="test-cycle",
        action="SELL:TP1",
        trade_date=date(2026, 10, 2),
    )
    claim = claims.acquire(
        identity, attempt_id="attempt-1", requested_qty=5,
        client_order_key="pb1-sell-1",
    )
    assert claim.acquired
    source_order = {
        "client_order_key": "pb1-sell-1",
        "order_id": "db-order-1",
        "qty": 5,
        "side": "SELL",
        "strategy": "PB1",
        "market": "KRX",
        "request_json": {},
        "response_json": {},
    }

    class OrdersRepoWithClaimFailure:
        def get_order_by_kis_odno(self, _env, _order_no):
            return source_order

        def upsert_reconciled_order(self, **_kwargs):
            return None

        def record_execution_claim_for_order(self, _key, **_kwargs):
            raise OSError("execution claim ledger write failed")

        def execution_claim_health(self):
            return claims.health()

    class NoopRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def __getattr__(self, _name):
            return lambda **_kwargs: None

    monkeypatch.setattr(reconcile_kis, "_resolve_reconcile_run_id", lambda *_args: "run")
    monkeypatch.setattr(reconcile_kis, "now_kst", lambda: datetime(2026, 10, 2, 12))
    monkeypatch.setattr(reconcile_kis, "OrdersRepo", lambda _engine: OrdersRepoWithClaimFailure())
    monkeypatch.setattr(reconcile_kis, "FillsRepo", NoopRepo)
    monkeypatch.setattr(reconcile_kis, "PositionsRepo", NoopRepo)
    monkeypatch.setattr(reconcile_kis, "LedgerEventsRepo", NoopRepo)
    monkeypatch.setattr(reconcile_kis, "ReconcileLogRepo", NoopRepo)

    class FakeKis:
        def inquire_daily_ccld(self, **_kwargs):
            return {
                "output1": [{
                    "pdno": "123456",
                    "side": "SELL",
                    "ord_qty": "5",
                    "odno": "broker-1",
                    "ord_stat_cd": "CANCELLED",
                }]
            }

    ctx = RunContext(
        run_id="run",
        env="practice",
        strategy="pb1_pullback_close",
        started_at=datetime(2026, 10, 2, 12),
        dry_run=False,
    )
    result = reconcile_today(engine=engine, kis=FakeKis(), ctx=ctx)

    assert result["ok"] is False
    assert result["degraded"] == "execution_claim_observation_failed"
    assert result["execution_claim_observation_failures"] == 1
    assert result["execution_claim_health"]["integrity_status"] == "DEGRADED"
    assert claims.get(identity).action_state == "IN_FLIGHT"
    assert not claims.acquire(
        identity, attempt_id="attempt-2", requested_qty=5,
        fresh_validation=True, client_order_key="pb1-sell-2",
    ).acquired
