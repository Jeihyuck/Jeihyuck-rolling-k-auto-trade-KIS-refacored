"""Offline 2026-10-19 KR PB1 claim repair regressions.

These tests use only SQLite. In particular, an unverified CREATED/EXPIRED or
broker-ambiguous order must retain its durable execution fence.
"""
from __future__ import annotations

from datetime import date, datetime
from uuid import uuid4

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import OrdersRepo, _proven_kr_pb1_before_kis_request
from trader.db.schema import schema_for_engine


PROVEN_BEFORE_KIS = {
    "rt_cd": "LOCAL_SUBMIT_FAILURE",
    "msg_cd": "KisTemporaryError",
    "msg1": "KR_TICK_DEADLINE_EXHAUSTED_BEFORE_KIS_REQUEST",
    "broker_submit": False,
}


def _setup():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    epoch_id = str(uuid4())
    with engine.begin() as conn:
        conn.execute(
            schema.trading_epochs.insert().values(
                trading_epoch_id=epoch_id,
                env="practice",
                account_id=get_account_key(env="practice"),
                status="ACTIVE",
                reason="test-only active epoch",
            )
        )
    return engine, schema, OrdersRepo(engine), epoch_id


def _claim(
    engine, schema, repo, epoch, *,
    code="251970",
    stage="PB1-AM",
    owner="PB1",
    trade_date=date(2026, 10, 19),
    order_status="ERROR",
    response=PROVEN_BEFORE_KIS,
    submitted_at=None,
    kis_odno=None,
    order_epoch=None,
):
    key = f"test:{owner}:{code}:{stage}:{uuid4()}"
    identity, claim = repo.claim_execution_action(
        env="practice",
        market="KR",
        strategy_owner=owner,
        lifecycle_id=f"test-lifecycle:{code}:{uuid4()}",
        action=f"BUY_ENTRY:{stage}",
        trade_date=trade_date,
        attempt_id=f"attempt:{uuid4()}",
        requested_qty=5,
        client_order_key=key,
        fresh_validation=True,
        retry_action_prefix=f"BUY_ENTRY:{stage}",
        entry_generation=True,
    )
    assert claim.acquired
    with engine.begin() as conn:
        conn.execute(
            schema.orders.insert().values(
                order_id=str(uuid4()),
                trading_epoch_id=order_epoch if order_epoch is not None else epoch,
                env="practice",
                run_id=None,
                strategy="pb1_pullback_close",
                sid=1,
                mode=1,
                code=code,
                market="KOSDAQ",
                side="BUY",
                ord_type="LIMIT",
                qty=5,
                limit_price=10000.0,
                stage=stage,
                client_order_key=key,
                status=order_status,
                request_json={},
                response_json=response,
                submitted_at=submitted_at,
                kis_odno=kis_odno,
            )
        )
    return identity, key


def test_before_kis_request_proof_requires_explicit_non_submit_evidence():
    assert _proven_kr_pb1_before_kis_request(
        {"status": "ERROR", "response_json": PROVEN_BEFORE_KIS}
    )
    for order in [
        {"status": "EXPIRED", "response_json": PROVEN_BEFORE_KIS},
        {"status": "CREATED", "response_json": PROVEN_BEFORE_KIS},
        {"status": "ERROR", "response_json": {"broker_submit": False}},
        {"status": "ERROR", "response_json": {
            **PROVEN_BEFORE_KIS, "msg1": "transport connection lost",
        }},
        {"status": "ERROR", "response_json": {
            **PROVEN_BEFORE_KIS, "broker_submit": True,
        }},
        {"status": "ERROR", "response_json": PROVEN_BEFORE_KIS, "kis_odno": "001"},
        {"status": "ERROR", "response_json": PROVEN_BEFORE_KIS, "submitted_at": "2026-10-19"},
        {"status": "ERROR", "response_json": 'invalid-json'},
    ]:
        assert not _proven_kr_pb1_before_kis_request(order)


def test_oct_19_kr_pb1_claim_releases_once_using_hashed_account_scope():
    engine, schema, repo, epoch = _setup()
    identity, key = _claim(engine, schema, repo, epoch)
    assert repo._execution_claim_repo.get(identity).action_state == "IN_FLIGHT"
    assert repo.execution_claim_health(market="KR")["unresolved_execution_actions"] == 1

    assert repo.recover_proven_kr_pb1_prebroker_claims(
        env="practice", trade_date=date(2026, 10, 19)
    ) == 1
    snapshot = repo._execution_claim_repo.get(identity)
    assert snapshot.action_state == "OPEN"
    assert snapshot.active_attempt_id is None
    assert snapshot.remaining_target_qty == 5
    assert repo.execution_claim_health(market="KR")["unresolved_execution_actions"] == 0
    with engine.connect() as conn:
        attempt_state = conn.execute(
            sa.select(schema.execution_attempts.c.attempt_state).where(
                schema.execution_attempts.c.client_order_key == key
            )
        ).scalar_one()
        order_state = conn.execute(
            sa.select(schema.orders.c.status).where(
                schema.orders.c.client_order_key == key
            )
        ).scalar_one()
    assert attempt_state == "ABANDONED_PRE_SUBMIT"
    assert order_state == "ERROR"
    assert repo.recover_proven_kr_pb1_prebroker_claims(
        env="practice", trade_date=date(2026, 10, 19)
    ) == 0
    engine.dispose()


def test_broker_uncertainty_expired_and_other_owner_are_not_released():
    engine, schema, repo, epoch = _setup()
    identities = [
        _claim(engine, schema, repo, epoch, code="192820", order_status="EXPIRED"),
        _claim(
            engine, schema, repo, epoch, code="096530",
            response={"broker_submit": True, "rt_cd": "UNRESOLVED_ACK"},
        ),
        _claim(
            engine, schema, repo, epoch, code="007000",
            submitted_at=datetime(2026, 10, 19, 10, 0),
        ),
        _claim(engine, schema, repo, epoch, code="123123", owner="KR_INFINITE"),
        _claim(
            engine, schema, repo, epoch, code="008888",
            trade_date=date(2026, 10, 16),
        ),
        _claim(engine, schema, repo, epoch, code="004444", order_epoch=str(uuid4())),
    ]
    assert repo.recover_proven_kr_pb1_prebroker_claims(
        env="practice", trade_date=date(2026, 10, 19)
    ) == 0
    for identity, _ in identities:
        assert repo._execution_claim_repo.get(identity).action_state == "IN_FLIGHT"
    engine.dispose()



def test_reconcile_tick_releases_only_proven_before_kis_attempt(monkeypatch):
    """The real KR reconcile entrypoint must invoke scoped repair before health."""
    from trader import reconcile_kis as reconcile
    from trader.run_context import RunContext

    engine, schema, repo, epoch = _setup()
    releasable, _ = _claim(engine, schema, repo, epoch, code="251970")
    ambiguous, _ = _claim(
        engine, schema, repo, epoch, code="192820", order_status="EXPIRED"
    )

    class FakeKis:
        def inquire_daily_ccld(self, **_kwargs):
            return {"output1": [], "_ccld_status": "ok"}

    class NoopRepo:
        def __init__(self, *_args, **_kwargs):
            pass

        def __getattr__(self, _name):
            return lambda **_kwargs: None

    monkeypatch.setattr(reconcile, "OrdersRepo", lambda _engine: repo)
    monkeypatch.setattr(reconcile, "FillsRepo", NoopRepo)
    monkeypatch.setattr(reconcile, "PositionsRepo", NoopRepo)
    monkeypatch.setattr(reconcile, "LedgerEventsRepo", NoopRepo)
    monkeypatch.setattr(reconcile, "ReconcileLogRepo", NoopRepo)
    monkeypatch.setattr(reconcile, "_resolve_reconcile_run_id", lambda *_args: "test-run")
    monkeypatch.setattr(
        reconcile, "now_kst", lambda: datetime(2026, 10, 19, 14, 30)
    )
    # Both modules own a clock reference. Leaving the repository on wall
    # time makes this fixed Oct-19 regression fail on every later trade date.
    monkeypatch.setattr(
        "trader.db.repos.now_kst", lambda: datetime(2026, 10, 19, 14, 30)
    )
    result = reconcile.reconcile_today(
        engine=engine,
        kis=FakeKis(),
        ctx=RunContext(
            run_id="test-run",
            env="practice",
            strategy="pb1_pullback_close",
            started_at=datetime(2026, 10, 19, 14, 30),
            dry_run=True,
        ),
    )
    assert result["orders"] == 0
    assert result["fills"] == 0
    assert result["degraded"] == "unresolved_execution_actions"
    assert result["execution_claim_health"]["unresolved_execution_actions"] == 1
    assert repo._execution_claim_repo.get(releasable).action_state == "OPEN"
    assert repo._execution_claim_repo.get(ambiguous).action_state == "IN_FLIGHT"
    engine.dispose()
