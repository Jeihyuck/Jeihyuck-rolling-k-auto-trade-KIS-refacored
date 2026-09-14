from datetime import datetime, timedelta, timezone

import pytest
import sqlalchemy as sa

from trader.db.repos import OrdersRepo
from trader.db.schema import schema_for_engine


def _repo():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine, OrdersRepo(engine)


def _create(repo: OrdersRepo, *, key: str, status: str = "CREATED", request: dict | None = None):
    return repo.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1,
        code="000660", market="KOSPI", side="BUY", ord_type="LIMIT", qty=1,
        limit_price=1_700_000, stage="PB1-ENTRY", client_order_key=key,
        request_json=request or {}, status=status, account_id="acct",
    )


def test_enforced_buy_contract_is_read_back_with_cycle_and_policy():
    engine, repo = _repo()
    _create(repo, key="contract-ok", request={
        "enforce_entry_contract": True,
        "entry_exit_plan": {"policy_version": "pb1_entry_exit_plan_v1", "exit_policy_family": "SWING_STAGED_EXIT"},
        "pre_order_holding_qty": 0,
        "requested_qty": 1,
        "submitted_qty": 1,
        "balance_snapshot_id": "balance-1",
    })
    row = repo.get_order_by_client_order_key("practice", "contract-ok")
    persisted = row["request_json"]
    assert persisted["entry_contract_version"] == "kr_buy_entry_contract_v1"
    assert persisted["entry_contract_sha256"]
    assert persisted["position_cycle_id"] == str(row["position_cycle_id"])
    assert persisted["portfolio_epoch_id"] == str(row["portfolio_epoch_id"])


def test_enforced_buy_contract_blocks_missing_reconciliation_baseline():
    _, repo = _repo()
    with pytest.raises(RuntimeError, match="pre_order_holding_qty"):
        _create(repo, key="contract-missing", request={
            "enforce_entry_contract": True,
            "entry_exit_plan": {"policy_version": "pb1_entry_exit_plan_v1"},
            "requested_qty": 1,
            "submitted_qty": 1,
        })


def test_stale_repair_does_not_expire_broker_evidence_rows():
    engine, repo = _repo()
    _create(repo, key="created-old", status="CREATED")
    _create(repo, key="acked-old", status="ACKED")
    schema = schema_for_engine(engine)
    old = datetime.now(timezone.utc) - timedelta(days=2)
    with engine.begin() as conn:
        conn.execute(sa.update(schema.orders).values(created_at=old))
    assert repo.expire_stale_open_orders("practice", before_dt=datetime.now(timezone.utc) - timedelta(days=1)) == 1
    assert repo.get_order_by_client_order_key("practice", "created-old")["status"] == "EXPIRED"
    assert repo.get_order_by_client_order_key("practice", "acked-old")["status"] == "ACKED"
