"""11/26 KR BUY contract readback regression; no KIS or production DB I/O."""
import copy
import json
from types import MappingProxyType

import pytest
import sqlalchemy as sa

from trader.db.repos import (
    KR_BUY_ENTRY_CONTRACT_VERSION,
    _assert_kr_buy_intent_readback,
    _kr_buy_entry_contract_hash,
)
from trader.kr.us_standard_profit_parity import build_new_swing_parity_contract


@pytest.fixture(params=[("003490", 66, 29250, "PULLBACK"), ("012750", 19, 102300, "MOMENTUM")])
def contract(monkeypatch, request):
    code, qty, price, style = request.param
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("KR_SWING_US_PROFIT_MODE", "ACTIVE_PRACTICE")
    parity = build_new_swing_parity_contract(
        strategy_owner="PB1", exit_policy_family="SWING_STAGED_EXIT",
        trade_horizon="SWING", position_action="NEW_POSITION_BUY",
        account_id="test-practice", trading_epoch_id="test-epoch",
        position_cycle_id="test-cycle", runner_version="US_V3", code=code,
    )
    assert parity is not None
    payload = {
        "entry_exit_plan": {"exit_policy_family": "SWING_STAGED_EXIT",
                            "trade_horizon": "SWING", "entry_style": style},
        "pre_order_holding_qty": 0, "requested_qty": qty, "submitted_qty": qty,
        "balance_snapshot_id": "test-balance",
        "client_order_key": "test-key-" + code,
        "position_cycle_id": "test-cycle", "portfolio_epoch_id": "test-portfolio",
        "trading_epoch_id": "test-epoch",
        "entry_contract_version": KR_BUY_ENTRY_CONTRACT_VERSION,
        "kr_us_profit_contract": parity,
        "entry_meta": {"kr_us_profit_contract": copy.deepcopy(parity)},
        "limit_price": price,
    }
    payload["entry_contract_sha256"] = _kr_buy_entry_contract_hash(payload)
    return payload


@pytest.mark.parametrize("representation", ["object", "mapping", "text"])
def test_valid_readback(contract, representation):
    persisted = {"object": copy.deepcopy(contract),
                 "mapping": MappingProxyType(copy.deepcopy(contract)),
                 "text": json.dumps(contract)}[representation]
    before = copy.deepcopy(contract)
    _assert_kr_buy_intent_readback(persisted, contract)
    assert contract == before


@pytest.mark.parametrize("bad", [None, "null", "[]", "42", "{", [], 123, '"{}"'])
def test_invalid_top_level_fails_closed(contract, bad):
    with pytest.raises(RuntimeError, match="KR_BUY_ENTRY_CONTRACT_NOT_JSON"):
        _assert_kr_buy_intent_readback(bad, contract)


@pytest.mark.parametrize("field", ["kr_us_profit_contract", "entry_meta"])
@pytest.mark.parametrize("bad", [None, [], "not-an-object", {}])
def test_wrong_nested_shape_fails_closed(contract, field, bad):
    persisted = copy.deepcopy(contract)
    persisted[field] = bad
    with pytest.raises(RuntimeError, match="KR_PROFIT_PARITY_BUY_PERSISTENCE_MISMATCH"):
        _assert_kr_buy_intent_readback(json.dumps(persisted), contract)


def test_tampered_signed_parity_fails_closed(contract):
    persisted = copy.deepcopy(contract)
    persisted["kr_us_profit_contract"]["code"] = "000000"
    with pytest.raises(RuntimeError, match="KR_PROFIT_PARITY_BUY_PERSISTENCE_MISMATCH"):
        _assert_kr_buy_intent_readback(json.dumps(persisted), contract)


def test_changed_root_even_with_recomputed_hash_fails_closed(contract):
    persisted = copy.deepcopy(contract)
    persisted["requested_qty"] += 1
    persisted["entry_contract_sha256"] = _kr_buy_entry_contract_hash(persisted)
    with pytest.raises(RuntimeError, match="KR_BUY_ENTRY_CONTRACT_PERSISTENCE_MISMATCH"):
        _assert_kr_buy_intent_readback(persisted, contract)


@pytest.mark.parametrize("storage", ["text", "json"])
@pytest.mark.parametrize("tampered", [False, True])
def test_sqlalchemy_readback_inside_transaction(contract, storage, tampered):
    # Isolated SQLite types exercise driver readback shapes; this is not
    # PostgreSQL or a full PB1 order integration test.
    engine = sa.create_engine("sqlite://")
    meta = sa.MetaData()
    table = sa.Table("intent_fixture", meta,
                     sa.Column("request_json", sa.Text if storage == "text" else sa.JSON))
    meta.create_all(engine)
    stored = copy.deepcopy(contract)
    if tampered:
        stored["entry_meta"] = "invalid"
    value = json.dumps(stored) if storage == "text" else stored

    def transaction():
        with engine.begin() as conn:
            conn.execute(table.insert().values(request_json=value))
            persisted = conn.execute(sa.select(table.c.request_json)).scalar_one()
            _assert_kr_buy_intent_readback(persisted, contract)

    if tampered:
        with pytest.raises(RuntimeError, match="KR_PROFIT_PARITY_BUY_PERSISTENCE_MISMATCH"):
            transaction()
    else:
        transaction()
    with engine.connect() as conn:
        assert conn.execute(sa.select(sa.func.count()).select_from(table)).scalar_one() == (0 if tampered else 1)
    engine.dispose()


@pytest.mark.parametrize("storage", ["text", "json", "jsonb"])
@pytest.mark.parametrize("tampered", [False, True])
def test_postgres_driver_readback_and_rollback(contract, storage, tampered):
    """Use only the disposable CI PostgreSQL URL, with a connection-local table."""
    import os
    from sqlalchemy.dialects.postgresql import JSONB

    url = os.getenv("PBCORE_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("Disposable PostgreSQL service not configured")
    engine = sa.create_engine(url)
    if engine.dialect.name != "postgresql":
        engine.dispose()
        pytest.fail("PBCORE_TEST_POSTGRES_URL must be PostgreSQL")
    types = {"text": sa.Text(), "json": sa.JSON(), "jsonb": JSONB()}
    table = sa.Table("kr401_readback_fixture", sa.MetaData(),
                     sa.Column("request_json", types[storage]),
                     prefixes=["TEMPORARY"])
    stored = copy.deepcopy(contract)
    if tampered:
        stored["entry_meta"] = "invalid"
    value = json.dumps(stored) if storage == "text" else stored
    try:
        with engine.connect() as conn:
            with conn.begin():
                table.create(conn)
            def transaction():
                with conn.begin():
                    conn.execute(table.insert().values(request_json=value))
                    persisted = conn.execute(sa.select(table.c.request_json)).scalar_one()
                    assert isinstance(persisted, str if storage == "text" else dict)
                    _assert_kr_buy_intent_readback(persisted, contract)
            if tampered:
                with pytest.raises(RuntimeError, match="KR_PROFIT_PARITY_BUY_PERSISTENCE_MISMATCH"):
                    transaction()
            else:
                transaction()
            with conn.begin():
                assert conn.execute(sa.select(sa.func.count()).select_from(table)).scalar_one() == (0 if tampered else 1)
    finally:
        engine.dispose()


# Extracted from 11/26 AM DECISION_LATENCY, ENTRY_PLAN_CHECK and BASELINE.
# These are independent past evaluations, NOT 14 economic orders. Raw full
# minute/quote/DB inputs were not archived; this tests only the repository edge.
INCIDENT_INTENTS = [
    ("10:18:37", "003490", 66, 29250, 29500, "ENTRY_PULLBACK"),
    ("10:24:15", "003490", 66, 29400, 29650, "ENTRY_PULLBACK"),
    ("10:30:01", "003490", 66, 29550, 29750, "ENTRY_PULLBACK"),
    ("10:38:33", "012750", 19, 102300, 102500, "ENTRY_MOMENTUM"),
    ("10:40:37", "003490", 66, 29550, 29750, "ENTRY_PULLBACK"),
    ("10:42:23", "003490", 65, 29650, 29800, "ENTRY_PULLBACK"),
    ("10:44:13", "012750", 19, 102300, 102500, "ENTRY_MOMENTUM"),
    ("10:52:50", "003490", 65, 29700, 29950, "ENTRY_PULLBACK"),
    ("11:07:03", "003490", 65, 29700, 29950, "ENTRY_PULLBACK"),
    ("11:20:43", "012750", 19, 102100, 102500, "ENTRY_MOMENTUM"),
    ("11:37:36", "003490", 65, 29700, 29900, "ENTRY_PULLBACK"),
    ("11:51:58", "003490", 66, 29550, 29800, "ENTRY_PULLBACK"),
    ("11:58:27", "003490", 66, 29500, 29750, "ENTRY_PULLBACK"),
    ("12:25:58", "003490", 66, 29550, 29800, "ENTRY_PULLBACK"),
]


@pytest.mark.parametrize("clock,code,qty,price,cap,style", INCIDENT_INTENTS,
                         ids=[row[0]+"-"+row[1] for row in INCIDENT_INTENTS])
def test_actual_repository_new_swing_intent_and_idempotency(monkeypatch, clock, code, qty, price, cap, style):
    """Reconstructed 14 incident contract edges, not full native signal replay."""
    assert price <= cap
    from uuid import uuid4
    from trader.account_state import get_account_key
    from trader.db.repos import OrdersRepo, PortfolioEpochsRepo
    from tests.kr.execution_claim_fixtures import create_schema_with_active_test_epoch
    from trader.kr.us_standard_profit_parity import verified_contract

    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("KR_SWING_US_PROFIT_MODE", "ACTIVE_PRACTICE")
    engine = sa.create_engine("sqlite:///:memory:")
    try:
        schema = create_schema_with_active_test_epoch(engine)
        account = get_account_key(env="practice")
        portfolio = PortfolioEpochsRepo(engine).get_or_create_active(
            env="practice", account_id=account, strategy="pb1_pullback_close", sid=1, mode=1,
        )
        cycle = str(uuid4())
        key = "kr401-" + uuid4().hex
        plan = {"entry_style_selected": style, "entry_reason": style,
                "exit_policy_family": "SWING_STAGED_EXIT", "trade_horizon": "SWING"}
        req = dict(enforce_entry_contract=True, pre_order_holding_qty=0,
                   requested_qty=qty, submitted_qty=qty, balance_snapshot_id="test-snapshot",
                   entry_exit_plan=plan)
        args = dict(env="practice", run_id=None, strategy="pb1_pullback_close",
                    sid=1, mode=1, code=code, market="KOSPI", side="BUY",
                    ord_type="LIMIT", qty=qty, limit_price=price, stage="PB1-AM",
                    client_order_key=key, request_json=req, account_id=account,
                    portfolio_epoch_id=portfolio, position_cycle_id=cycle)
        orders = OrdersRepo(engine)
        order_id, created = orders.create_intent_idempotent(**args)
        assert created
        with engine.connect() as conn:
            frozen = conn.execute(sa.select(schema.orders.c.request_json).where(
                schema.orders.c.client_order_key == key)).scalar_one()
        if isinstance(frozen, str):
            frozen = json.loads(frozen)
        parity = frozen["kr_us_profit_contract"]
        assert verified_contract(parity)
        assert frozen["entry_meta"]["kr_us_profit_contract"] == parity
        assert parity["code"] == code
        assert frozen["position_cycle_id"] == cycle
        assert frozen["entry_contract_sha256"] == _kr_buy_entry_contract_hash(frozen)
        assert OrdersRepo(engine).create_intent_idempotent(**args) == (order_id, False)
        changed = dict(args, qty=qty + 1, request_json={
            **req, "requested_qty": qty + 1, "submitted_qty": qty + 1,
        })
        with pytest.raises(RuntimeError, match="KR_BUY_ENTRY_CONTRACT_PERSISTENCE_MISMATCH"):
            OrdersRepo(engine).create_intent_idempotent(**changed)
        with engine.connect() as conn:
            assert conn.execute(sa.select(sa.func.count()).select_from(schema.orders).where(
                schema.orders.c.client_order_key == key)).scalar_one() == 1
    finally:
        engine.dispose()
