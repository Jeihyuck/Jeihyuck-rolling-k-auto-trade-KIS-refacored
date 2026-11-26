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


@pytest.fixture(params=[("003490", 66, 29250, "PULLBACK"), ("012750", 18, 102300, "MOMENTUM")])
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
