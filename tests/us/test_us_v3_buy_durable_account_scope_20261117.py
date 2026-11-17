"""P0 US BUY original account lineage must exist BEFORE native broker POST.

The fake transaction records the exact SQL and identity requirements without
contacting KIS or modifying an actual DB. Real PG Gate A must run separately.
"""
import json

import pytest

from trader.us.db import repos


class _Result:
    def __init__(self, value=None):
        self.value = value
    def fetchone(self):
        return None
    def scalar_one_or_none(self):
        return self.value


class _Transaction:
    def __init__(self, *, stored_override=None):
        self.statements = []
        self.stored = None
        self.stored_override = stored_override
    def __enter__(self):
        return self
    def __exit__(self, typ, value, trace):
        return False
    def execute(self, sql, params):
        query = str(sql)
        self.statements.append((query, params))
        if "SELECT * FROM us_order_intents" in query:
            return _Result()
        if "INSERT INTO us_order_intents" in query:
            self.stored = dict(params)
            return _Result()
        if "SELECT account_id FROM us_order_intents" in query:
            if self.stored_override is not None:
                return _Result(self.stored_override)
            return _Result(self.stored.get("account_id") if self.stored else None)
        raise AssertionError("unexpected SQL: " + query[:150])


class _Engine:
    def __init__(self, **kw):
        self.conn = _Transaction(**kw)
    def begin(self):
        return self.conn


@pytest.fixture
def setup(monkeypatch):
    account = "a" * 64
    epoch = "us-practice-epoch"
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("WSL_RUN_MARKET", "US")
    monkeypatch.setenv("US_ORDER_ARMED", "1")
    monkeypatch.setattr(repos, "_active_us_epoch", lambda *args, **kw: epoch)
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        "env": "practice", "market": "US", "trading_epoch_id": epoch,
        "account_id": account,
    })
    return account, epoch


def _intent():
    return {
        "trade_date": "2026-11-16",
        "client_order_key": "TQQQ_INF_V3:test-cycle:2026-11-16:BUY",
        "symbol": "TQQQ", "side": "BUY", "exchange": "NASDAQ",
        "strategy_owner": "TQQQ_INFINITE",
        "strategy": "TQQQ_INFINITE_V3",
        "qty": 2, "limit_price": 50, "notional_usd": 100,
        "meta": {"cycle_id": "test-cycle", "strategy_owner": "TQQQ_INFINITE"},
    }


def test_new_native_buy_stores_exact_claim_account_atomically(setup, monkeypatch):
    account, epoch = setup
    engine = _Engine()
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
    assert repos.save_order_intent(_intent(), trade_date="2026-11-16")
    writes = [(sql,params) for sql,params in engine.conn.statements
              if "INSERT INTO us_order_intents" in sql]
    assert len(writes) == 1
    sql, params = writes[0]
    assert "trading_epoch_id, account_id, meta" in sql
    assert ":trading_epoch_id, :account_id" in sql
    assert params["account_id"] == account
    assert params["trading_epoch_id"] == epoch
    meta = json.loads(params["meta"])
    assert meta["broker_account_hash"] == account
    assert meta["strategy_owner"] == "TQQQ_INFINITE"


@pytest.mark.parametrize("scope_patch", [
    {"account_id": None},
    {"env": "real"},
    {"market": "KR"},
    {"trading_epoch_id": "foreign-epoch"},
])
def test_scope_mismatch_aborts_before_durable_insert(setup, monkeypatch, scope_patch):
    account, epoch = setup
    engine = _Engine()
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
    monkeypatch.setattr(repos, "_us_execution_claim_scope", lambda: {
        **{"env": "practice", "market": "US",
           "trading_epoch_id": epoch, "account_id": account},
        **scope_patch,
    })
    assert not repos.save_order_intent(_intent(), trade_date="2026-11-16")
    assert not any("INSERT INTO us_order_intents" in s
                   for s,p in engine.conn.statements)


def test_preexisting_foreign_account_is_not_reused(setup, monkeypatch):
    account, _ = setup
    engine = _Engine()
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
    intent = _intent()
    intent["broker_account_hash"] = "b" * 64
    assert not repos.save_order_intent(intent, trade_date="2026-11-16")
    assert not any("INSERT INTO us_order_intents" in s
                   for s,p in engine.conn.statements)


def test_postwrite_identity_mismatch_blocks_success(setup, monkeypatch):
    engine = _Engine(stored_override="foreign-account")
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
    assert not repos.save_order_intent(_intent(), trade_date="2026-11-16")
    assert any("INSERT INTO us_order_intents" in s for s,p in engine.conn.statements)


def test_offline_unarmed_uses_original_sql_without_new_scope_requirement(
    setup, monkeypatch,
):
    monkeypatch.setenv("US_ORDER_ARMED", "0")
    engine = _Engine()
    monkeypatch.setattr(repos, "_get_engine_or_none", lambda: engine)
    assert repos.save_order_intent(_intent(), trade_date="2026-11-16")
    sql = next(s for s,p in engine.conn.statements if "INSERT INTO us_order_intents" in s)
    assert "trading_epoch_id, account_id, meta" not in sql
