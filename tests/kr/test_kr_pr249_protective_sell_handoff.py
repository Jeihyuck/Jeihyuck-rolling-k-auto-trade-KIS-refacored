"""PR249 SELL-before-historical reconciliation: prove exact KIS/ledger truth.

No live KIS orders or production DB. The only optimization is a read-only
handoff; when proof is incomplete the old blocking reconciliation remains.
"""
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
import sqlalchemy as sa

from trader.db.schema import schema_for_engine
import trader.kr.protective_sell_handoff as gate


KST = ZoneInfo("Asia/Seoul")
NOW = datetime(2026, 10, 22, 9, 37, tzinfo=KST)


class FakeOrderRepo:
    def __init__(self, *_args, **_kwargs):
        self.open = []
        self.health = {"available": True, "in_flight": 0,
                       "uncertain": 0, "unresolved": 0}

    def get_open_orders(self, *args, **kwargs):
        return self.open

    def execution_claim_health(self, *args, **kwargs):
        return self.health


class FakePositionRepo:
    def __init__(self, *_args, **_kwargs):
        pass

    def list_active_positions_for_exit(self, *args, **kwargs):
        return [{
            "code": "053800", "status": "OPEN", "strategy": "pb1_pullback_close",
            "qty": 12, "avg_buy_price": 90200.0,
            "position_cycle_id": "bb24815a-f713-49ab-8a05-bde33c9d8a53",
            "exit_policy_family": "SWING_STAGED_EXIT",
            "entry_meta_json": {"entry_style_selected": "ENTRY_PULLBACK"},
            "entry_exit_plan_json": {"exit_policy_family": "SWING_STAGED_EXIT"},
        }]


class KISReadOnly:
    def __init__(self):
        self.rows = []
        self.calls = 0

    def inquire_daily_ccld(self, **kwargs):
        self.calls += 1
        return {"rt_cd": "0", "output1": self.rows, "output2": []}

    def __getattr__(self, name):
        if any(x in name.lower() for x in ("buy", "sell", "submit", "cancel")):
            raise AssertionError("KIS order submission forbidden in preflight proof")
        raise AttributeError(name)


@pytest.fixture()
def proof_fixture(monkeypatch):
    monkeypatch.setattr(gate, "now_kst", lambda: NOW)
    fake_orders = FakeOrderRepo()
    monkeypatch.setattr(gate, "OrdersRepo", lambda *_: fake_orders)
    fake_positions = FakePositionRepo()
    monkeypatch.setattr(gate, "PositionsRepo", lambda *_: fake_positions)
    monkeypatch.setattr(gate, "get_account_key", lambda **_: "scoped-test-account")
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    kis = KISReadOnly()
    balance = {
        "output1": [{"pdno": "053800", "hldg_qty": "12"},
                    {"pdno": "122630", "hldg_qty": "81"}],
        "_kr_verified_fresh_at_kst": NOW.isoformat(),
    }
    def prove():
        return gate.prove_safe_protective_sell_handoff(
            engine=engine, kis=kis, env="practice",
            strategy="pb1_pullback_close", trade_date=date(2026, 10, 22),
            balance_snapshot=balance,
        )
    yield prove, balance, fake_orders, fake_positions, kis, engine
    engine.dispose()


def test_clean_day_allows_protective_sell_before_full_historical_repair(proof_fixture):
    prove,_,_,_,kis,_=proof_fixture
    assert prove() == (True, "EXACT_BROKER_LEDGER_PRETRADE_PROOF")
    assert kis.calls == 1


def test_expired_balance_or_unresolved_claim_falls_back_to_old_order(proof_fixture):
    prove,balance,orders,_,_,_=proof_fixture
    balance["_kr_verified_fresh_at_kst"]=(NOW-timedelta(seconds=30)).isoformat()
    assert prove()[0] is False
    balance["_kr_verified_fresh_at_kst"]=NOW.isoformat()
    orders.health["unresolved"] = 1
    assert prove() == (False,"UNRESOLVED_EXECUTION_CLAIM")


def test_old_unknown_order_and_missing_cycle_block_handoff(proof_fixture):
    prove,balance,orders,positions,_,_=proof_fixture
    orders.open = [{"status":"UNRESOLVED_ACK","code":"053800"}]
    assert prove() == (False,"OPEN_OR_UNRESOLVED_ORDER")
    orders.open = []
    positions.list_active_positions_for_exit = lambda *a, **k: []
    assert prove() == (False,"KIS_LEDGER_SYMBOL_MISMATCH")


def test_unmatched_kis_execution_forces_full_reconciliation(proof_fixture):
    prove,_,_,_,kis,_=proof_fixture
    kis.rows=[{"pdno":"053800","sll_buy_dvsn_cd":"01","ord_dt":"20261022",
               "ord_tmd":"093722","odno":"0000003319",
               "ord_qty":"12","tot_ccld_qty":"5","tot_ccld_amt":"450000"}]
    assert prove() == (False,"BROKER_DURABLE_ORDER_SET_MISMATCH")


def test_broker_pagination_failure_is_not_a_clean_day(proof_fixture):
    prove,_,_,_,kis,_=proof_fixture
    kis.inquire_daily_ccld=lambda **_: {"rt_cd":"1", "output1":[]}
    eligible, reason=prove()
    assert eligible is False and reason.startswith("PROOF_READ_FAILED_")


def test_different_market_or_owner_never_adopts_pb1_handoff(proof_fixture):
    _,balance,_,_,kis,engine=proof_fixture
    for env,strategy in (("real","pb1_pullback_close"),
                         ("practice","KR_INFINITE")):
        assert gate.prove_safe_protective_sell_handoff(
            engine=engine,kis=kis,env=env,strategy=strategy,
            trade_date=date(2026,10,22),balance_snapshot=balance
        )==(False,"SCOPE_NOT_VALIDATED")


def test_engine_source_has_post_sell_buy_fence_and_owner_handoff():
    from inspect import getsource
    from trader.pb1_engine import PB1Engine
    from trader import pb1_runner
    engine_source=getsource(PB1Engine.run)
    runner_source=getsource(pb1_runner.run_once)
    assert 'getattr(self, "_kr_post_exit_reconcile_ok", True)' in engine_source
    assert "KR_POST_EXIT_BROKER_RECONCILE_UNVERIFIED" in engine_source
    assert runner_source.index("prove_safe_protective_sell_handoff(") < runner_source.index("reconcile_kis(")
    assert "deferred_full_reconcile" in runner_source
    assert "and getattr(engine_runner, \"_kr_post_exit_reconcile_ok\", True)" in runner_source


def test_existing_durable_order_uses_portfolio_epoch_account_not_nonexistent_order_account_id(proof_fixture):
    """Actual current schema: account identity lives in portfolio_epochs."""
    from uuid import uuid4
    import sqlalchemy as sa

    prove, _, _, _, kis, engine = proof_fixture
    schema = schema_for_engine(engine)
    epoch_id = str(uuid4())
    trading_epoch_id = str(uuid4())
    with engine.begin() as conn:
        conn.execute(schema.portfolio_epochs.insert().values(
            portfolio_epoch_id=epoch_id, trading_epoch_id=trading_epoch_id,
            env="practice",
            account_id="scoped-test-account", sid=1, mode=1,
            strategy="pb1_pullback_close", status="ACTIVE",
        ))
        conn.execute(schema.orders.insert().values(
            order_id=str(uuid4()), portfolio_epoch_id=epoch_id,
            trading_epoch_id=trading_epoch_id,
            env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
            code="053800", market="J", side="BUY", ord_type="LIMIT", qty=1,
            client_order_key="pr249-proof-existing-kis-order", status="REJECTED",
            kis_odno="0000001234", response_json={"confirmed_fill_qty": 0},
            created_at=NOW,
        ))
    kis.rows = [{
        "pdno": "053800", "side": "BUY", "ord_dt": "20261022",
        "ord_tmd": "093700", "odno": "0000001234", "ord_qty": "1",
        "tot_ccld_qty": "0", "tot_ccld_amt": "0",
    }]
    assert prove() == (True, "EXACT_BROKER_LEDGER_PRETRADE_PROOF")
    with engine.begin() as conn:
        conn.execute(sa.update(schema.portfolio_epochs)
                     .where(schema.portfolio_epochs.c.portfolio_epoch_id == epoch_id)
                     .values(account_id="different-account"))
    assert prove() == (False, "DURABLE_ORDER_ACCOUNT_IDENTITY_MISSING")
    with engine.begin() as conn:
        conn.execute(sa.update(schema.portfolio_epochs)
                     .where(schema.portfolio_epochs.c.portfolio_epoch_id == epoch_id)
                     .values(account_id="scoped-test-account",
                             trading_epoch_id=str(uuid4())))
    assert prove() == (False, "DURABLE_ORDER_ACCOUNT_IDENTITY_MISSING")
