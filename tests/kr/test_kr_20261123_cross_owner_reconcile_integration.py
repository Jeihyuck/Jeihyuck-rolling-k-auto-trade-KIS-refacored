"""End-to-end read-only mock of Nov23 cross-sleeve KIS reconciliation."""
from datetime import date, datetime
from types import SimpleNamespace

import pytest
from trader import reconcile_kis
from trader.run_context import RunContext
from trader.kr import recovery_scope_20261104 as recovery_scope

import sqlalchemy as sa


@pytest.fixture()
def db_fixture():
    engine = sa.create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(sa.text("""
            CREATE TABLE kr_infinite_state (
                strategy_id TEXT, symbol TEXT, cycle_id TEXT,
                trading_epoch_id TEXT, status TEXT
            )
        """))
        conn.execute(sa.text("""
            CREATE TABLE kr_infinite_order_intents (
                id INTEGER PRIMARY KEY, strategy_id TEXT, symbol TEXT,
                cycle_id TEXT, side TEXT, broker_order_id TEXT, trade_date DATE,
                trading_epoch_id TEXT, requested_qty INTEGER,
                filled_qty INTEGER, status TEXT
            )
        """))
        conn.execute(sa.text("""
            INSERT INTO kr_infinite_state
                (strategy_id, symbol, cycle_id, trading_epoch_id, status)
            VALUES ('KR_INFINITE_V1','122630','cycle-1','current-epoch','ACTIVE')
        """))
        conn.execute(sa.text("""
            INSERT INTO kr_infinite_order_intents (
                id, strategy_id, symbol, cycle_id, side, broker_order_id,
                trade_date, trading_epoch_id, requested_qty, filled_qty, status
            ) VALUES (14,'KR_INFINITE_V1','122630','cycle-1','BUY',
                      '0000009745','2026-11-23','current-epoch',15,15,'FILLED')
        """))
    return engine


def _run(monkeypatch, db, *, total="15"):
    class Orders:
        def get_order_by_kis_odno_scoped(self, *args, **kwargs):
            return None

        def execution_claim_health(self, **filters):
            return {
                "unresolved_execution_actions": 0,
                "unresolved_source_date_prior": 0,
                "unresolved_source_date_current": 0,
                "unresolved_source_date_other_or_unknown": 0,
            }

        def recover_proven_kr_pb1_prebroker_claims(self, **kwargs):
            return 0

        def recover_audited_oct13_kr_pb1_prehttp_sells(self, **kwargs):
            return 0

    class Events:
        def append_event(self, **kwargs):
            assert kwargs["event_type"] == "RECONCILE"

    class ReconcileLog:
        def append_log(self, **kwargs):
            assert kwargs["action"] == "reconcile_today"

    class NoopRepo:
        def __init__(self, *args, **kwargs):
            pass

    class Kis:
        env = "practice"

        def inquire_daily_ccld(self, **kwargs):
            assert kwargs["start_date"] == "20261123"
            return {
                "rt_cd": "0",
                "output1": [{
                    "ord_dt": "20261123", "ord_tmd": "093123",
                    "pdno": "122630", "sll_buy_dvsn_cd": "02",
                    "ord_qty": "15", "tot_ccld_qty": total,
                    "odno": "0000009745", "ord_unpr": "47000",
                }],
                "output2": [],
            }

    scope = recovery_scope.KRRecoveryScope(
        env="practice", account_key="practice:broker:01",
        trading_epoch_id="current-epoch",
    )
    monkeypatch.setattr(recovery_scope, "resolve_kr_recovery_scope",
                        lambda **kwargs: scope)
    monkeypatch.setattr(reconcile_kis, "OrdersRepo", lambda engine: Orders())
    monkeypatch.setattr(reconcile_kis, "FillsRepo", NoopRepo)
    monkeypatch.setattr(reconcile_kis, "PositionsRepo", NoopRepo)
    monkeypatch.setattr(reconcile_kis, "LedgerEventsRepo", lambda engine: Events())
    monkeypatch.setattr(reconcile_kis, "ReconcileLogRepo", lambda engine: ReconcileLog())
    monkeypatch.setattr(reconcile_kis, "_resolve_reconcile_run_id",
                        lambda *args: "run")
    monkeypatch.setattr(reconcile_kis, "get_account_key",
                        lambda **kwargs: "practice:broker:01")
    monkeypatch.setattr(reconcile_kis, "active_trading_epoch_id",
                        lambda *args, **kwargs: "current-epoch")
    monkeypatch.setattr(reconcile_kis, "trading_epoch_enforced", lambda: True)
    return reconcile_kis.reconcile_today(
        engine=db, kis=Kis(), trade_date=date(2026, 11, 23),
        ctx=RunContext(
            run_id="run", env="practice",
            strategy="pb1_pullback_close",
            started_at=datetime(2026, 11, 23, 9, 32),
            dry_run=True,
        ),
    )


def test_actual_daily_reconcile_skips_15_share_kr_infinite(monkeypatch, db_fixture):
    result = _run(monkeypatch, db_fixture)
    assert result["ok"] is True
    assert result["orders"] == 0
    assert result["fills"] == 0
    assert result["verified_infinite_owner_orders"] == 1


def test_actual_daily_reconcile_fails_closed_on_14_vs_15(monkeypatch, db_fixture):
    with pytest.raises(RuntimeError, match="KR_RECONCILE_SOURCE_UNATTRIBUTED"):
        _run(monkeypatch, db_fixture, total="14")
