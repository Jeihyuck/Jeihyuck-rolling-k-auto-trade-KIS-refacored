"""2026-11-04: old and new practice accounts may both have ACTIVE epochs.

No KIS order submission. SQLite unit boundary tests; the PR also requires a
real-PostgreSQL integration check before MERGE READY.
"""
from __future__ import annotations

from datetime import timedelta
import json
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.db.schema import schema_for_engine
from trader.time_utils import now_kst
from trader.kr import recovery_scope_20261104 as scope
from trader.db.repos import OrdersRepo
from trader.kr.broker_truth_historical_retry_safety import _safe_owned_buy_fill_groups
from trader.kr.broker_truth_cross_date_unowned_retry import _find_exact_order_for_unowned_fill


STRATEGY = "pb1_pullback_close"
CURRENT = "practice:22222222:01"
PREVIOUS = "practice:11111111:01"


def _accounts(monkeypatch):
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    monkeypatch.setattr(scope, "trading_epoch_enforced", lambda: True)
    monkeypatch.setattr(scope, "get_account_key", lambda **_kw: CURRENT)
    day = now_kst() - timedelta(days=1)
    identities = {}
    with engine.begin() as conn:
        for account in (PREVIOUS, CURRENT):
            epoch, portfolio, cycle, order, fill = [str(uuid4()) for _ in range(5)]
            identities[account] = dict(
                epoch=epoch, portfolio=portfolio, cycle=cycle,
                order=order, fill=fill,
            )
            conn.execute(sa.insert(schema.trading_epochs).values(
                trading_epoch_id=epoch, env="practice", account_id=account,
                status="ACTIVE", reason="test",
            ))
            conn.execute(sa.insert(schema.portfolio_epochs).values(
                portfolio_epoch_id=portfolio, trading_epoch_id=epoch,
                env="practice", account_id=account, sid=1, mode=1,
                strategy=STRATEGY, status="ACTIVE",
            ))
            conn.execute(sa.insert(schema.orders).values(
                order_id=order, position_cycle_id=cycle,
                portfolio_epoch_id=portfolio, trading_epoch_id=epoch,
                env="practice", strategy=STRATEGY, sid=1, mode=1,
                code="005930", market="KOSPI", side="BUY",
                ord_type="MARKET", qty=2, status="FILLED",
                client_order_key=account + ":order",
                # Durable immutable pre-order KIS holding evidence. The original
                # fixture lacked both a lifecycle and this zero baseline;
                # production correctly rejected it as unsafe recovery.
                request_json=json.dumps({"pre_order_holding_qty": 0}),
                kis_odno="0000001212", created_at=day, submitted_at=day,
            ))
            conn.execute(sa.insert(schema.fills).values(
                fill_id=fill, order_id=order,
                position_cycle_id=cycle, portfolio_epoch_id=portfolio,
                trading_epoch_id=epoch, env="practice",
                kis_odno="0000001212", trade_id=account + ":trade",
                code="005930", side="BUY", market="KOSPI",
                qty=2, price=(100000.0 if account == CURRENT else 99999.0), fee=0.0, tax=0.0,
                filled_at=day,
            ))
    return engine, schema, identities, day


def test_old_active_account_does_not_enter_current_owned_buy_recovery(monkeypatch):
    engine, _schema, ids, _day = _accounts(monkeypatch)
    groups = _safe_owned_buy_fill_groups(
        engine=engine, env="practice", strategy=STRATEGY,
    )
    assert set(groups) == {ids[CURRENT]["order"]}
    assert groups[ids[CURRENT]["order"]]["order"]["position_cycle_id"] == ids[CURRENT]["cycle"]
    assert ids[PREVIOUS]["order"] not in groups



@pytest.mark.parametrize("unsafe_request", [
    {},
    {"pre_order_holding_qty": None},
    {"pre_order_holding_qty": "unknown"},
    {"pre_order_holding_qty": 1},
])
def test_current_account_without_zero_baseline_is_blocked(monkeypatch, unsafe_request):
    engine, schema, identities, _day = _accounts(monkeypatch)
    with engine.begin() as conn:
        conn.execute(sa.update(schema.orders).where(
            schema.orders.c.order_id == identities[CURRENT]["order"],
        ).values(request_json=json.dumps(unsafe_request)))
    groups = _safe_owned_buy_fill_groups(
        engine=engine, env="practice", strategy=STRATEGY,
    )
    assert identities[CURRENT]["order"] not in groups
    assert identities[PREVIOUS]["order"] not in groups


def test_unowned_fill_lookup_uses_both_active_epoch_and_order_cycle(monkeypatch):
    engine, schema, ids, day = _accounts(monkeypatch)
    active = scope.resolve_kr_recovery_scope(engine=engine, env="practice")
    assert active.trading_epoch_id == ids[CURRENT]["epoch"]
    evidence = {
        "env": "practice", "trading_epoch_id": ids[CURRENT]["epoch"],
        "code": "005930", "side": "BUY",
        "kis_odno": "0000001212", "filled_at": day,
    }
    with engine.connect() as conn:
        selected = _find_exact_order_for_unowned_fill(
            conn=conn, schema=schema, env="practice", strategy=STRATEGY,
            fill=evidence, scope=active,
        )
        assert selected["order_id"] == ids[CURRENT]["order"]
        assert _find_exact_order_for_unowned_fill(
            conn=conn, schema=schema, env="practice", strategy=STRATEGY,
            fill={**evidence, "trading_epoch_id": ids[PREVIOUS]["epoch"]},
            scope=active,
        ) is None
        assert _find_exact_order_for_unowned_fill(
            conn=conn, schema=schema, env="practice", strategy=STRATEGY,
            fill={**evidence, "position_cycle_id": ids[PREVIOUS]["cycle"]},
            scope=active,
        ) is None


def test_portfolio_and_position_cycle_must_match(monkeypatch):
    engine, schema, ids, _ = _accounts(monkeypatch)
    active = scope.resolve_kr_recovery_scope(engine=engine, env="practice")
    with engine.connect() as conn:
        order = dict(conn.execute(sa.select(schema.orders).where(
            schema.orders.c.order_id == ids[CURRENT]["order"],
        )).mappings().one())
        assert scope.order_matches_recovery_scope(
            conn=conn, schema=schema, order=order, strategy=STRATEGY, scope=active,
        )
        for field, invalid in (
            ("trading_epoch_id", ids[PREVIOUS]["epoch"]),
            ("portfolio_epoch_id", ids[PREVIOUS]["portfolio"]),
            ("position_cycle_id", None),
            ("strategy", "KR_INFINITE"),
        ):
            assert not scope.order_matches_recovery_scope(
                conn=conn, schema=schema, order={**order, field: invalid},
                strategy=STRATEGY, scope=active,
            )


def test_auth_broker_account_mismatch_fails_closed(monkeypatch):
    class FakeKIS:
        CANO = "11111111"
        ACNT_PRDT_CD = "01"

    engine, _schema, _ids, _day = _accounts(monkeypatch)
    monkeypatch.setattr(
        scope, "get_account_key",
        lambda *, env=None, kis=None: PREVIOUS if kis is not None else CURRENT,
    )
    with pytest.raises(RuntimeError, match="BROKER_ACCOUNT_MISMATCH"):
        scope.resolve_kr_recovery_scope(
            engine=engine, env="practice", kis=FakeKIS(),
        )


def test_missing_identity_is_not_an_unscoped_production_retry(monkeypatch):
    engine, _schema, _ids, _day = _accounts(monkeypatch)
    monkeypatch.setattr(scope, "get_account_key", lambda **_kw: "practice:unknown:unknown")
    with pytest.raises(RuntimeError, match="ACCOUNT_UNPROVEN"):
        scope.resolve_kr_recovery_scope(engine=engine, env="practice")


def test_am_exit_137_is_never_labelled_a_proven_timeout():
    script = Path("scripts/wsl/run-kr-am.sh").read_text()
    assert "SESSION_KILLED_UNVERIFIED" in script
    assert 'if [[ "$rc" -eq 124 || "$rc" -eq 137 ]]' in script
    assert 'if [[ "$rc" -eq 124 ]]; then' in script
    assert "elapsed_sec=" in script



def test_scoped_source_lookup_prefers_current_account_despite_identical_odno(monkeypatch):
    engine, _schema, ids, day = _accounts(monkeypatch)
    repo = OrdersRepo(engine)
    match = repo.get_order_by_kis_odno_scoped(
        "practice", "0000001212", trade_date=day.date(),
        code="005930", side="BUY", account_id=CURRENT,
        trading_epoch_id=ids[CURRENT]["epoch"],
    )
    assert match is not None
    assert match["order_id"] == ids[CURRENT]["order"]
    previous = repo.get_order_by_kis_odno_scoped(
        "practice", "1212", trade_date=day.date(),
        code="005930", side="BUY", account_id=PREVIOUS,
        trading_epoch_id=ids[PREVIOUS]["epoch"],
    )
    assert previous is not None
    assert previous["order_id"] == ids[PREVIOUS]["order"]
    assert repo.get_order_by_kis_odno_scoped(
        "practice", "0000001212", trade_date=day.date(),
        code="005930", side="BUY", account_id=CURRENT,
        trading_epoch_id=ids[PREVIOUS]["epoch"],
    ) is None


def test_open_order_scope_ignores_only_proven_foreign_unresolved(monkeypatch):
    engine, schema, ids, _day = _accounts(monkeypatch)
    with engine.begin() as conn:
        conn.execute(sa.update(schema.orders).values(status="UNRESOLVED_ACK"))
    class FakeKIS:
        CANO = "22222222"
        ACNT_PRDT_CD = "01"
    blockers = scope.current_scope_open_order_blockers(
        engine=engine, env="practice", kis=FakeKIS(),
        orders_repo=OrdersRepo(engine),
    )
    assert {item["order_id"] for item in blockers} == {ids[CURRENT]["order"]}


def test_unproven_open_order_never_disappears_from_protective_sell_gate():
    active = scope.KRRecoveryScope(
        env="practice", account_key=CURRENT, trading_epoch_id="current-epoch"
    )
    foreign = {
        "order_id": "old", "env": "practice", "strategy": STRATEGY,
        "portfolio_epoch_id": "p-old", "position_cycle_id": "c-old",
        "trading_epoch_id": "old-epoch",
    }
    current = {
        "order_id": "current", "env": "practice", "strategy": STRATEGY,
        "portfolio_epoch_id": "p-current", "position_cycle_id": "c-current",
        "trading_epoch_id": "current-epoch",
    }
    unknown = {**current, "order_id": "unknown", "portfolio_epoch_id": None}
    foreign_portfolio = {
        "portfolio_epoch_id": "p-old", "env": "practice",
        "account_id": PREVIOUS, "strategy": STRATEGY,
        "trading_epoch_id": "old-epoch",
    }
    current_portfolio = {
        "portfolio_epoch_id": "p-current", "env": "practice",
        "account_id": CURRENT, "strategy": STRATEGY,
        "trading_epoch_id": "current-epoch",
    }
    relevant = scope.relevant_open_orders_for_scope(
        orders=[foreign, current, unknown],
        portfolios={"p-old": foreign_portfolio, "p-current": current_portfolio},
        scope=active,
    )
    assert {item["order_id"] for item in relevant} == {"current", "unknown"}
    # Tampered foreign ownership is UNKNOWN, not a proven harmless prior order.
    relevant = scope.relevant_open_orders_for_scope(
        orders=[foreign],
        portfolios={"p-old": {**foreign_portfolio, "strategy": "KR_INFINITE"}},
        scope=active,
    )
    assert relevant == [foreign]
