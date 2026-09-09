from trader.db.repos import _plan_position_values
from trader.trade_plan import build_entry_exit_plan, classify_close_action_from_plan


def _record():
    plan = build_entry_exit_plan(code="005930", market="KOSPI", entry_style_selected="ENTRY_PULLBACK", entry_reason="ENTRY_PULLBACK", entry_price=10000, features={"stop_price": 9500}).to_dict()
    return {"entry_exit_plan": plan, "entry_meta": {"source": "test"}}


def test_restore_values_from_latest_buy_fill_plan():
    values = _plan_position_values(_record())
    assert values["trade_horizon"] == "SWING"
    assert values["entry_exit_plan_json"]["exit_policy_family"] == "SWING_STAGED_EXIT"


def test_restore_values_from_latest_buy_order_plan():
    values = _plan_position_values(_record())
    assert values["exit_policy_family"] == "SWING_STAGED_EXIT"


def test_restore_values_policy_missing_when_no_plan():
    values = _plan_position_values(None, missing=True)
    assert values["entry_thesis"] == "POLICY_MISSING"
    assert values["exit_policy_family"] == "POLICY_MISSING"
    assert values["force_eod_close"] is False


def test_policy_missing_not_force_sold():
    assert classify_close_action_from_plan({}) == ("SKIP", "POLICY_MISSING")

from datetime import date

from sqlalchemy import create_engine, select

from trader.db.repos import FillsRepo, LedgerEventsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.reconcile_kis import reconcile_today, _restore_entry_meta_for_promoted_positions
from trader.run_context import RunContext
from trader.pb1_engine import _position_policy_missing_contract, _resolve_position_horizon


class _DailyCcldKis:
    def inquire_daily_ccld(self, **_kwargs):
        return {
            "output1": [
                {
                    "pdno": "005930",
                    "side": "BUY",
                    "ord_qty": "1",
                    "ccld_qty": "1",
                    "ord_unpr": "10000",
                    "ccld_prc": "10000",
                    "odno": "KIS123",
                    "ord_dt": "20260610",
                    "ord_tmd": "091500",
                }
            ],
            "output2": [],
        }


def test_daily_ccld_reconcile_merges_order_plan_and_restores_position():
    engine = create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    plan = build_entry_exit_plan(
        code="005930", market="KOSPI", entry_style_selected="ENTRY_PULLBACK", entry_reason="ENTRY_PULLBACK", entry_price=10000, features={"stop_price": 9500}
    ).to_dict()
    orders_repo = OrdersRepo(engine)
    orders_repo.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1_pullback_close", sid=1, mode=1, code="005930", market="KOSPI", side="BUY",
        ord_type="LIMIT", qty=1, limit_price=10000, stage="PB1-AM-ENTRY", client_order_key="original-buy-plan",
        request_json={"entry_exit_plan": plan, "entry_meta": {"entry_reason": "ENTRY_PULLBACK"}}, status="SUBMITTED",
    )
    reconcile_today(engine=engine, kis=_DailyCcldKis(), ctx=RunContext(env="practice", strategy="pb1_pullback_close"))
    positions_repo = PositionsRepo(engine)
    restored = positions_repo.restore_missing_from_holdings(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1,
        holdings=[{"pdno": "005930", "hldg_qty": "1", "pchs_avg_pric": "10000", "pchs_amt": "10000", "market": "KOSPI"}],
        fills_repo=FillsRepo(engine), orders_repo=orders_repo,
    )
    assert restored == 1
    with engine.connect() as conn:
        row = conn.execute(select(schema.positions)).mappings().first()
    assert row["entry_exit_plan_json"] == {}
    assert row["position_origin"] == "IMPORTED"
    assert row["position_meta"]["holding_age_unknown"] is True


def test_same_cycle_meta_restore_clears_stale_policy_missing_top_level():
    import uuid

    engine = create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    cycle_id = str(uuid.uuid4())
    epoch_id = str(uuid.uuid4())
    other_cycle_id = str(uuid.uuid4())
    other_epoch_id = str(uuid.uuid4())

    recovered_entry_meta = {
        "entry_thesis": "PULLBACK_CONTINUATION",
        "entry_reason": "ENTRY_PULLBACK",
        "entry_style_selected": "ENTRY_PULLBACK",
        "book": "SWING_BOOK",
        "trade_horizon": "SWING_CARRY",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "eod_action": "CARRY_IF_NO_EXIT_SIGNAL",
        "force_eod_close": False,
        "policy_source": "style_mapping",
        "policy_version": "pb1_entry_exit_plan_v1",
    }

    with engine.begin() as conn:
        conn.execute(
            schema.positions.insert().values(
                position_id=str(uuid.uuid4()),
                position_cycle_id=cycle_id,
                portfolio_epoch_id=epoch_id,
                position_origin="SYSTEM",
                env="practice",
                strategy="pb1_pullback_close",
                sid=1,
                mode=1,
                code="005930",
                market="KOSPI",
                qty=1,
                avg_buy_price=10000.0,
                total_cost=10000.0,
                realized_pnl=0.0,
                status="OPEN",
                entry_thesis="POLICY_MISSING",
                exit_policy_family="POLICY_MISSING",
                policy_source="missing",
                entry_meta_json={},
                entry_exit_plan_json={},
                position_meta={},
            )
        )
        conn.execute(
            schema.positions.insert().values(
                position_id=str(uuid.uuid4()),
                position_cycle_id=other_cycle_id,
                portfolio_epoch_id=other_epoch_id,
                position_origin="SYSTEM",
                env="practice",
                strategy="kr_infinite",
                sid=1,
                mode=1,
                code="005930",
                market="KOSPI",
                qty=2,
                avg_buy_price=9000.0,
                total_cost=18000.0,
                realized_pnl=0.0,
                status="OPEN",
                entry_thesis="POLICY_MISSING",
                exit_policy_family="POLICY_MISSING",
                policy_source="missing",
                entry_meta_json={},
                entry_exit_plan_json={},
                position_meta={},
            )
        )

    orders_repo = OrdersRepo(engine)
    orders_repo.create_intent_idempotent(
        env="practice",
        run_id=None,
        strategy="pb1_pullback_close",
        sid=1,
        mode=1,
        code="005930",
        market="KOSPI",
        side="BUY",
        ord_type="LIMIT",
        qty=1,
        limit_price=10000.0,
        stage="PB1-AM-ENTRY",
        client_order_key="restore-same-cycle-meta",
        request_json={},
        status="ACKED",
        position_cycle_id=cycle_id,
        portfolio_epoch_id=epoch_id,
        entry_meta_json=recovered_entry_meta,
    )

    restored = _restore_entry_meta_for_promoted_positions(
        env="practice",
        strategy="pb1_pullback_close",
        engine=engine,
        orders_repo=orders_repo,
        positions_repo=PositionsRepo(engine),
        ledger_repo=LedgerEventsRepo(engine),
    )

    assert restored == 1
    with engine.connect() as conn:
        row = dict(
            conn.execute(
                select(schema.positions).where(schema.positions.c.strategy == "pb1_pullback_close")
            ).mappings().one()
        )
        other = dict(
            conn.execute(
                select(schema.positions).where(schema.positions.c.strategy == "kr_infinite")
            ).mappings().one()
        )

    assert row["entry_thesis"] == "PULLBACK_CONTINUATION"
    assert row["trade_horizon"] == "SWING"
    assert row["exit_policy_family"] == "SWING_STAGED_EXIT"
    assert row["policy_source"] == "style_mapping"
    assert row["entry_meta_json"]["trade_horizon"] == "SWING_CARRY"
    assert row["position_meta"]["trade_horizon"] == "SWING_CARRY"
    assert _position_policy_missing_contract(row) is False
    assert _resolve_position_horizon(row) == "SWING_CARRY"
    assert other["entry_thesis"] == "POLICY_MISSING"
    assert other["exit_policy_family"] == "POLICY_MISSING"
    assert other["policy_source"] == "missing"


def test_already_restored_system_entry_meta_repairs_stale_top_level_without_order_lookup():
    import uuid

    engine = create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    cycle_id = str(uuid.uuid4())
    epoch_id = str(uuid.uuid4())
    existing_entry_meta = {
        "entry_thesis": "PULLBACK_CONTINUATION",
        "entry_reason": "ENTRY_PULLBACK",
        "entry_style_selected": "ENTRY_PULLBACK",
        "book": "SWING_BOOK",
        "trade_horizon": "SWING_CARRY",
        "exit_policy_family": "SWING_STAGED_EXIT",
        "eod_action": "CARRY_IF_NO_EXIT_SIGNAL",
        "force_eod_close": False,
        "policy_source": "style_mapping",
        "policy_version": "pb1_entry_exit_plan_v1",
    }

    with engine.begin() as conn:
        conn.execute(
            schema.positions.insert().values(
                position_id=str(uuid.uuid4()),
                position_cycle_id=cycle_id,
                portfolio_epoch_id=epoch_id,
                position_origin="SYSTEM",
                env="practice",
                strategy="pb1_pullback_close",
                sid=1,
                mode=1,
                code="000660",
                market="KOSPI",
                qty=1,
                avg_buy_price=10000.0,
                total_cost=10000.0,
                realized_pnl=0.0,
                status="OPEN",
                entry_thesis="POLICY_MISSING",
                exit_policy_family="POLICY_MISSING",
                policy_source="missing",
                entry_meta_json=existing_entry_meta,
                entry_exit_plan_json={},
                position_meta={
                    "book": "SWING_BOOK",
                    "trade_horizon": "SWING_CARRY",
                    "exit_policy_family": "SWING_STAGED_EXIT",
                    "meta_source": "order_meta",
                },
            )
        )

    restored = _restore_entry_meta_for_promoted_positions(
        env="practice",
        strategy="pb1_pullback_close",
        engine=engine,
        orders_repo=OrdersRepo(engine),
        positions_repo=PositionsRepo(engine),
        ledger_repo=LedgerEventsRepo(engine),
    )

    assert restored == 1
    with engine.connect() as conn:
        row = dict(conn.execute(select(schema.positions)).mappings().one())

    assert row["entry_thesis"] == "PULLBACK_CONTINUATION"
    assert row["trade_horizon"] == "SWING"
    assert row["exit_policy_family"] == "SWING_STAGED_EXIT"
    assert row["policy_source"] == "style_mapping"
    assert _position_policy_missing_contract(row) is False
    assert _resolve_position_horizon(row) == "SWING_CARRY"
