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

from trader.db.repos import FillsRepo, OrdersRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.reconcile_kis import reconcile_today
from trader.run_context import RunContext


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
    assert row["entry_exit_plan_json"]["policy_version"] == "pb1_entry_exit_plan_v1"
    assert row["trade_horizon"] == "SWING"
    assert row["exit_policy_family"] == "SWING_STAGED_EXIT"
