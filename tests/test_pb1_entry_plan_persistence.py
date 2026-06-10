from datetime import datetime, timezone

from sqlalchemy import create_engine, select

from trader.db.repos import PositionsRepo
from trader.db.schema import schema_for_engine
from trader.trade_plan import build_entry_exit_plan


def test_buy_fill_persists_entry_exit_plan_to_position():
    engine = create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    plan = build_entry_exit_plan(
        code="005930", market="KOSPI", entry_style_selected="ENTRY_PULLBACK", entry_reason="ENTRY_PULLBACK", entry_price=10000, features={"stop_price": 9500}
    ).to_dict()
    repo = PositionsRepo(engine)
    repo.apply_fill(
        env="practice", strategy="pb1_pullback_close", sid=1, mode=1, code="005930", market="KOSPI", side="BUY", qty=1, price=10000, fee=0, tax=0,
        filled_at=datetime.now(timezone.utc), entry_exit_plan=plan, entry_meta=plan,
    )
    with engine.connect() as conn:
        row = conn.execute(select(schema.positions)).mappings().first()
    assert row["entry_exit_plan_json"]["policy_version"] == "pb1_entry_exit_plan_v1"
    assert row["trade_horizon"] == "SWING"
    assert row["exit_policy_family"] == "SWING_STAGED_EXIT"
    assert row["eod_action"] == "CARRY_IF_NO_EXIT_SIGNAL"


def test_plan_missing_candidate_rejected_before_order():
    try:
        build_entry_exit_plan(code="005930", market="KOSPI", entry_style_selected="ENTRY_UNKNOWN", entry_reason="ENTRY_UNKNOWN", entry_price=10000, features={"stop_price": 9500})
    except ValueError:
        return
    raise AssertionError("unknown entry style must not create BUY plan")
