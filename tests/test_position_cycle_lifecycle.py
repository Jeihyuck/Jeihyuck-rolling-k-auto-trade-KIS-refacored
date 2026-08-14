from datetime import datetime, timezone

import sqlalchemy as sa

from trader.db.repos import FillsRepo, OrdersRepo, PortfolioEpochsRepo, PositionsRepo
from trader.db.schema import schema_for_engine
from trader.exit_policy.router import apply_swing_exit_decision
from trader.position_lifecycle import new_cycle_state, validate_active_cycle, validate_long_stop


def _repo():
    engine = sa.create_engine("sqlite:///:memory:")
    schema_for_engine(engine).metadata.create_all(engine)
    return engine, PositionsRepo(engine)


def _fill(repo, side, price=100.0):
    repo.apply_fill(env="practice", strategy="pb1", sid=1, mode=1, code="000001",
                    market="KOSPI", side=side, qty=1, price=price, fee=0, tax=0,
                    filled_at=datetime.now(timezone.utc))


def test_full_sell_then_rebuy_creates_new_clean_cycle():
    engine, repo = _repo()
    _fill(repo, "BUY")
    first = repo.get_position(env="practice", strategy="pb1", sid=1, mode=1, code="000001")
    repo.update_position_fields(env="practice", strategy="pb1", sid=1, mode=1, code="000001",
                                fields={"position_meta": {"tp1_done": True, "holding_bars": 56}, "max_price": 200})
    _fill(repo, "SELL", 90)
    _fill(repo, "BUY", 110)
    second = repo.get_position(env="practice", strategy="pb1", sid=1, mode=1, code="000001")
    assert second["position_cycle_id"] != first["position_cycle_id"]
    assert second["portfolio_epoch_id"] == first["portfolio_epoch_id"]
    assert second["status"] == "OPEN" and second["avg_buy_price"] == 110
    assert second["position_meta"] == {} and second["max_price"] is None
    with engine.begin() as conn:
        old = conn.execute(sa.select(schema_for_engine(engine).positions).where(
            schema_for_engine(engine).positions.c.position_cycle_id == first["position_cycle_id"])).mappings().one()
    assert old["status"] == "CLOSED" and old["closed_reason"] == "FULL_SELL"


def test_imported_cycle_does_not_inherit_history():
    state = new_cycle_state(epoch_id="epoch-new", price=491000, origin="IMPORTED")
    assert state["entry_price"] == 491000
    assert state["position_meta"]["holding_age_unknown"] is True
    assert state["position_meta"]["holding_bars"] == 0
    assert state["tp1_done"] is False and state["last_trail_stop"] is None


def test_cycle_cost_basis_and_provenance_fail_closed():
    pos = {"status": "OPEN", "portfolio_epoch_id": "e", "position_cycle_id": "c", "position_origin": "SYSTEM"}
    mismatch = validate_active_cycle(pos, epoch_id="e", kis_qty=1, kis_avg=100,
                                     fills=[{"side": "BUY", "qty": 1, "price": 150,
                                             "position_cycle_id": "c", "portfolio_epoch_id": "e"}])
    assert not mismatch.ok and mismatch.reason == "POSITION_COST_BASIS_MISMATCH"
    stale = validate_active_cycle({**pos, "portfolio_epoch_id": "old"}, epoch_id="e", kis_qty=1,
                                  kis_avg=100, fills=[])
    assert not stale.ok and stale.reason == "POSITION_STATE_MISMATCH"


def test_invalid_long_stop_blocks_router_sell():
    valid, reason, risk = validate_long_stop(491000, 616590)
    assert not valid and reason == "INVALID_STOP" and risk < 0
    result = apply_swing_exit_decision(
        {"code": "000001", "avg_buy_price": 491000, "qty": 3}, 490000,
        {"max_hold_days": 10, "partial_sell_rules": [], "full_exit_rules": []},
        ret_pct=-1, current_r=-1, highest_ret_pct=0, days_held=0,
        stop_hit=True, effective_stop=616590, effective_r=-125590,
    )
    assert result["exit_ok"] is False and result["reason"] == "INVALID_STOP" and result["qty"] == 0


def test_zero_r_is_invalid_r():
    assert validate_long_stop(100, 100) == (False, "INVALID_R", 0.0)


def test_active_epoch_is_shared_and_only_explicit_reset_changes_it():
    engine, positions = _repo()
    epochs = PortfolioEpochsRepo(engine)
    identity = dict(env="practice", account_id="acct-1", sid=1, mode=1, strategy="pb1")
    first = epochs.get_or_create_active(**identity)
    assert epochs.get_or_create_active(**identity) == first
    positions.bootstrap_from_kis_holdings(
        "practice", "pb1", 1, 1,
        [{"code": "000001", "qty": 1, "avg_price": 100},
         {"code": "000002", "qty": 1, "avg_price": 200}], account_id="acct-1",
    )
    with engine.begin() as conn:
        position_epochs = set(conn.execute(sa.select(schema_for_engine(engine).positions.c.portfolio_epoch_id)).scalars())
    assert position_epochs == {first}
    second = epochs.start_new_epoch(**identity, reason="NEW_PRACTICE_EPOCH")
    assert second != first and epochs.get_or_create_active(**identity) == second


def test_order_fill_position_share_cycle_and_epoch_provenance():
    engine, positions = _repo()
    orders, fills = OrdersRepo(engine), FillsRepo(engine)
    order_id, created = orders.create_intent_idempotent(
        env="practice", run_id=None, strategy="pb1", sid=1, mode=1,
        code="000003", market="KOSPI", side="BUY", ord_type="LIMIT", qty=2,
        limit_price=300, stage="TEST", client_order_key="cycle-provenance",
        request_json={}, account_id="acct-1",
    )
    assert created
    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        order = conn.execute(sa.select(schema.orders).where(schema.orders.c.order_id == order_id)).mappings().one()
    fills.upsert_fill(env="practice", run_id=None, order_id=order_id, kis_odno="1",
                      trade_id="trade-1", code="000003", market="KOSPI", side="BUY",
                      qty=2, price=300, fee=0, tax=0, filled_at=datetime.now(timezone.utc), raw_json={})
    positions.apply_fill(env="practice", strategy="pb1", sid=1, mode=1, code="000003",
                         market="KOSPI", side="BUY", qty=2, price=300, fee=0, tax=0,
                         filled_at=datetime.now(timezone.utc), account_id="acct-1", order_id=order_id)
    with engine.begin() as conn:
        fill = conn.execute(sa.select(schema.fills)).mappings().one()
        position = conn.execute(sa.select(schema.positions).where(schema.positions.c.code == "000003")).mappings().one()
    assert fill["position_cycle_id"] == position["position_cycle_id"] == order["position_cycle_id"]
    assert fill["portfolio_epoch_id"] == position["portfolio_epoch_id"] == order["portfolio_epoch_id"]
