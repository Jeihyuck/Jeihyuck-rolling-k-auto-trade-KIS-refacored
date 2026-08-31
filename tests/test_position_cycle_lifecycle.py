from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from trader.db.repos import FillsRepo, OrdersRepo, PortfolioEpochsRepo, PositionsRepo, _ensure_active_epoch
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


def test_valid_long_stop_hit_remains_sellable():
    result = apply_swing_exit_decision(
        {"code": "000001", "avg_buy_price": 100, "qty": 1}, 92,
        {"max_hold_days": 10, "partial_sell_rules": [], "full_exit_rules": []},
        ret_pct=-8, current_r=-1, highest_ret_pct=0, days_held=0,
        stop_hit=True, effective_stop=93, effective_r=7,
    )
    assert result["exit_ok"] is True and result["reason"] == "STOP_HIT_EFFECTIVE" and result["qty"] == 1


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


def test_concurrent_active_epoch_unique_conflict_reselects_winner():
    class Result:
        def __init__(self, value): self.value = value
        def scalar(self): return self.value

    class Savepoint:
        def __enter__(self): return self
        def __exit__(self, *_args): return False

    class RacingConnection:
        def __init__(self): self.selects = 0
        def begin_nested(self): return Savepoint()
        def execute(self, statement):
            if getattr(statement, "is_select", False):
                self.selects += 1
                return Result(None if self.selects == 1 else "winning-epoch")
            raise IntegrityError("concurrent insert", {}, Exception("unique violation"))

    schema = schema_for_engine(sa.create_engine("sqlite:///:memory:"))
    result = _ensure_active_epoch(RacingConnection(), schema, env="practice", account_id="acct",
                                  sid=1, mode=1, strategy="pb1")
    assert result == "winning-epoch"


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


def test_legacy_closed_position_bootstraps_clean_imported_cycle():
    engine, positions = _repo()
    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        conn.execute(sa.insert(schema.positions).values(
            position_id="legacy-pos", position_cycle_id="legacy-cycle",
            portfolio_epoch_id="legacy-epoch", opened_at=datetime.now(timezone.utc),
            position_origin="RECOVERY", env="real", strategy="pb1", sid=1, mode=1,
            code="000004", qty=0, total_cost=0, realized_pnl=0, status="CLOSED",
            closed_reason="LEGACY_MIGRATION_0050", position_meta={"tp1_done": True, "holding_bars": 50},
        ))
    assert positions.restore_missing_from_holdings(
        env="real", strategy="pb1", sid=1, mode=1,
        holdings=[{"code": "000004", "qty": 2, "avg_price": 400}], account_id="real:acct:01",
    ) == 1
    current = positions.get_position(env="real", strategy="pb1", sid=1, mode=1, code="000004")
    assert current["position_cycle_id"] != "legacy-cycle"
    assert current["position_origin"] == "IMPORTED"
    assert current["position_meta"] == {"holding_age_unknown": True, "holding_bars": 0, "tp1_done": False, "tp2_done": False}


def test_legacy_null_provenance_fills_are_excluded_and_remaining_basis_is_primary():
    pos = {"status": "OPEN", "portfolio_epoch_id": "e", "position_cycle_id": "c",
           "position_origin": "SYSTEM", "qty": 10, "total_cost": 1100}
    fills = [
        {"side": "BUY", "qty": 10, "price": 100, "position_cycle_id": None, "portfolio_epoch_id": None},
        {"side": "BUY", "qty": 1, "price": 100, "position_cycle_id": "c", "portfolio_epoch_id": "e"},
    ]
    result = validate_active_cycle(pos, epoch_id="e", kis_qty=10, kis_avg=110, fills=fills)
    assert result.ok and result.reason == "OK"
    assert result.cycle_fill_avg == 100 and result.difference_pct == 0


def test_imported_cycle_is_persisted_once_and_reused_across_ticks():
    engine, positions = _repo()
    schema = schema_for_engine(engine)
    with engine.begin() as conn:
        conn.execute(sa.insert(schema.positions).values(
            position_id="legacy-pos-oci", position_cycle_id="legacy-cycle-oci",
            portfolio_epoch_id="legacy-epoch-oci", opened_at=datetime.now(timezone.utc),
            position_origin="RECOVERY", env="practice", strategy="pb1_pullback_close",
            sid=1, mode=1, code="010060", qty=14, avg_buy_price=271660,
            total_cost=3803240, realized_pnl=0, status="OPEN",
            max_price=397500, last_trail_stop=334107,
            position_meta={"holding_bars": 68, "trading_days_held": 68},
        ))
    cycles = []
    for _ in range(3):
        row, created = positions.get_or_create_imported_cycle_for_kis_holding(
            env="practice", strategy="pb1_pullback_close", account_id="acct-practice",
            sid=1, mode=1, code="010060", market="J", qty=14, avg_price=271660,
        )
        cycles.append(str(row["position_cycle_id"]))
    assert len(set(cycles)) == 1
    with engine.begin() as conn:
        open_rows = list(conn.execute(sa.select(schema.positions).where(
            sa.and_(schema.positions.c.code == "010060", schema.positions.c.status == "OPEN"))).mappings())
    assert len(open_rows) == 1
    assert open_rows[0]["position_origin"] == "IMPORTED"
    assert not str(open_rows[0]["position_cycle_id"]).startswith("legacy-cycle-")
    assert open_rows[0]["entry_ts"] is None
    assert open_rows[0]["last_trail_stop"] is None
    assert open_rows[0]["position_meta"]["trail_eligible"] is False
