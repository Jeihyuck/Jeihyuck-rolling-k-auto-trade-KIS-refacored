"""Offline exact 33-share closed-cycle incident replay; never calls KIS."""
from datetime import datetime, timezone, date
from types import SimpleNamespace
from hashlib import sha256

import pytest
import sqlalchemy as sa

from trader.db.schema import schema_for_engine
from trader.execution_claims import DurableExecutionClaimRepo
from trader.kr.closed_cycle_claim_recovery import retire_closed_pb1_rejected_claims

ACCOUNT = "practice:offline:00"
HASH = sha256(ACCOUNT.encode()).hexdigest()
SCOPE = SimpleNamespace(env="practice", account_key=ACCOUNT, trading_epoch_id="epoch")
CODE = "028050"
ACTION = "SELL:KR_TAKE_PROFIT_RUNNER_FINAL:KR_US_RUNNER_FINAL"
NOW = datetime(2026, 11, 24, 4, tzinfo=timezone.utc)


def _db():
    engine = sa.create_engine("sqlite:///:memory:")
    s = schema_for_engine(engine)
    s.metadata.create_all(engine)
    claim = dict(action_key="a"*64, env="practice", account_id=HASH, market="KR",
                 trading_epoch_id="epoch", strategy_owner="PB1", lifecycle_id="cycle",
                 action=ACTION, target_qty=8, cumulative_filled_qty=0,
                 remaining_target_qty=8, active_attempt_id=None, action_state="RETRYABLE",
                 claim_conflicts=0, trade_date=date(2026, 11, 19))
    common = dict(env="practice", trading_epoch_id="epoch", portfolio_epoch_id="portfolio",
                  position_cycle_id="cycle", code=CODE, market="KOSPI")
    pos_common = dict(strategy="pb1_pullback_close", sid=1, mode=1)
    with engine.begin() as c:
        c.execute(s.trading_epochs.insert().values(trading_epoch_id="epoch", env="practice",
                  account_id=ACCOUNT, status="ACTIVE", reason="offline"))
        c.execute(s.portfolio_epochs.insert().values(portfolio_epoch_id="portfolio",
                  trading_epoch_id="epoch", env="practice", account_id=ACCOUNT,
                  status="ACTIVE", **pos_common))
        c.execute(s.positions.insert().values(**common, **pos_common, qty=0, status="CLOSED",
                  position_origin="SYSTEM", closed_reason="FULL_SELL",
                  closed_ts=datetime(2026, 11, 23, 0, 1, 23, tzinfo=timezone.utc)))
        c.execute(s.execution_claims.insert().values(**claim))
        rejected_at = datetime(2026, 11, 19, 0, 1, 38, tzinfo=timezone.utc)
        c.execute(s.execution_attempts.insert().values(attempt_id="reject", action_key=claim["action_key"],
                  attempt_no=1, requested_qty=8, client_order_key="rejected",
                  cumulative_filled_qty=0, attempt_state="REJECTED_EXPLICIT", authoritative=True,
                  created_at=rejected_at))
        c.execute(s.orders.insert().values(**common, **pos_common, order_id="rejected",
                  side="SELL", qty=8, ord_type="MARKET", client_order_key="rejected", status="ERROR",
                  request_json={"strategy_owner":"PB1", "semantic_action":ACTION, "position_lifecycle_id":"cycle"},
                  response_json={"rt_cd":"1", "_order_execution":{"accepted":False,"code":CODE,
                                     "side":"SELL", "broker_order_id":None}}))
        for i, (day, hour, side, qty) in enumerate([
            (6, 0, "BUY", 33), (12, 0, "SELL", 8), (13, 0, "SELL", 6),
            (16, 0, "SELL", 3), (18, 0, "SELL", 8), (19, 4, "SELL", 2), (23, 0, "SELL", 6)
        ]):
            at = datetime(2026, 11, day, hour, 0, tzinfo=timezone.utc)
            odno = str(i+1).zfill(10)
            oid = f"order-{i}"
            c.execute(s.orders.insert().values(**common, **pos_common, order_id=oid, side=side,
                      qty=qty, ord_type="MARKET", client_order_key=oid, status="FILLED",
                      kis_odno=odno))
            c.execute(s.fills.insert().values(**common, order_id=oid, side=side, qty=qty,
                      price=60000, kis_odno=odno, filled_at=at,
                      broker_fill_id=f"KR_KIS_CUM:{HASH}:202611{day:02d}:{odno}:{CODE}:{side}",
                      fill_meta_json={"evidence_kind":"KIS_ORDER_CUMULATIVE_EXECUTION"},
                      raw_json={"kis_response":{"ord_dt":f"202611{day:02d}","odno":odno,
                           "pdno":CODE,"sll_buy_dvsn_cd":"02" if side=="BUY" else "01",
                           "ord_qty":str(qty),"tot_ccld_qty":str(qty),"rmn_qty":"0"}}))
    return engine, s, claim["action_key"]


def test_closed_cycle_retirement_is_atomic_idempotent_and_not_an_economic_fill():
    e,s,key = _db()
    assert retire_closed_pb1_rejected_claims(engine=e, scope=SCOPE, holdings_rows=[], now=NOW) == 1
    assert retire_closed_pb1_rejected_claims(engine=e, scope=SCOPE, holdings_rows=[], now=NOW) == 0
    with e.connect() as c:
        row = c.execute(sa.select(s.execution_claims)).mappings().one()
        assert row["action_state"] == "SUPERSEDED"
        assert row["cumulative_filled_qty"] == 0 and row["remaining_target_qty"] == 8
        assert c.execute(sa.select(sa.func.count()).select_from(s.fills)).scalar_one() == 7
        log = c.execute(sa.select(s.reconcile_log)).mappings().one()
        assert log["details_json"]["later_sell_qty"] == 8
    repo = DurableExecutionClaimRepo(e,s.execution_claims,s.execution_attempts)
    assert repo.record_observation(key,attempt_id="reject",state="REJECTED_EXPLICIT",
                 cumulative_filled_qty=0,authoritative=True).action_state == "SUPERSEDED"
    assert repo.record_observation(key,attempt_id="reject",state="FILLED",
                 cumulative_filled_qty=8,authoritative=True).action_state == "UNCERTAIN"


@pytest.mark.parametrize("fault", ["holding", "missing_fill", "foreign_epoch", "unresolved", "unproven_rejection"])
def test_closed_cycle_recovery_fails_closed(fault):
    e,s,key = _db()
    holdings = []
    with e.begin() as c:
        if fault == "holding":
            holdings = [{"pdno":CODE,"hldg_qty":"1"}]
        elif fault == "missing_fill":
            c.execute(s.fills.delete().where(s.fills.c.order_id=="order-6"))
        elif fault == "foreign_epoch":
            c.execute(s.fills.update().where(s.fills.c.order_id=="order-6").values(trading_epoch_id="other"))
        elif fault == "unresolved":
            c.execute(s.execution_attempts.update().values(attempt_state="UNRESOLVED"))
        else:
            c.execute(s.orders.update().where(s.orders.c.order_id=="rejected").values(response_json={}))
    assert retire_closed_pb1_rejected_claims(engine=e, scope=SCOPE, holdings_rows=holdings, now=NOW) == 0
    with e.connect() as c:
        assert c.execute(sa.select(s.execution_claims.c.action_state)).scalar_one() == "RETRYABLE"


@pytest.mark.parametrize("broker_id_kind, expected", [
    ("local_alias", 1), ("native", 0), ("other_local", 0),
    ("alias_with_native_odno", 0), ("alias_with_accepted", 0),
])
def test_closed_cycle_rejected_legacy_local_alias(broker_id_kind, expected):
    e,s,key = _db()
    local_key = "practice:pb1_pullback_close:2026-11-19:028050:EXIT:SELL:KR_US_RUNNER_FINAL:close:1"
    values = {"client_order_key": local_key, "broker_order_id": local_key}
    if broker_id_kind == "native":
        values["broker_order_id"] = "0000001234"
    elif broker_id_kind == "other_local":
        values["broker_order_id"] = local_key + ":other"
    elif broker_id_kind == "alias_with_native_odno":
        values["kis_odno"] = "0000001234"
    elif broker_id_kind == "alias_with_accepted":
        values["response_json"] = {"rt_cd":"1", "_order_execution":{
            "accepted":True, "code":CODE, "side":"SELL", "broker_order_id":None}}
    with e.begin() as c:
        c.execute(s.execution_attempts.update().values(client_order_key=local_key))
        c.execute(s.orders.update().where(s.orders.c.order_id=="rejected").values(**values))
    assert retire_closed_pb1_rejected_claims(engine=e, scope=SCOPE, holdings_rows=[], now=NOW) == expected
    with e.connect() as c:
        assert c.execute(sa.select(s.execution_claims.c.action_state)).scalar_one() == (
            "SUPERSEDED" if expected else "RETRYABLE")
        assert c.execute(sa.select(sa.func.count()).select_from(s.fills)).scalar_one() == 7
