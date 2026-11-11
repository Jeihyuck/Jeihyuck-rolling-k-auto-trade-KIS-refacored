"""Disposable PostgreSQL KR TP claims: no duplicate submit after restart."""
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.db.schema import schema_for_engine
from trader.kr.us_profit_stage_journal import (
    load_frozen_stage_states, reserve_tp_stage, observe_broker_stage,
)
from trader.kr.us_standard_profit_parity import build_new_swing_parity_contract


@pytest.fixture
def stage_db(monkeypatch):
    url = os.getenv("PBCORE_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("Disposable PostgreSQL URL unavailable")
    monkeypatch.setenv("KR_SWING_US_PROFIT_MODE", "SHADOW")
    engine = sa.create_engine(url, future=True)
    schema_for_engine(engine).metadata.create_all(engine)
    account = "practice:krstage:" + uuid4().hex
    epoch = "epoch-" + uuid4().hex
    cycle = "cycle-" + uuid4().hex
    contract = build_new_swing_parity_contract(
        strategy_owner="PB1", exit_policy_family="SWING_STAGED_EXIT",
        trade_horizon="SWING", position_action="NEW_POSITION_BUY",
        account_id=account, trading_epoch_id=epoch,
        position_cycle_id=cycle, runner_version="US_V3", code="005930",
    )
    args = dict(
        engine=engine, contract=contract, account_id=account,
        trading_epoch_id=epoch, position_cycle_id=cycle, code="005930",
    )
    yield args
    engine.dispose()


DAY = date(2026, 11, 6)


def reserve(args, stage, qty, key):
    return reserve_tp_stage(
        **args, stage=stage, target_qty=qty,
        client_order_key=key, trade_date=DAY,
    )


def observe(args, stage, key, odno, filled, *, date_=DAY, source="KIS_DAILY_CCLD_VERIFIED"):
    return observe_broker_stage(
        **args, stage=stage, client_order_key=key,
        broker_odno=odno, cumulative_filled_qty=filled,
        broker_order_accepted=True, broker_fill_source=source,
        source_trade_date=date_,
    )


def test_full_tp_sequence_and_restart_with_exact_cumulative_fill(stage_db):
    args = stage_db
    assert load_frozen_stage_states(**args)["TP1"]["status"] == "NOT_TRIGGERED"
    first = reserve(args, "TP1", 25, "new-tp1")
    assert first.accepted and not first.order_authorized
    assert not reserve(args, "TP1", 25, "new-tp1").accepted
    assert not reserve(args, "TP2", 18, "new-tp2").accepted
    assert observe(args, "TP1", "new-tp1", "ODNO1", 0).status == "ACK"
    assert observe(args, "TP1", "new-tp1", "ODNO1", 12).status == "PARTIAL_FILLED"
    assert not reserve(args, "TP2", 18, "new-tp2").accepted
    assert observe(args, "TP1", "new-tp1", "ODNO1", 25).status == "FILLED"
    assert reserve(args, "TP2", 18, "new-tp2").accepted
    assert observe(args, "TP2", "new-tp2", "ODNO2", 18).status == "FILLED"
    assert reserve(args, "TP3", 11, "new-tp3").accepted
    assert observe(args, "TP3", "new-tp3", "ODNO3", 11).status == "FILLED"
    fresh = load_frozen_stage_states(**args)
    assert all(fresh[k]["status"] == "FILLED" for k in ("TP1", "TP2", "TP3"))
    assert all(fresh[k]["broker_fill_proven"] for k in ("TP1", "TP2", "TP3"))
    assert not reserve(args, "TP3", 11, "new-tp3").accepted


def test_broker_unknown_mismatched_odno_and_source_never_close_stage(stage_db):
    args = stage_db
    assert reserve(args, "TP1", 25, "client-tp1").accepted
    assert not observe(args, "TP1", "client-tp1", "ODNO1", 25, source="DB_ONLY").accepted
    assert not observe(args, "TP1", "client-tp1", "ODNO1", 25, date_=DAY-timedelta(days=1)).accepted
    assert observe(args, "TP1", "client-tp1", "ODNO1", 12).accepted
    assert not observe(args, "TP1", "client-tp1", "ODNO2", 25).accepted
    assert not observe(args, "TP1", "client-tp1", "ODNO1", 8).accepted
    assert not observe(args, "TP1", "client-tp1", "ODNO1", 26).accepted
    assert load_frozen_stage_states(**args)["TP1"]["status"] == "PARTIAL_FILLED"


def test_wrong_account_cycle_and_symbol_cannot_claim_or_load(stage_db):
    args = stage_db
    tampered = dict(args, account_id="foreign")
    assert not reserve(tampered, "TP1", 25, "client-tp1").accepted
    with pytest.raises(ValueError, match="CROSS_OWNER_OR_LIFECYCLE_SCOPE"):
        load_frozen_stage_states(**tampered)
    assert not reserve(dict(args, code="122630"), "TP1", 25, "client-tp1").accepted


def test_concurrent_same_tp_exactly_one_durable_claim(stage_db):
    args = stage_db
    with ThreadPoolExecutor(max_workers=6) as pool:
        result = list(pool.map(lambda _: reserve(args, "TP1", 25, "one-claim"), range(6)))
    assert sum(item.accepted for item in result) == 1
    assert all(not item.order_authorized for item in result)
    assert load_frozen_stage_states(**args)["TP1"]["status"] == "PENDING"



def _prove_three_tp_stages(stage_db):
    for stage, qty in (("TP1", 25), ("TP2", 18), ("TP3", 11)):
        assert reserve(stage_db, stage, qty, f"key-{stage}").accepted
        assert observe(stage_db, stage, f"key-{stage}", f"KIS-{stage}", qty).status == "FILLED"


def test_native_v3_runner_requires_kis_proven_tp3_and_partial_before_final(stage_db):
    args = stage_db
    assert not reserve(args, "PROFIT_DEFENSE_PARTIAL", 23, "runner-partial").accepted
    assert not reserve(args, "RUNNER_FINAL", 46, "runner-final").accepted
    _prove_three_tp_stages(args)
    assert reserve(args, "PROFIT_DEFENSE_PARTIAL", 23, "runner-partial").accepted
    assert not reserve(args, "PROFIT_DEFENSE_PARTIAL", 23, "runner-partial").accepted
    assert not reserve(args, "RUNNER_FINAL", 23, "runner-final").accepted
    assert observe(args, "PROFIT_DEFENSE_PARTIAL", "runner-partial",
                   "KIS-RUNNER-P", 0).status == "ACK"
    assert not reserve(args, "RUNNER_FINAL", 23, "runner-final").accepted
    assert observe(args, "PROFIT_DEFENSE_PARTIAL", "runner-partial",
                   "KIS-RUNNER-P", 12).status == "PARTIAL_FILLED"
    assert not reserve(args, "RUNNER_FINAL", 23, "runner-final").accepted
    assert observe(args, "PROFIT_DEFENSE_PARTIAL", "runner-partial",
                   "KIS-RUNNER-P", 23).status == "FILLED"
    assert reserve(args, "RUNNER_FINAL", 23, "runner-final").accepted
    assert not reserve(args, "RUNNER_FINAL", 23, "runner-final").accepted
    assert observe(args, "RUNNER_FINAL", "runner-final", "KIS-RUNNER-F", 23).status == "FILLED"
    states = load_frozen_stage_states(**args)
    assert states["PROFIT_DEFENSE_PARTIAL"]["status"] == "FILLED"
    assert states["RUNNER_FINAL"]["status"] == "FILLED"


def test_native_runner_final_without_partial_is_legal_but_not_duplicate(stage_db):
    args = stage_db
    _prove_three_tp_stages(args)
    assert reserve(args, "RUNNER_FINAL", 46, "runner-final-no-partial").accepted
    assert not reserve(args, "PROFIT_DEFENSE_PARTIAL", 23, "runner-partial-late").accepted
    assert observe(args, "RUNNER_FINAL", "runner-final-no-partial", "KIS-RUNNER-F", 46).status == "FILLED"

def test_concurrent_runner_partial_and_final_never_both_claim(stage_db):
    """Across native workers, same-cycle runner orders cannot race before KIS."""
    args = stage_db
    _prove_three_tp_stages(args)
    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_partial = pool.submit(reserve, args, "PROFIT_DEFENSE_PARTIAL", 23, "race-P")
        fut_final = pool.submit(reserve, args, "RUNNER_FINAL", 46, "race-F")
        outcomes = (fut_partial.result(), fut_final.result())
    assert sum(result.accepted for result in outcomes) == 1
    assert all(result.order_authorized is False for result in outcomes)
    states = load_frozen_stage_states(**args)
    active = [name for name in ("PROFIT_DEFENSE_PARTIAL", "RUNNER_FINAL")
              if states[name]["status"] == "PENDING"]
    assert len(active) == 1


@pytest.mark.parametrize("corruption", [
    {"cumulative_filled_qty": 22},
    {"target_qty": True},
    {"broker_odno": ""},
    {"broker_fill_proven": False},
])
def test_runner_final_never_unlocks_on_corrupt_partial_fill_proof(stage_db, corruption):
    """A forged FILLED label must not open the last SELL claim in PostgreSQL."""
    from trader.kr.us_profit_stage_journal import _key

    _prove_three_tp_stages(stage_db)
    assert reserve(stage_db, "PROFIT_DEFENSE_PARTIAL", 23, "partial-verified").accepted
    assert observe(
        stage_db, "PROFIT_DEFENSE_PARTIAL", "partial-verified", "KIS-P", 23
    ).status == "FILLED"

    table = schema_for_engine(stage_db["engine"]).job_checkpoints
    job_key = _key(stage_db["contract"], "PROFIT_DEFENSE_PARTIAL")
    with stage_db["engine"].begin() as conn:
        payload = dict(conn.execute(
            sa.select(table.c.payload).where(table.c.job_key == job_key)
        ).scalar_one())
        payload.update(corruption)
        conn.execute(
            sa.update(table).where(table.c.job_key == job_key).values(payload=payload)
        )

    rejected = reserve(stage_db, "RUNNER_FINAL", 23, "must-not-submit")
    assert not rejected.accepted
    assert rejected.status == "BLOCK"
    assert load_frozen_stage_states(**stage_db)["RUNNER_FINAL"]["status"] == "NOT_TRIGGERED"
