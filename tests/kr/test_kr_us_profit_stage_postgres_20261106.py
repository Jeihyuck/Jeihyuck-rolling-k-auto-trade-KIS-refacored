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


def test_broker_stage_uses_shared_connection_savepoint_and_outer_commit(stage_db):
    """P1 regression: savepoint is not a Connection; commit proof with outer tx."""
    args = stage_db
    assert reserve(args, "TP1", 7, "savepoint-tp1").accepted
    engine = args["engine"]
    with engine.begin() as conn:
        observed = observe_broker_stage(
            **args, stage="TP1", client_order_key="savepoint-tp1",
            broker_odno="SP-ODNO1", cumulative_filled_qty=7,
            broker_order_accepted=True,
            broker_fill_source="KIS_DAILY_CCLD_VERIFIED",
            source_trade_date=DAY, _conn=conn,
        )
        assert observed.accepted, observed.reason
        assert observed.status == "FILLED"
    assert load_frozen_stage_states(**args)["TP1"]["status"] == "FILLED"


def test_broker_stage_shared_connection_rolls_back_with_outer_transaction(stage_db):
    """A rolled-back economic settlement cannot leave TP journal FILLED."""
    args = stage_db
    assert reserve(args, "TP1", 7, "rollback-tp1").accepted
    with pytest.raises(RuntimeError, match="INJECTED_OUTER_ROLLBACK"):
        with args["engine"].begin() as conn:
            observed = observe_broker_stage(
                **args, stage="TP1", client_order_key="rollback-tp1",
                broker_odno="SP-ODNO2", cumulative_filled_qty=7,
                broker_order_accepted=True,
                broker_fill_source="KIS_DAILY_CCLD_VERIFIED",
                source_trade_date=DAY, _conn=conn,
            )
            assert observed.accepted, observed.reason
            raise RuntimeError("INJECTED_OUTER_ROLLBACK")
    assert load_frozen_stage_states(**args)["TP1"]["status"] == "PENDING"


def test_20261119_proven_preack_reject_unblocks_stage_only_for_new_key(stage_db):
    """KIS 40570000 explicit reject; no ODNO/fill; next fresh intent may differ in qty."""
    from trader.kr.us_profit_stage_journal import (
        release_explicitly_rejected_tp_stage, _key,
    )
    from trader.kr.us_standard_profit_parity import _canonical_sha
    from datetime import datetime, timezone
    # The large KR contracts workflow creates an intentionally minimal
    # legacy "orders" table for other tests. Our full execution-contract test
    # must not mutate that shared table or mistake it for the production
    # schema. Run this test in its own disposable PostgreSQL search_path.
    root_engine = stage_db["engine"]
    isolated_schema = "kr_rejected_tp_" + uuid4().hex
    with root_engine.begin() as conn:
        conn.exec_driver_sql(f'CREATE SCHEMA "{isolated_schema}"')
    isolated_engine = sa.create_engine(
        root_engine.url, future=True,
        connect_args={"options": f"-c search_path={isolated_schema}"},
    )
    try:
        schema_for_engine(isolated_engine).metadata.create_all(isolated_engine)
        args = dict(stage_db, engine=isolated_engine)
        _test_20261119_preack_reject_in_isolated_db(args)
    finally:
        isolated_engine.dispose()
        with root_engine.begin() as conn:
            conn.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{isolated_schema}" CASCADE')


def _test_20261119_preack_reject_in_isolated_db(args):
    from trader.kr.us_profit_stage_journal import (
        release_explicitly_rejected_tp_stage,
    )
    from trader.kr.us_standard_profit_parity import _canonical_sha
    from datetime import datetime, timezone

    contract = dict(args["contract"])
    contract["activation_mode"] = "ACTIVE_PRACTICE"
    contract["sha256"] = _canonical_sha({
        key: value for key, value in contract.items() if key != "sha256"
    })
    args["contract"] = contract
    _prove_three_tp_stages(args)
    original = "20261119-samsung-ea-8-rejected"
    trade_day = date(2026, 11, 19)
    assert reserve_tp_stage(
        **args, stage="RUNNER_FINAL", target_qty=8,
        client_order_key=original, trade_date=trade_day,
    ).accepted

    schema = schema_for_engine(args["engine"])
    order = {
        "env": "practice", "strategy": "pb1_pullback_close",
        "sid": 1, "mode": 1, "code": "005930", "market": "KR",
        "side": "SELL", "ord_type": "MARKET", "qty": 8,
        "stage": "KR_US_RUNNER_FINAL", "client_order_key": original,
        "trading_epoch_id": args["trading_epoch_id"],
        "position_cycle_id": args["position_cycle_id"],
        "status": "ERROR", "kis_odno": None, "acked_at": None,
        "submitted_at": datetime.now(timezone.utc),
        "broker_order_id": original,
        "request_json": {
            "kr_us_tp_contract": contract, "kr_us_tp_stage": "RUNNER_FINAL",
        },
        "response_json": {
            "rt_cd": "1", "msg_cd": "40570000",
            "_order_execution": {
                "side": "SELL", "code": "005930", "requested_qty": 8,
                "submitted_qty": 8, "accepted": False,
                "broker_order_id": None,
            },
        },
    }
    with args["engine"].begin() as conn:
        conn.execute(sa.insert(schema.orders).values(**order))
    release = release_explicitly_rejected_tp_stage(
        **args, stage="RUNNER_FINAL", client_order_key=original,
        trade_date=trade_day,
    )
    assert release.accepted, release.reason
    snapshot = load_frozen_stage_states(**args)["RUNNER_FINAL"]
    assert snapshot["status"] == "NOT_TRIGGERED"
    assert snapshot["rejected_client_order_key"] == original
    assert not reserve_tp_stage(
        **args, stage="RUNNER_FINAL", target_qty=8,
        client_order_key=original, trade_date=trade_day,
    ).accepted
    assert reserve_tp_stage(
        **args, stage="RUNNER_FINAL", target_qty=6,
        client_order_key="fresh-after-verified-balance",
        trade_date=trade_day + timedelta(days=1),
    ).accepted
    assert not reserve_tp_stage(
        **args, stage="RUNNER_FINAL", target_qty=6,
        client_order_key="fresh-after-verified-balance",
        trade_date=trade_day + timedelta(days=1),
    ).accepted


@pytest.mark.parametrize("mutation", [
    {"code": "OTHER_ERROR"},
    {"accepted": True},
    {"broker_order_id": "REAL-KIS-ODNO"},
    {"submitted_qty": 7},
    {"side": "BUY"},
])
def test_20261119_preack_reject_proof_never_fakes_acceptance(mutation):
    from trader.kr.us_profit_stage_journal import _explicit_kis_preack_reject

    resp = {
        "rt_cd": "1", "msg_cd": "40570000",
        "_order_execution": {
            "side": "SELL", "code": "028050",
            "requested_qty": 8, "submitted_qty": 8,
            "accepted": False, "broker_order_id": None,
        },
    }
    if "code" in mutation:
        resp["msg_cd"] = mutation["code"]
    else:
        resp["_order_execution"].update(mutation)
    assert not _explicit_kis_preack_reject(resp, code="028050", qty=8)


def test_20261119_explicit_reject_only_and_unknown_ack_stays_fenced():
    from trader.kr.us_profit_execution_router import _unresolved_sell_row

    key = "practice:pb1:028050:EXIT:SELL:KR_US_RUNNER_FINAL"
    base = {
        "code": "028050", "qty": 8, "status": "ERROR",
        "client_order_key": key,
        "broker_order_id": key,
        "kis_odno": None, "acked_at": None,
        "submitted_at": "2026-11-19T00:01:41Z",
        "request_json": {"kr_us_tp_stage": "RUNNER_FINAL"},
        "response_json": {
            "rt_cd": "1", "msg_cd": "40570000",
            "_order_execution": {
                "side": "SELL", "code": "028050",
                "requested_qty": 8, "submitted_qty": 8,
                "accepted": False, "broker_order_id": None,
            },
        },
    }
    assert _unresolved_sell_row(base) is False
    from copy import deepcopy
    unknown = deepcopy(base)
    unknown["response_json"] = {"rt_cd": "UNRESOLVED_ACK"}
    assert _unresolved_sell_row(unknown) is True
    acked = deepcopy(base)
    acked["kis_odno"] = "0000052241"
    assert _unresolved_sell_row(acked) is True
    corrupted = deepcopy(base)
    corrupted["response_json"]["_order_execution"]["accepted"] = None
    assert _unresolved_sell_row(corrupted) is True


@pytest.mark.parametrize("text", [
    "SSL connection has been closed unexpectedly",
    "psycopg.OperationalError: SSL connection closed unexpectedly",
    "OperationalError: connection closed unexpectedly",
    "SSL SYSCALL error: EOF detected",
])
def test_20261119_psycopg_ssl_disconnect_is_poisoned_connection(text):
    from trader.db.engine import _is_connection_poison_error

    assert _is_connection_poison_error(RuntimeError(text))


def test_20261119_business_reject_not_database_disconnect():
    from trader.db.engine import _is_connection_poison_error

    assert not _is_connection_poison_error(
        RuntimeError("KIS business rejection 40570000")
    )


def _native_partial_cancel_row(**overrides):
    row = {"pdno": "005930", "sll_buy_dvsn_cd": "01",
           "ord_dt": DAY.strftime("%Y%m%d"), "odno": "0000012345",
           "cncl_yn": "N", "ord_qty": "25", "tot_ccld_qty": "12",
           "cncl_cfrm_qty": "13", "rjct_qty": "0", "rmn_qty": "0",
           "tot_ccld_amt": "1272", "avg_prvs": "106"}
    row.update(overrides)
    return row


@pytest.mark.parametrize("change", [
    {"rmn_qty": "1"}, {"cncl_cfrm_qty": "12"}, {"rjct_qty": "1"},
    {"ord_qty": "24"}, {"tot_ccld_qty": "13"}, {"odno": "0000099999"},
    {"pdno": "012750"}, {"ord_dt": "20261105"}, {"cncl_yn": "Y"},
    {"tot_ccld_amt": "0"}, {"avg_prvs": "120"},
])
def test_kr401_native_cancel_proof_requires_exact_terminal_shares(change):
    from trader.kr.us_profit_stage_journal import verified_native_tp_cancellation
    assert verified_native_tp_cancellation(
        _native_partial_cancel_row(), code="005930", broker_odno="0000012345",
        trade_date=DAY, requested_qty=25, filled_qty=12)
    assert verified_native_tp_cancellation(
        _native_partial_cancel_row(**change), code="005930", broker_odno="0000012345",
        trade_date=DAY, requested_qty=25, filled_qty=12) is None


def test_kr401_partial_cancel_retry_keeps_stage_target_and_original_attempt(stage_db):
    from trader.kr.us_standard_profit_parity import _canonical_sha, calculate_next_tp_stage
    args = dict(stage_db)
    contract = {**args["contract"], "activation_mode": "ACTIVE_PRACTICE"}
    contract["sha256"] = _canonical_sha({k:v for k,v in contract.items() if k!="sha256"})
    args["contract"] = contract
    original = "kr401-old"
    assert reserve(args, "TP1", 25, original).accepted
    assert observe(args, "TP1", original, "0000012345", 12).status == "PARTIAL_FILLED"
    common = dict(**args, stage="TP1", client_order_key=original,
                  broker_odno="0000012345", cumulative_filled_qty=12,
                  broker_order_accepted=True, broker_fill_source="KIS_DAILY_CCLD_VERIFIED",
                  source_trade_date=DAY)
    assert not observe_broker_stage(**common, cancellation_row=_native_partial_cancel_row(rmn_qty="1")).accepted
    assert not reserve(args, "TP1", 13, "not-yet-proven").accepted
    result = observe_broker_stage(**common, cancellation_row=_native_partial_cancel_row())
    assert result.accepted and result.status == "RETRY_READY"
    states = load_frozen_stage_states(**args)
    assert states["TP1"]["target_qty"] == 25
    assert states["TP1"]["cumulative_filled_qty"] == 12
    assert states["TP1"]["cancelled_attempts"][0]["cancelled_qty"] == 13
    def decision(**extra):
        return calculate_next_tp_stage(contract=contract, stage_states=states,
                                      holding_qty=88, orderable_qty=88,
                                      verified_entry_price=100, executable_price=106,
                                      **extra)
    assert decision().target_qty == 13
    assert decision().action == "ACTIVE_TP"
    assert decision().order_authorized is False
    assert decision(unresolved_sell=True).action == "BLOCK"
    assert not reserve(args, "TP1", 12, "wrong-quantity").accepted
    assert not reserve(args, "TP1", 13, original).accepted
    assert not reserve(args, "TP2", 20, "premature-next-stage").accepted
    assert reserve(args, "TP1", 13, "kr401-new").accepted
    pending = load_frozen_stage_states(**args)["TP1"]
    assert pending["target_qty"] == 25 and pending["attempt_target_qty"] == 13
    assert observe_broker_stage(**common, cancellation_row=_native_partial_cancel_row()).accepted
    assert load_frozen_stage_states(**args)["TP1"]["client_order_key"] == "kr401-new"
    assert not observe(args, "TP1", "kr401-new", "0000012346", 14).accepted
    assert observe(args, "TP1", "kr401-new", "0000012346", 13).status == "FILLED"
    final = load_frozen_stage_states(**args)["TP1"]
    assert final["cumulative_filled_qty"] == final["target_qty"] == 25
    assert final["broker_fill_proven"] is True
    assert observe_broker_stage(**common, cancellation_row=_native_partial_cancel_row()).accepted
    assert load_frozen_stage_states(**args)["TP1"] == final
    assert reserve(args, "TP2", 18, "new-tp2").accepted


@pytest.mark.parametrize("stage", ["PROFIT_DEFENSE_PARTIAL", "RUNNER_FINAL"])
def test_kr401_native_runner_cancellation_retains_original_target(stage_db, stage):
    from trader.kr.us_standard_profit_parity import _canonical_sha
    args = dict(stage_db)
    contract = {**args["contract"], "activation_mode": "ACTIVE_PRACTICE"}
    contract["sha256"] = _canonical_sha({k:v for k,v in contract.items() if k!="sha256"})
    args["contract"] = contract
    _prove_three_tp_stages(args)
    old, new = "kr401-runner-original", "kr401-runner-residual"
    assert reserve(args, stage, 25, old).accepted
    assert observe(args, stage, old, "0000012345", 12).status == "PARTIAL_FILLED"
    common = dict(**args, stage=stage, client_order_key=old, broker_odno="0000012345",
                  cumulative_filled_qty=12, broker_order_accepted=True,
                  broker_fill_source="KIS_DAILY_CCLD_VERIFIED", source_trade_date=DAY)
    assert not observe_broker_stage(**common, cancellation_row=_native_partial_cancel_row(rmn_qty="1")).accepted
    assert observe_broker_stage(**common, cancellation_row=_native_partial_cancel_row()).status == "RETRY_READY"
    assert not reserve(args, stage, 12, new).accepted
    assert not reserve(args, stage, 13, old).accepted
    assert reserve(args, stage, 13, new).accepted
    assert observe_broker_stage(**common, cancellation_row=_native_partial_cancel_row()).accepted
    assert observe(args, stage, new, "0000012346", 13).status == "FILLED"
    result = load_frozen_stage_states(**args)[stage]
    assert result["target_qty"] == result["cumulative_filled_qty"] == 25
    assert result["broker_fill_proven"] is True
