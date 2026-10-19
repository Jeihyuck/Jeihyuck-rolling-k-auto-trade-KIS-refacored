"""October 13 PB1 STOP_HIT SELL false-ambiguous repair, with strict proofs.

No KIS calls. The pinned legacy source SHA calls get_positions() before its
broker HTTP order boundary; the exception text is unique to that precheck.
Other historical/uncertain SELL attempts must remain protected.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db.repos import OrdersRepo
from trader.db.schema import schema_for_engine
from trader.kr.legacy_sell_prebroker import (
    LEGACY_PROVEN_PRE_HTTP_SHA, LEGACY_RESPONSE, proven_oct13_prehttp_sell,
)


def test_exact_provenance_required_before_any_legacy_sell_repair():
    row = {
        "env": "practice", "git_sha": LEGACY_PROVEN_PRE_HTTP_SHA,
        "trade_date": date(2026, 10, 13), "code": "036540",
        "strategy_owner": "PB1", "action": "SELL:STOP_HIT_EFFECTIVE:FULL_EXIT",
        "action_state": "UNCERTAIN", "attempt_state": "UNRESOLVED",
        "order_status": "UNRESOLVED_ACK", "stage": "FULL_EXIT",
        "side": "SELL", "strategy": "pb1_pullback_close",
        "acked_at": None, "kis_odno": None, "broker_order_id": None,
        "response_json": dict(LEGACY_RESPONSE),
    }
    assert proven_oct13_prehttp_sell(row)
    for changes in (
        {"git_sha": "new_or_unknown"},
        {"trade_date": date(2026, 10, 14)},
        {"response_json": {**LEGACY_RESPONSE, "msg1": "connection lost"}},
        {"response_json": {**LEGACY_RESPONSE, "broker_submit": False}},
        {"response_json": {**LEGACY_RESPONSE, "msg_cd": "HTTP_500"}},
        {"acked_at": datetime(2026, 10, 13, tzinfo=timezone.utc)},
        {"kis_odno": "00123"},
        {"order_status": "ACKED"},
        {"strategy_owner": "KR_INFINITE"},
        {"code": "777777"},
    ):
        assert not proven_oct13_prehttp_sell({**row, **changes})


def _repo_with_active_epoch():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    epoch = str(uuid4())
    with engine.begin() as conn:
        conn.execute(schema.trading_epochs.insert().values(
            trading_epoch_id=epoch, env="practice",
            account_id=get_account_key(env="practice"), status="ACTIVE",
            reason="offline legacy proof fixture",
        ))
    return engine, schema, OrdersRepo(engine), epoch


def _legacy_sell(engine, schema, repo, epoch, *, code="036540", original_sha=LEGACY_PROVEN_PRE_HTTP_SHA,
                 response=None, status="UNRESOLVED_ACK", with_fill=False):
    run_id = str(uuid4())
    key = f"practice:pb1_pullback_close:2026-10-13:{code}:EXIT:SELL:FULL_EXIT:{uuid4()}"
    attempt_id = f"attempt:{uuid4()}"
    identity, acquired = repo.claim_execution_action(
        env="practice", market="KR", strategy_owner="PB1",
        lifecycle_id=f"legacy-lifecycle:{code}:{uuid4()}",
        action="SELL:STOP_HIT_EFFECTIVE:FULL_EXIT",
        trade_date=date(2026, 10, 13), attempt_id=attempt_id,
        requested_qty=5, client_order_key=key, fresh_validation=True,
    )
    assert acquired.acquired
    order_id = str(uuid4())
    with engine.begin() as conn:
        conn.execute(schema.runs.insert().values(
            run_id=run_id, env="practice", strategy="pb1_pullback_close",
            dry_run=False, status="OK", git_sha=original_sha,
        ))
        conn.execute(schema.orders.insert().values(
            order_id=order_id, trading_epoch_id=epoch, env="practice",
            run_id=run_id, strategy="pb1_pullback_close",
            sid=1, mode=1, code=code, market="KOSPI", side="SELL",
            ord_type="MARKET", qty=5, limit_price=0, stage="FULL_EXIT",
            client_order_key=key, status=status, request_json={},
            response_json=response if response is not None else dict(LEGACY_RESPONSE),
            # The 2026-10-13 bug wrote submitted_at optimistically even though
            # get_positions() failed BEFORE the broker POST.
            submitted_at=datetime(2026, 10, 13, 0, 3, tzinfo=timezone.utc),
        ))
        if with_fill:
            conn.execute(schema.fills.insert().values(
                fill_id=str(uuid4()), trading_epoch_id=epoch, env="practice",
                order_id=order_id, code=code, side="SELL", qty=5,
                price=10000.0, fee=0.0, tax=0.0,
                filled_at=datetime(2026, 10, 13, 0, 5, tzinfo=timezone.utc),
                raw_json={},
            ))
    repo._execution_claim_repo.record_observation(
        identity, attempt_id=attempt_id, state="UNRESOLVED",
        cumulative_filled_qty=None, authoritative=False,
    )
    return identity, key


def test_oct13_strict_four_stop_claims_recover_atomically_without_order_http():
    engine, schema, repo, epoch = _repo_with_active_epoch()
    recorded = [_legacy_sell(engine, schema, repo, epoch, code=code) for code in (
        "036540", "039030", "042700", "067310",
    )]
    assert repo.execution_claim_health(market="KR")["unresolved_execution_actions"] == 4
    assert repo.recover_audited_oct13_kr_pb1_prehttp_sells(env="practice") == 4
    for identity, key in recorded:
        assert repo._execution_claim_repo.get(identity).action_state == "OPEN"
        with engine.connect() as conn:
            order = conn.execute(sa.select(schema.orders).where(
                schema.orders.c.client_order_key == key,
            )).mappings().one()
            attempt = conn.execute(sa.select(schema.execution_attempts).where(
                schema.execution_attempts.c.client_order_key == key,
            )).mappings().one()
        assert order["status"] == "ERROR"
        assert order["response_json"]["broker_submit"] is False
        assert order["response_json"]["legacy_misclassified_response"]["broker_submit"] is True
        assert order["submitted_at"] is not None  # immutable evidence preserved
        assert attempt["attempt_state"] == "ABANDONED_PRE_SUBMIT"
        assert attempt["authoritative"] is True
    assert repo.execution_claim_health(market="KR")["unresolved_execution_actions"] == 0
    assert repo.recover_audited_oct13_kr_pb1_prehttp_sells(env="practice") == 0
    engine.dispose()


def test_ambiguous_sha_mismatch_and_confirmed_fill_stay_fenced():
    engine, schema, repo, epoch = _repo_with_active_epoch()
    bad = [
        _legacy_sell(engine, schema, repo, epoch, code="036540", original_sha="unknown"),
        _legacy_sell(engine, schema, repo, epoch, code="039030",
                     response={**LEGACY_RESPONSE, "msg1": "timeout after HTTP"}),
        _legacy_sell(engine, schema, repo, epoch, code="042700", with_fill=True),
        _legacy_sell(engine, schema, repo, epoch, code="067310", status="CREATED"),
    ]
    assert repo.recover_audited_oct13_kr_pb1_prehttp_sells(env="practice") == 0
    for identity, _ in bad:
        assert repo._execution_claim_repo.get(identity).action_state == "UNCERTAIN"
    engine.dispose()
