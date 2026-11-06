"""Real disposable PostgreSQL journal tests: single writer, immutable scope, restart."""

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from types import SimpleNamespace
from uuid import uuid4

import pytest
import sqlalchemy as sa

import trader.kr.historical_recovery_journal as journal
from trader.db.schema import schema_for_engine


@pytest.fixture
def pg_lane(monkeypatch):
    url = os.getenv("PBCORE_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("Disposable PostgreSQL URL not available")
    engine = sa.create_engine(url, future=True)
    schema_for_engine(engine).metadata.create_all(engine)
    account = "practice:test:" + uuid4().hex
    scope = SimpleNamespace(env="practice", account_key=account, trading_epoch_id="epoch-" + uuid4().hex)
    monkeypatch.setattr(journal, "resolve_kr_recovery_scope", lambda **_: scope)
    monkeypatch.setenv("KR_HISTORICAL_RECOVERY_ONCE_ENABLED", "1")
    monkeypatch.setenv("KIS_ENV", "practice")
    yield engine, scope
    engine.dispose()


def _select(engine, *, day=date(2026, 11, 6)):
    return journal.choose_historical_lane(
        engine=engine, kis=object(), env="practice",
        strategy="pb1_pullback_close", trading_date=day,
    )


def _success():
    return dict(
        ok=True, historical_recovery_executed=True,
        historical_repair_fastpath=False,
        unresolved_broker_activity=False,
        holdings_error=None, guard_reason=None,
        frozen_contract_invariant_violations=0,
    )


def test_single_owner_then_durable_complete_after_restart(pg_lane):
    engine, scope = pg_lane
    first = _select(engine)
    assert first.mode == "FULL" and first.fencing_token
    assert _select(engine).mode == "CURRENT_ONLY_BLOCK"
    assert journal.record_historical_result(engine=engine, lane=first, result=_success())
    assert _select(engine).mode == "CURRENT_ONLY"
    # An old nonce is fenced even after the exact broker evidence is recorded.
    assert not journal.record_historical_result(engine=engine, lane=first, result=_success())
    scope.trading_epoch_id = "changed"  # New generation must bootstrap again.
    assert _select(engine).mode == "FULL"


def test_unverified_full_result_never_authorizes_future_buy(pg_lane):
    engine, _ = pg_lane
    first = _select(engine)
    assert first.mode == "FULL"
    assert not journal.record_historical_result(
        engine=engine, lane=first, result={"ok": True, "historical_repair_fastpath": False},
    )
    assert _select(engine).mode == "CURRENT_ONLY_BLOCK"


def test_concurrent_journal_claims_only_one_full_recovery(pg_lane):
    engine, _ = pg_lane
    with ThreadPoolExecutor(max_workers=5) as pool:
        modes = list(pool.map(lambda _: _select(engine).mode, range(5)))
    assert modes.count("FULL") == 1
    assert modes.count("CURRENT_ONLY_BLOCK") == 4


def test_independent_am_and_close_only_one_each(pg_lane):
    engine, _ = pg_lane
    am = _select(engine)
    assert am.mode == "FULL"
    close = journal.choose_historical_lane(
        engine=engine, kis=object(), env="practice",
        strategy="pb1_pullback_close", trading_date=date(2026, 11, 6),
        lane_name="CLOSE",
    )
    assert close.mode == "FULL"
    assert am.job_key != close.job_key
    assert journal.record_historical_result(engine=engine, lane=am, result=_success())
    assert journal.record_historical_result(engine=engine, lane=close, result=_success())
    assert _select(engine).mode == "CURRENT_ONLY"
    assert journal.choose_historical_lane(
        engine=engine, kis=object(), env="practice",
        strategy="pb1_pullback_close", trading_date=date(2026, 11, 6),
        lane_name="CLOSE",
    ).mode == "CURRENT_ONLY"


def test_close_failed_does_not_mark_completed(pg_lane):
    engine, _ = pg_lane
    close = journal.choose_historical_lane(
        engine=engine, kis=object(), env="practice",
        strategy="pb1_pullback_close", trading_date=date(2026, 11, 6),
        lane_name="CLOSE",
    )
    assert close.mode == "FULL"
    assert not journal.record_historical_result(
        engine=engine, lane=close,
        result={"ok": True, "historical_repair_fastpath": False},
    )
    assert journal.choose_historical_lane(
        engine=engine, kis=object(), env="practice",
        strategy="pb1_pullback_close", trading_date=date(2026, 11, 6),
        lane_name="CLOSE",
    ).mode == "CURRENT_ONLY_BLOCK"
