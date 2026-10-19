"""October 19 KR tick interruption during DB pool pre-ping cannot evade epoch safety."""
from __future__ import annotations

from uuid import uuid4

import pytest
import sqlalchemy as sa

from trader.account_state import get_account_key
from trader.db import trading_epoch
from trader.db.schema import schema_for_engine


class CheckoutPoisonEngine:
    def __init__(self):
        self.disposed = False
        self.connect_calls = 0

    def connect(self):
        self.connect_calls += 1
        raise RuntimeError(
            "(psycopg.ProgrammingError) can't change 'autocommit' now: "
            "connection in transaction status ACTIVE"
        )

    def dispose(self):
        self.disposed = True


def test_interrupt_during_epoch_connection_checkout_disposes_pool_and_fails_closed():
    engine = CheckoutPoisonEngine()
    with pytest.raises(trading_epoch.TradingEpochError, match="ACTIVE_TRADING_EPOCH_UNAVAILABLE"):
        trading_epoch.active_trading_epoch_id(engine, env="practice", required=True)
    assert engine.connect_calls == 1
    assert engine.disposed


def test_interrupt_never_returns_an_epoch_for_fail_open_callers():
    engine = CheckoutPoisonEngine()
    assert trading_epoch.active_trading_epoch_id(engine, env="practice", required=False) is None
    assert engine.disposed


def test_successful_epoch_lookup_still_selects_exactly_one_active_account_epoch():
    engine = sa.create_engine("sqlite:///:memory:")
    schema = schema_for_engine(engine)
    schema.metadata.create_all(engine)
    expected = str(uuid4())
    with engine.begin() as conn:
        conn.execute(schema.trading_epochs.insert().values(
            trading_epoch_id=expected, env="practice",
            account_id=get_account_key(env="practice"),
            status="ACTIVE", reason="offline test",
        ))
    assert trading_epoch.active_trading_epoch_id(engine, env="practice", required=True) == expected
    engine.dispose()
