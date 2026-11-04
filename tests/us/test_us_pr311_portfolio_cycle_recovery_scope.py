"""US three-gate recovery proof (account/trading + portfolio + cycle).

This test suite runs without KIS HTTP or production DB mutation.
"""
import hashlib
from pathlib import Path

import pytest

from trader.us.execution.recovery_identity import (
    USRecoveryIdentityError,
    assert_recovery_scope,
    bind_original_us_order_scope,
    current_us_portfolio_epoch,
)


ACCT = "practice:fixture:01"
ACCOUNT_HASH = hashlib.sha256(ACCT.encode()).hexdigest()


class Cursor:
    def __init__(self, rows):
        self.rows = rows
    def all(self):
        return self.rows


class DB:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []
    def execute(self, query, args):
        self.calls.append((str(query), dict(args)))
        return Cursor(self.rows)


def _intent(**changes):
    item = {
        "symbol": "SYY", "side": "BUY", "position_action": "NEW_POSITION_BUY",
        "strategy_owner": "US_STANDARD", "trade_date": "2026-11-04",
        "client_order_key": "buy-base-key", "meta": {"account_id": ACCT},
    }
    item.update(changes)
    return item


def _bind(item, db=None):
    return bind_original_us_order_scope(
        item, conn=db or DB([("owner-portfolio-20261104",)]),
        env="practice", account_key=ACCT, trading_epoch_id="active-epoch",
    )


def test_same_parent_new_buy_retries_share_exact_cycle_and_scope():
    first = _intent()
    retry = _intent(client_order_key="different-proven-unsent-retry")
    one = _bind(first)
    two = _bind(retry)
    assert one.position_cycle_id == two.position_cycle_id
    assert first["meta"]["position_lifecycle_id"] == one.position_cycle_id
    assert first["meta"]["broker_account_hash"] == ACCOUNT_HASH
    assert first["meta"]["account_id"] == ACCT
    assert first["portfolio_epoch_id"] == "owner-portfolio-20261104"


def test_new_position_in_later_day_new_cycle_not_reused():
    old = _intent()
    new = _intent(trade_date="2026-11-05")
    assert _bind(old).position_cycle_id != _bind(new).position_cycle_id


@pytest.mark.parametrize("changes,reason", [
    ({"portfolio_epoch_id": "other-portfolio"}, "PORTFOLIO_EPOCH_ID_MISMATCH"),
    ({"trading_epoch_id": "different-epoch"}, "TRADING_EPOCH_ID_MISMATCH"),
    ({"position_lifecycle_id": "conflicting-cycle", "position_cycle_id": "other"},
     "ORIGINAL_IDENTITY_CONFLICT"),
    ({"broker_account_hash": "another-account"}, "ACCOUNT_ID_MISMATCH"),
    ({"meta": {"account_id": "wrong:account:01"}}, "ACCOUNT_ID_MISMATCH"),
])
def test_new_order_rejects_incorrect_preexisting_identity(changes, reason):
    with pytest.raises(USRecoveryIdentityError, match=reason):
        _bind(_intent(**changes))


def test_existing_add_or_sell_must_carry_original_cycle():
    for side, action in (("SELL", "PROTECTIVE"), ("BUY", "ADD_TO_EXISTING_BUY")):
        with pytest.raises(USRecoveryIdentityError, match="POSITION_CYCLE_UNVERIFIED"):
            _bind(_intent(side=side, position_action=action))
        item = _intent(side=side, position_action=action,
                       position_lifecycle_id="real-source-cycle")
        assert _bind(item).position_cycle_id == "real-source-cycle"


def test_portfolio_epoch_must_belong_to_current_account_and_owner():
    db = DB([("current-owner-epoch",)])
    assert current_us_portfolio_epoch(
        db, env="practice", account_key=ACCT, trading_epoch_id="epoch-current",
        strategy_owner="US_STANDARD",
    ) == "current-owner-epoch"
    query, params = db.calls[0]
    assert "account_id=:account" in query
    assert "trading_epoch_id=:epoch" in query
    assert "status='ACTIVE'" in query
    assert "sid=0 AND mode=0" in query
    assert "strategy=:owner" in query
    assert params == {
        "env": "practice", "account": ACCT,
        "epoch": "epoch-current", "owner": "US_STANDARD",
    }
    for rows in ([], [("a",), ("b",)]):
        with pytest.raises(USRecoveryIdentityError, match="PORTFOLIO_EPOCH_"):
            current_us_portfolio_epoch(
                DB(rows), env="practice", account_key=ACCT,
                trading_epoch_id="epoch-current", strategy_owner="US_STANDARD",
            )


@pytest.mark.parametrize("field,bad", [
    ("broker_account_hash", "different-account"),
    ("trading_epoch_id", "different-epoch"),
    ("portfolio_epoch_id", "kr-not-us-owner-epoch"),
    ("position_cycle_id", "prior-position-cycle"),
    ("strategy_owner", "TQQQ_INFINITE"),
    ("env", "real"),
])
def test_historical_replay_rejects_every_mismatched_recovery_dimension(field, bad):
    older = _intent()
    _bind(older)
    active = dict(older)
    active["meta"] = dict(older["meta"])
    active[field] = bad
    with pytest.raises(USRecoveryIdentityError, match="MISMATCH"):
        assert_recovery_scope(older, active,
                              original_meta=older["meta"], current_meta=active["meta"])


def test_missing_legacy_identity_cannot_be_backfilled_from_current_tick():
    current = _intent()
    _bind(current)
    old = {"env": "practice", "strategy_owner": "US_STANDARD",
           "trading_epoch_id": "active-epoch", "symbol": "SYY"}
    with pytest.raises(USRecoveryIdentityError, match="UNVERIFIED"):
        assert_recovery_scope(old, current, current_meta=current["meta"])


def test_original_matching_identity_is_verifiable():
    order = _intent()
    _bind(order)
    assert assert_recovery_scope(order, dict(order),
                                 original_meta=order["meta"],
                                 current_meta=dict(order["meta"])) is None


def test_migration_never_guesses_legacy_epoch_or_deletes_orders():
    sql = (Path(__file__).resolve().parents[2]
           / "migrations" / "0062_us_portfolio_position_cycle_recovery_scope.sql"
           ).read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS portfolio_epoch_id" in sql
    assert "ADD COLUMN IF NOT EXISTS position_cycle_id" in sql
    assert "uq_us_orders_broker_account_epoch_day_0062" in sql
    assert "uq_us_fills_broker_account_epoch_0062" in sql
    assert "DROP INDEX IF EXISTS public.uq_us_orders_trade_date_order_no_nonblank" in sql
    assert "UPDATE public.us_orders" not in sql.upper()
    assert "DELETE FROM public.us_" not in sql.upper()
    assert "INSERT INTO public.portfolio_epochs" not in sql.upper()
