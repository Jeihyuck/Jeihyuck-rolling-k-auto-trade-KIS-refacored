"""KR historical recovery lanes: explicit opt-in, durable proof or fail-closed."""

import os
from datetime import date
from types import SimpleNamespace

import pytest

from trader.kr.historical_recovery_journal import (
    RecoveryLane, choose_historical_lane, lane_enabled,
    record_historical_result,
)


class UnexpectedEngine:
    @property
    def dialect(self):
        raise AssertionError("Disabled lane must not touch the DB")


def test_default_legacy_no_extra_db_or_broker_io(monkeypatch):
    monkeypatch.delenv("KR_HISTORICAL_RECOVERY_ONCE_ENABLED", raising=False)
    monkeypatch.setenv("KIS_ENV", "practice")
    assert not lane_enabled("practice")
    lane = choose_historical_lane(
        engine=UnexpectedEngine(), kis=object(), env="practice",
        strategy="pb1_pullback_close", trading_date=date(2026, 11, 6),
    )
    assert lane.mode == "LEGACY"
    assert lane.historical_recovery


def test_no_us_or_real_money_activation(monkeypatch):
    monkeypatch.setenv("KR_HISTORICAL_RECOVERY_ONCE_ENABLED", "1")
    monkeypatch.setenv("KIS_ENV", "real")
    assert not lane_enabled("practice")
    monkeypatch.setenv("KIS_ENV", "practice")
    assert not lane_enabled("real")
    assert lane_enabled("practice")


@pytest.mark.parametrize("mode,historical,can_buy", [
    ("FULL", True, True),
    ("LEGACY", True, True),
    ("CURRENT_ONLY", False, True),
    ("CURRENT_ONLY_BLOCK", False, False),
])
def test_state_semantics_never_treat_incomplete_recovery_as_buy_proof(
    mode, historical, can_buy,
):
    lane = RecoveryLane(mode, job_key="key", fencing_token="nonce")
    assert lane.historical_recovery is historical
    assert lane.allow_buy_from_recovery is can_buy


def test_only_claimed_full_recovery_can_persist_terminal_state():
    assert not record_historical_result(
        engine=UnexpectedEngine(),
        lane=RecoveryLane("FULL"),
        result={"ok": True, "historical_repair_fastpath": True},
    )
    assert not record_historical_result(
        engine=UnexpectedEngine(),
        lane=RecoveryLane("CURRENT_ONLY_BLOCK"),
        result={"ok": True},
    )


def test_read_only_current_lane_is_not_implicitly_historical_complete():
    assert record_historical_result(
        engine=UnexpectedEngine(),
        lane=RecoveryLane("CURRENT_ONLY"),
        result={"ok": True},
    )
