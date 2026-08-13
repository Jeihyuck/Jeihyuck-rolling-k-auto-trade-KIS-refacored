from datetime import datetime, timezone

import pytest

from trader.kr.infinite.config import InfiniteConfig
from trader.kr.infinite.risk_adapter import REGIME_ACTIONS, assess
from trader.kr.regime import (KRExecutionPolicy, KRMarketExecutionPolicy, KRMarketState,
                              KRRegimeSnapshot, STATE_ORDER)


def snapshot(state, *, kosdaq="KR_NORMAL", quality="OK", allow=True, add=True, as_of=None):
    now = datetime.now(timezone.utc)
    states = {"KOSPI": KRMarketState("KOSPI", 1, state, quality),
              "KOSDAQ": KRMarketState("KOSDAQ", -99, kosdaq, "OK")}
    policies = {m: KRMarketExecutionPolicy(m, v.data_quality, allow if m == "KOSPI" else False, 1, None)
                for m, v in states.items()}
    return KRRegimeSnapshot(as_of or now.isoformat(), state, states, "OK",
        KRExecutionPolicy(1, None, True, add), policies)


def test_defaults_are_enabled_and_real(monkeypatch):
    for key in ("ENABLED", "REAL_ORDER", "ALLOW_BUY", "ALLOW_SELL", "SYMBOL", "CAPITAL_KRW", "UNITS", "UNIT_KRW"):
        monkeypatch.delenv("KR_INFINITE_" + key, raising=False)
    c = InfiniteConfig.from_env(); c.validate()
    assert c.enabled and c.real_order and c.allow_buy and c.allow_sell
    assert (c.symbol, c.capital_krw, c.units, c.unit_krw) == ("122630", 15_000_000, 40, 375_000)


def test_mapping_is_exactly_canonical():
    assert set(REGIME_ACTIONS) == set(STATE_ORDER)


@pytest.mark.parametrize("state,allowed,decision", [
    ("KR_DEFENSE_CRASH", False, "BLOCK_BUY_CRASH"),
    ("KR_DEFENSE_RISK_OFF", False, "BLOCK_BUY_RISK_OFF"),
    ("KR_DEFENSE_CAUTION", False, "BLOCK_BUY_CAUTION"),
    ("KR_SHOCK_REBOUND_PENDING", False, "BLOCK_BUY_REBOUND_PENDING"),
    ("KR_SHOCK_REBOUND_CONFIRMED", True, "ALLOW_RECOVERY_PROBE"),
    ("KR_NORMAL", True, "ALLOW_ROUTINE_BUY"),
    ("KR_RISK_ON", True, "ALLOW_ROUTINE_BUY"),
    ("KR_STRONG_RISK_ON", True, "ALLOW_ROUTINE_BUY"),
])
def test_all_eight_states(state, allowed, decision):
    snap = snapshot(state); got = assess(snap, datetime.now(timezone.utc).date())
    assert (got.allow_buy, got.decision) == (allowed, decision)


def test_unknown_blocked_and_policy_mismatch():
    today = datetime.now(timezone.utc).date()
    assert assess(snapshot("FUTURE_STATE"), today).decision == "BLOCK_BUY_UNKNOWN_STATE"
    assert assess(snapshot("KR_NORMAL", quality="BLOCKED"), today).decision == "BLOCK_BUY_DATA_QUALITY"
    assert assess(snapshot("KR_NORMAL", add=False), today).decision == "BLOCK_BUY_POLICY_MISMATCH"


def test_stale_and_account_kill_switch_block():
    today = datetime.now(timezone.utc).date()
    assert assess(snapshot("KR_NORMAL", as_of="2020-01-01T00:00:00+00:00"), today).decision == "BLOCK_BUY_STALE_REGIME"
    assert assess(snapshot("KR_NORMAL"), today, account_loss_kill_switch=True).decision == "BLOCK_BUY_ACCOUNT_KILL_SWITCH"


def test_kosdaq_alone_does_not_veto_healthy_kospi():
    snap = snapshot("KR_NORMAL", kosdaq="KR_DEFENSE_CRASH")
    assert assess(snap, datetime.now(timezone.utc).date()).allow_buy
