from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

from trader.kr.infinite.config import InfiniteConfig
from trader.kr.infinite.integration import run_session_hook
from trader.kr.infinite.models import InfiniteState
from trader.kr.regime import (KRExecutionPolicy, KRMarketExecutionPolicy, KRMarketState,
                              KRRegimeSnapshot)


class Repo:
    def __init__(self): self.state = InfiniteState(); self.saved = []; self.archived = []
    def ensure_schema(self): pass
    @contextmanager
    def critical_section(self): yield True
    def load_state(self, symbol): return self.state
    def save_state(self, state): self.state = state; self.saved.append(state)
    def reconcile_evidence(self, state, **kwargs): return state
    def archive_cycle(self, state): self.archived.append(state)


def snap(state):
    now = datetime.now(timezone.utc)
    local = KRMarketState("KOSPI", 10, state, "OK")
    other = KRMarketState("KOSDAQ", -100, "KR_DEFENSE_CRASH", "OK")
    policies = {"KOSPI": KRMarketExecutionPolicy("KOSPI", "OK", state in {
        "KR_SHOCK_REBOUND_CONFIRMED", "KR_NORMAL", "KR_RISK_ON", "KR_STRONG_RISK_ON"}, 1, None),
        "KOSDAQ": KRMarketExecutionPolicy("KOSDAQ", "OK", False, 0, 0)}
    return KRRegimeSnapshot(now.isoformat(), state, {"KOSPI": local, "KOSDAQ": other}, "OK",
        KRExecutionPolicy(1, None, True, True), policies)


@pytest.mark.parametrize("state,expected", [
    ("KR_DEFENSE_CRASH", 0), ("KR_DEFENSE_RISK_OFF", 0),
    ("KR_DEFENSE_CAUTION", 0), ("KR_SHOCK_REBOUND_PENDING", 0),
    ("KR_SHOCK_REBOUND_CONFIRMED", 1), ("KR_NORMAL", 1),
    ("KR_RISK_ON", 1), ("KR_STRONG_RISK_ON", 1),
])
def test_hook_to_router_for_all_canonical_states(monkeypatch, state, expected):
    monkeypatch.setattr(InfiniteConfig, "from_env", classmethod(lambda cls: InfiniteConfig(deployable_buy_policy=True)))
    for key, value in {"STRATEGY_MODE": "LIVE", "KIS_ENV": "real", "LIVE_TRADING_ENABLED": "1",
                       "DRY_RUN": "0", "DISABLE_LIVE_TRADING": "0", "FORCE_BLOCK_LIVE": "0"}.items():
        monkeypatch.setenv(key, value)
    routed = []
    result = run_session_hook(trading_date=datetime.now(timezone.utc).date(), positions=[], price=10_000,
        quote_at=datetime.now(timezone.utc), snapshot=snap(state), orderable_cash=1_000_000,
        pending=False, daily_filled_buy_notional=0, route=lambda intent: routed.append(intent) or {"rt_cd": "0"},
        account_loss_kill_switch=False, repository=Repo(), evidence_fills=[], evidence_orders=[])
    assert len(routed) == expected
    if routed:
        assert routed[0]["quantity"] == 37 and routed[0]["strategy_id"] == "KR_KODEX_LEVERAGE_INFINITE_V1"


def test_global_gate_blocks_before_router(monkeypatch):
    monkeypatch.setattr(InfiniteConfig, "from_env", classmethod(lambda cls: InfiniteConfig(deployable_buy_policy=True)))
    monkeypatch.setenv("FORCE_BLOCK_LIVE", "1")
    routed = []
    result = run_session_hook(trading_date=datetime.now(timezone.utc).date(), positions=[], price=10_000,
        quote_at=datetime.now(timezone.utc), snapshot=snap("KR_NORMAL"), orderable_cash=1_000_000,
        pending=False, daily_filled_buy_notional=0, route=lambda intent: routed.append(intent),
        account_loss_kill_switch=False, repository=Repo(), evidence_fills=[], evidence_orders=[])
    assert not routed and result["status"] == "BLOCKED"
