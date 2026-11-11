"""Practice KR US TP router contract evidence, owner isolation and fail-closed tests."""
import pytest

from trader.kr.us_standard_profit_parity import build_new_swing_parity_contract
from trader.kr import us_profit_execution_router as router


class FakeKis:
    env = "practice"

    def __init__(self, quote=None):
        self.quote = quote if quote is not None else {"bid": 109.0}
        self.calls = 0

    def get_price_snapshot(self, code, market="J"):
        assert (code, market) == ("005930", "J")
        self.calls += 1
        return self.quote


def setup_contract(monkeypatch):
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("KR_SWING_US_PROFIT_MODE", "ACTIVE_PRACTICE")
    c = build_new_swing_parity_contract(
        strategy_owner="PB1", exit_policy_family="SWING_STAGED_EXIT",
        trade_horizon="SWING", position_action="NEW_POSITION_BUY",
        account_id="practice:account", trading_epoch_id="epoch-1",
        position_cycle_id="cycle-1", runner_version="US_V3", code="005930",
    )
    pos = {
        "code": "005930", "strategy": "pb1_pullback_close", "status": "OPEN",
        "trade_horizon": "SWING", "exit_policy_family": "SWING_STAGED_EXIT",
        "holding_source": "kis_balance", "kis_qty": 100, "qty": 100,
        "orderable_qty": 100, "avg_buy_price": 100,
        "position_cycle_id": "cycle-1",
        "position_meta": {"kr_us_profit_contract": c},
    }
    monkeypatch.setattr(router, "get_account_key", lambda **kw: "practice:account")
    monkeypatch.setattr(router, "active_trading_epoch_id", lambda *args, **kw: "epoch-1")
    monkeypatch.setattr(router, "load_frozen_stage_states", lambda **kw: {
        "TP1": {"status": "NOT_TRIGGERED"},
        "TP2": {"status": "NOT_TRIGGERED"},
        "TP3": {"status": "NOT_TRIGGERED"},
    })
    return pos


def test_tp1_at_9pct_is_sequential_only(monkeypatch):
    pos = setup_contract(monkeypatch)
    kis = FakeKis()
    result = router.evaluate_active_kr_tp(
        position=pos, engine=object(), kis=kis, env="practice", code="005930",
    )
    assert result["action"] == "ACTIVE_TP"
    assert result["stage"] == "TP1" and result["qty"] == 25
    assert kis.calls == 1


def test_unresolved_tp1_never_advances_or_submits(monkeypatch):
    pos = setup_contract(monkeypatch)
    monkeypatch.setattr(router, "load_frozen_stage_states", lambda **kw: {
        "TP1": {"status": "ACK"},
        "TP2": {"status": "NOT_TRIGGERED"},
        "TP3": {"status": "NOT_TRIGGERED"},
    })
    result = router.evaluate_active_kr_tp(
        position=pos, engine=object(), kis=FakeKis(), env="practice", code="005930",
    )
    assert result["action"] == "BLOCK" and result["stage"] == "TP1"


@pytest.mark.parametrize("mutated,reason", [
    ({"strategy": "KR_INFINITE_V1"}, "KR_TP_OWNER_NOT_PB1"),
    ({"holding_source": "db"}, "KR_TP_KIS_HOLDING_SOURCE_UNVERIFIED"),
    ({"position_cycle_id": "unrelated-cycle"}, "KR_TP_POSITION_CYCLE_UNVERIFIED"),
    ({"kis_qty": 0}, "KR_TP_KIS_DB_QTY_MISMATCH"),
    ({"qty": 101}, "KR_TP_KIS_DB_QTY_MISMATCH"),
    ({"exit_policy_family": "CORE_TREND_FOLLOW"}, "KR_TP_POLICY_OWNER_OR_CYCLE_CONFLICT"),
])
def test_owner_cycle_and_kis_holdings_fence(monkeypatch, mutated, reason):
    pos = setup_contract(monkeypatch)
    pos.update(mutated)
    out = router.evaluate_active_kr_tp(
        position=pos, engine=object(), kis=FakeKis(), env="practice", code="005930",
    )
    assert out["action"] == "BLOCK" and out["reason"] == reason


def test_missing_price_or_stage_db_fails_closed(monkeypatch):
    pos = setup_contract(monkeypatch)
    out = router.evaluate_active_kr_tp(
        position=pos, engine=object(), kis=FakeKis(quote={}),
        env="practice", code="005930",
    )
    assert out["action"] == "BLOCK"
    monkeypatch.setattr(router, "load_frozen_stage_states", lambda **kw: (_ for _ in ()).throw(TimeoutError()))
    out = router.evaluate_active_kr_tp(
        position=pos, engine=object(), kis=FakeKis(), env="practice", code="005930",
    )
    assert out["action"] == "BLOCK" and "EVIDENCE_UNAVAILABLE" in out["reason"]


def test_legacy_and_shadow_do_not_create_active_order(monkeypatch):
    pos = setup_contract(monkeypatch)
    pos["position_meta"]["kr_us_profit_contract"]["activation_mode"] = "SHADOW"
    assert router.evaluate_active_kr_tp(
        position=pos, engine=object(), kis=FakeKis(), env="practice", code="005930",
    ) is None
    pos["position_meta"] = {}
    assert router.evaluate_active_kr_tp(
        position=pos, engine=object(), kis=FakeKis(), env="practice", code="005930",
    ) is None
