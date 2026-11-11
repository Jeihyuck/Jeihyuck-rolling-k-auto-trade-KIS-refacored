"""Practice KR US TP router contract evidence, owner isolation and fail-closed tests."""
from dataclasses import dataclass, field
from datetime import datetime, timezone
import pytest

from trader.execution_state import BrokerPosition, BalanceFreshness


@dataclass
class FakeBalance:
    snapshot_id: str = "kis-snapshot"
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    freshness: BalanceFreshness = BalanceFreshness.FRESH
    broker_qty: int = 100
    broker_orderable: int = 100
    broker_avg: float = 100

    def position(self, code):
        return BrokerPosition(
            qty=self.broker_qty, orderable_qty=self.broker_orderable,
            avg_price=self.broker_avg,
        )

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
    monkeypatch.setattr(router, "has_unresolved_kr_sell_orders", lambda **kw: False)
    return pos


def test_tp1_at_9pct_is_sequential_only(monkeypatch):
    pos = setup_contract(monkeypatch)
    kis = FakeKis()
    result = router.evaluate_active_kr_tp(
        position=pos, engine=object(), kis=kis, env="practice", code="005930",
        balance_snapshot=FakeBalance(),
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
        balance_snapshot=FakeBalance(),
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
        balance_snapshot=FakeBalance(),
    )
    assert out["action"] == "BLOCK" and out["reason"] == reason


def test_missing_price_or_stage_db_fails_closed(monkeypatch):
    pos = setup_contract(monkeypatch)
    out = router.evaluate_active_kr_tp(
        position=pos, engine=object(), kis=FakeKis(quote={}),
        env="practice", code="005930", balance_snapshot=FakeBalance(),
    )
    assert out["action"] == "BLOCK"
    monkeypatch.setattr(router, "load_frozen_stage_states", lambda **kw: (_ for _ in ()).throw(TimeoutError()))
    out = router.evaluate_active_kr_tp(
        position=pos, engine=object(), kis=FakeKis(), env="practice", code="005930",
        balance_snapshot=FakeBalance(),
    )
    assert out["action"] == "BLOCK" and "EVIDENCE_UNAVAILABLE" in out["reason"]


def test_legacy_and_shadow_do_not_create_active_order(monkeypatch):
    pos = setup_contract(monkeypatch)
    pos["position_meta"]["kr_us_profit_contract"]["activation_mode"] = "SHADOW"
    assert router.evaluate_active_kr_tp(
        position=pos, engine=object(), kis=FakeKis(), env="practice", code="005930",
        balance_snapshot=FakeBalance(),
    ) is None
    pos["position_meta"] = {}
    assert router.evaluate_active_kr_tp(
        position=pos, engine=object(), kis=FakeKis(), env="practice", code="005930",
        balance_snapshot=FakeBalance(),
    ) is None


def test_cross_owner_unresolved_sell_blocks_new_tp(monkeypatch):
    pos = setup_contract(monkeypatch)
    monkeypatch.setattr(router, "has_unresolved_kr_sell_orders", lambda **kw: True)
    result = router.evaluate_active_kr_tp(
        position=pos, engine=object(), kis=FakeKis(),
        env="practice", code="005930", balance_snapshot=FakeBalance(),
    )
    assert result["action"] == "BLOCK"
    assert result["reason"] == "UNRESOLVED_SELL_OR_ACK"


def test_native_tp_fails_closed_when_any_broker_shares_are_reserved(monkeypatch):
    pos = setup_contract(monkeypatch)
    pos["orderable_qty"] = 92
    out = router.evaluate_active_kr_tp(
        position=pos, engine=object(), kis=FakeKis(),
        env="practice", code="005930",
        balance_snapshot=FakeBalance(broker_orderable=92),
    )
    assert out["action"] == "BLOCK"
    assert out["reason"] == "KR_TP_BROKER_RESERVED_INVENTORY_RECONCILE_REQUIRED"


@pytest.mark.parametrize("dry_run,intended_live,force_block_live,allowed", [
    (False, True, False, True),
    (True, True, False, False),
    (False, False, False, False),
    (False, True, True, False),
    (True, False, True, False),
])
def test_active_tp_durable_intent_gate(
    dry_run, intended_live, force_block_live, allowed,
):
    # Diagnostic/disabled TP evaluation must never persist a CREATED SELL.
    assert router.active_tp_durable_intent_permitted(
        dry_run=dry_run, intended_live=intended_live,
        force_block_live=force_block_live,
    ) is allowed


def test_active_tp_no_intent_preflight_precedes_native_sell_row():
    # Guard against regression from intent-before-preflight in a large PB1
    # engine method. Business logic is tested above; this asserts placement.
    from pathlib import Path
    src = (Path(__file__).resolve().parents[2] / "trader" / "pb1_engine.py").read_text()
    guard = src.index("if _kr_us_tp_context is not None:\n            # Active TP/V3 SELL")
    intent = src.index("self.orders_repo.create_intent_idempotent(", guard)
    assert src.index("active_tp_durable_intent_permitted(", guard) < intent
    assert src.index("if not self._pretrade_check(", guard) < intent


@pytest.mark.parametrize("status,response,broker,submitted,expected", [
    ("ERROR", {"rt_cd": "EXECUTION_ACTION_ALREADY_CLAIMED",
               "broker_submit": False}, None, None, False),
    ("ERROR", {"rt_cd": "KR_US_TP_STAGE_NOT_DURABLE",
               "broker_submit": False}, None, None, False),
    ("ERROR", {"rt_cd": "KR_US_TP_PRETRADE_DENIED",
               "broker_submit": False}, None, None, False),
    ("ERROR", {"rt_cd": "EXECUTION_CLAIM_UNAVAILABLE",
               "broker_submit": False}, None, None, False),
    ("ERROR", {"rt_cd": "EXECUTION_CLAIM_UNAVAILABLE",
               "broker_submit": False}, "0000033", None, True),
    ("ERROR", {"rt_cd": "EXECUTION_CLAIM_UNAVAILABLE",
               "broker_submit": False}, None, "submitted", True),
    ("ERROR", {"rt_cd": "TIMEOUT", "broker_submit": False}, None, None, True),
    ("ERROR", {}, None, None, True),
    ("CREATED", {}, None, None, True),
    ("ACKED", {}, "0000033", "submitted", True),
    ("PARTIAL_FILLED", {}, "0000033", "submitted", True),
    ("FILLED", {}, "0000033", "submitted", False),
])
def test_cross_owner_sell_fence_distinguishes_proven_pre_http_error(
    status, response, broker, submitted, expected,
):
    assert router._unresolved_sell_row({
        "status": status, "response_json": response,
        "kis_odno": broker, "broker_order_id": None,
        "submitted_at": submitted, "acked_at": None,
    }) is expected

def test_diagnostic_or_pretrade_denied_never_claims_durable_tp_intent():
    """Only an armed, non-diagnostic session may create a native TP SELL claim."""
    permitted = router.active_tp_durable_intent_permitted
    assert permitted(dry_run=False, intended_live=True, force_block_live=False) is True
    assert permitted(dry_run=True, intended_live=True, force_block_live=False) is False
    assert permitted(dry_run=False, intended_live=False, force_block_live=False) is False
    assert permitted(dry_run=False, intended_live=True, force_block_live=True) is False


def test_unresolved_sell_fence_ignores_only_broker_free_pre_http_errors():
    """No ACK, missing submit evidence, or ambiguous ERROR may bypass SELL dedupe."""
    row = {
        "status": "ERROR",
        "response_json": {
            "rt_cd": "KR_US_TP_STAGE_NOT_DURABLE",
            "broker_submit": False,
        },
        "kis_odno": None, "broker_order_id": None,
        "submitted_at": None, "acked_at": None,
    }
    assert router._unresolved_sell_row(row) is False
    assert router._unresolved_sell_row({**row, "response_json": {
        "rt_cd": "KR_US_TP_STAGE_NOT_DURABLE",
    }}) is True
    assert router._unresolved_sell_row({**row, "response_json": {
        "rt_cd": "HTTP_TIMEOUT", "broker_submit": False,
    }}) is True
    assert router._unresolved_sell_row({**row, "kis_odno": "00000099"}) is True
    assert router._unresolved_sell_row({**row, "submitted_at": "2026-11-11T15:10:00Z"}) is True
    assert router._unresolved_sell_row({**row, "status": "ACK"}) is True
    assert router._unresolved_sell_row({**row, "status": "PARTIAL_FILLED"}) is True
    assert router._unresolved_sell_row({**row, "status": "FILLED"}) is False
