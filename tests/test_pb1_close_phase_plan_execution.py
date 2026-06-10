from trader.trade_plan import build_entry_exit_plan, classify_close_action_from_plan


def _plan(style):
    return build_entry_exit_plan(code="005930", market="KOSPI", entry_style_selected=style, entry_reason=style, entry_price=10000, features={"stop_price": 9500}).to_dict()


def test_day_trade_force_exit_sell_intent():
    assert classify_close_action_from_plan(_plan("ENTRY_BREAKOUT"))[0] == "FORCE_SELL"


def test_swing_carry_no_sell_without_signal():
    assert classify_close_action_from_plan(_plan("ENTRY_PULLBACK")) == ("CARRY", "SWING_CARRY")


def test_swing_stop_signal_can_still_sell():
    action, reason = classify_close_action_from_plan(_plan("ENTRY_PULLBACK"))
    stop_hit = True
    assert action == "CARRY" and stop_hit


def test_policy_missing_skip():
    assert classify_close_action_from_plan({}) == ("SKIP", "POLICY_MISSING")

from datetime import date
from types import SimpleNamespace

import pandas as pd

from trader.pb1_engine import PB1Engine


class _DummyPositionsRepo:
    def __init__(self):
        self.updates = []

    def update_position_fields(self, **kwargs):
        self.updates.append(kwargs)


class _DummyOrdersRepo:
    def __init__(self):
        self.created = []

    def create_intent_idempotent(self, **kwargs):
        self.created.append(kwargs)
        return "order-1", True


class _DummyFillsRepo:
    def upsert_fill(self, **kwargs):
        return "fill-1"


def _engine_for_close_plan():
    engine = object.__new__(PB1Engine)
    engine.env = "practice"
    engine.STRATEGY_NAME = "pb1_pullback_close"
    engine.run_id = None
    engine._today = "2026-06-10"
    engine._trade_date = date(2026, 6, 10)
    engine._now_kst = pd.Timestamp("2026-06-10 15:20:00", tz="Asia/Seoul")
    engine.window_internal = "close"
    engine.dry_run = True
    engine.order_allowed = True
    engine._code_name_map = {}
    engine._exit_holdings_meta = {"source": "test"}
    engine.minervini_config = SimpleNamespace(heavy_volume_mult=2.0)
    engine.positions_repo = _DummyPositionsRepo()
    engine.orders_repo = _DummyOrdersRepo()
    engine.fills_repo = _DummyFillsRepo()
    engine.kis = None
    engine._regime = "NEUTRAL"
    engine.get_as_of = lambda: "2026-06-10"
    engine._resolve_price_with_fallback = lambda code, ohlcv_close=None: (float(ohlcv_close or 10000), "test")
    engine._resolve_force_exit_simulation = lambda **kwargs: None
    engine._resolve_exit_submit_gate_reasons = lambda code: []
    engine._should_block_order = lambda *args, **kwargs: (False, None)
    engine._append_ledger_event = lambda **kwargs: None
    return engine


def test_actual_pb1_close_phase_day_trade_creates_full_sell_intent():
    engine = _engine_for_close_plan()
    plan = _plan("ENTRY_BREAKOUT")
    pos = {
        "code": "005930",
        "sid": 1,
        "mode": 1,
        "qty": 3,
        "orderable_qty": 3,
        "avg_buy_price": 10000,
        "last_price": 10000,
        "market": "KOSPI",
        "entry_exit_plan_json": plan,
        "entry_thesis": plan["entry_thesis"],
        "trade_horizon": plan["trade_horizon"],
        "exit_policy_family": plan["exit_policy_family"],
        "eod_action": plan["eod_action"],
        "force_eod_close": plan["force_eod_close"],
    }
    result = engine._plan_exit_event(pos, {"close": 10000}, pd.DataFrame(), "close")
    assert result["exit_ok"] is True
    assert result["decision_reason"] == "EOD_FORCE_EXIT"
    assert result["close_action"] == "FORCE_SELL"
    assert engine.orders_repo.created
    assert engine.orders_repo.created[0]["qty"] == 3


def test_actual_pb1_close_phase_swing_no_signal_creates_no_sell_intent():
    engine = _engine_for_close_plan()
    plan = _plan("ENTRY_PULLBACK")
    pos = {
        "code": "005930",
        "sid": 1,
        "mode": 1,
        "qty": 3,
        "orderable_qty": 3,
        "avg_buy_price": 10000,
        "last_price": 10000,
        "market": "KOSPI",
        "entry_exit_plan_json": plan,
        "entry_thesis": plan["entry_thesis"],
        "trade_horizon": plan["trade_horizon"],
        "exit_policy_family": plan["exit_policy_family"],
        "eod_action": plan["eod_action"],
        "force_eod_close": plan["force_eod_close"],
    }
    result = engine._plan_exit_event(pos, {"close": 10000}, pd.DataFrame(), "close")
    assert result["exit_ok"] is False
    assert result["close_action"] == "CARRY"
    assert result["close_reason"] == "SWING_CARRY"
    assert engine.orders_repo.created == []
