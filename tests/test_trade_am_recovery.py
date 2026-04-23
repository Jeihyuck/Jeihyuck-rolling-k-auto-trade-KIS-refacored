from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import trader.pb1_runner as pb1_runner
from trader.pb1_engine import PB1Engine, resolve_pb1_phase


def _set_am_env(monkeypatch, *, event_name: str, force_late_start: str = "0", force_after_1030: str = "0") -> None:
    env_values = {
        "GITHUB_EVENT_NAME": event_name,
        "MODE": "trade",
        "PB1_ENABLE_LATE_AM_RECOVERY": "1",
        "PB1_FORCE_TRADE_AM_LATE_START": force_late_start,
        "PB1_FORCE_TRADE_AM_AFTER_1030": force_after_1030,
        "PB1_AM_EXPECTED_START": "09:07",
        "PB1_AM_EXPECTED_END": "09:15",
        "PB1_AM_START_ALLOW_UNTIL": "09:30",
        "PB1_AM_RECOVERY_ALLOW_UNTIL": "10:30",
    }
    for key, value in env_values.items():
        monkeypatch.setenv(key, value)


def _patch_orders_repo(monkeypatch, *, today_orders=None, open_orders=None, am_marker: bool = False) -> None:
    today_orders = list(today_orders or [])
    open_orders = list(open_orders or [])

    class DummyOrdersRepo:
        def __init__(self, _engine):
            pass

        def list_today_buy_orders(self, *_args, **_kwargs):
            return list(today_orders)

        def get_open_orders(self, *_args, **_kwargs):
            return list(open_orders)

        def has_today_am_session_marker(self, *_args, **_kwargs):
            return am_marker

    monkeypatch.setattr(pb1_runner, "OrdersRepo", DummyOrdersRepo)


def test_trade_am_policy_schedule_1007_recovery() -> None:
    now = datetime(2024, 1, 2, 10, 7, tzinfo=ZoneInfo("Asia/Seoul"))

    policy = pb1_runner._detect_trade_am_start_policy(
        now=now,
        event_name="schedule",
        run_mode="trade",
        trading_day=True,
        force_trade_am_late_start=False,
        force_trade_am_after_1030=False,
        am_expected_start="09:07",
        am_expected_end="09:15",
        am_start_allow_until="09:30",
        am_recovery_allow_until="10:30",
    )

    assert policy["should_run"] is True
    assert policy["recovery"] is True
    assert policy["classification"] == "LATE_AM_RECOVERY"


def test_trade_am_policy_manual_1119_without_force() -> None:
    now = datetime(2024, 1, 2, 11, 19, tzinfo=ZoneInfo("Asia/Seoul"))

    policy = pb1_runner._detect_trade_am_start_policy(
        now=now,
        event_name="workflow_dispatch",
        run_mode="trade",
        trading_day=True,
        force_trade_am_late_start=False,
        force_trade_am_after_1030=False,
        am_expected_start="09:07",
        am_expected_end="09:15",
        am_start_allow_until="09:30",
        am_recovery_allow_until="10:30",
    )

    assert policy["should_run"] is False
    assert policy["skip_reason"] == "late_manual_start_after_1030_requires_force"


def test_trade_am_manual_1119_with_force_allows_recovery(monkeypatch) -> None:
    now = datetime(2024, 1, 2, 11, 19, tzinfo=ZoneInfo("Asia/Seoul"))
    _set_am_env(monkeypatch, event_name="workflow_dispatch", force_after_1030="1")
    _patch_orders_repo(monkeypatch, today_orders=[], open_orders=[], am_marker=False)

    result = pb1_runner._evaluate_session_recovery_guard(
        engine=SimpleNamespace(),
        env="practice",
        session_kind="am",
        now=now,
    )

    assert result["skip"] is False
    assert result["recovery_used"] is True
    assert result["force_override_used"] is True
    assert result["policy"]["classification"] == "FORCED_LATE_AM_MANUAL"


def test_trade_am_manual_1119_with_force_skips_duplicate_buy(monkeypatch) -> None:
    now = datetime(2024, 1, 2, 11, 19, tzinfo=ZoneInfo("Asia/Seoul"))
    _set_am_env(monkeypatch, event_name="workflow_dispatch", force_after_1030="1")
    _patch_orders_repo(monkeypatch, today_orders=[{"code": "005930", "status": "SUBMITTED"}], open_orders=[], am_marker=False)

    result = pb1_runner._evaluate_session_recovery_guard(
        engine=SimpleNamespace(),
        env="practice",
        session_kind="am",
        now=now,
    )

    assert result["skip"] is True
    assert result["exit_reason"] == "skip_duplicate_am_run"


def test_forced_trade_am_override_keeps_live_gate_separate() -> None:
    now = datetime(2024, 1, 2, 11, 19, tzinfo=ZoneInfo("Asia/Seoul"))

    phase, reason, _window = resolve_pb1_phase(
        now,
        True,
        None,
        force_entry_window_override=True,
        forced_trade_session="am",
    )

    engine = PB1Engine.__new__(PB1Engine)
    engine.trading_day = True
    engine.order_allowed = True
    engine.market_window_name = "after"
    engine.force_entry_window_override = True
    engine.am_recovery_continue = True
    engine.force_block_live = True
    engine.intended_live = False
    engine.strategy_mode = "AUTO"

    reasons = PB1Engine._order_precheck_gate_reasons(engine, side="BUY", stage="entry")

    assert phase == "entry"
    assert reason == "force_entry_window_override"
    assert "window_blocked" not in reasons
    assert "live_gate_blocked" in reasons