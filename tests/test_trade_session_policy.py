from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import trader.pb1_runner as pb1_runner
import trader.window_router as window_router
from trader.pb1_engine import PB1Engine, resolve_pb1_phase


def _set_session_env(monkeypatch, *, session: str, event_name: str, force_late: str = "0", force_hard: str = "0") -> None:
    session_key = session.upper()
    defaults = {
        "am": {
            "enable": "PB1_ENABLE_LATE_AM_RECOVERY",
            "expected_from": "PB1_AM_EXPECTED_START",
            "expected_to": "PB1_AM_EXPECTED_END",
            "start_allow": "PB1_AM_START_ALLOW_UNTIL",
            "recovery_allow": "PB1_AM_RECOVERY_ALLOW_UNTIL",
            "force_late": "PB1_FORCE_TRADE_AM_LATE_START",
            "force_hard": "PB1_FORCE_TRADE_AM_AFTER_1030",
            "expected_from_value": "09:07",
            "expected_to_value": "09:15",
            "start_allow_value": "09:30",
            "recovery_allow_value": "10:30",
        },
        "pm": {
            "enable": "PB1_ENABLE_LATE_PM_RECOVERY",
            "expected_from": "PB1_PM_EXPECTED_START",
            "expected_to": "PB1_PM_EXPECTED_END",
            "start_allow": "PB1_PM_START_ALLOW_UNTIL",
            "recovery_allow": "PB1_PM_RECOVERY_ALLOW_UNTIL",
            "force_late": "PB1_FORCE_TRADE_PM_LATE_START",
            "force_hard": "PB1_FORCE_TRADE_PM_AFTER_1340",
            "expected_from_value": "13:05",
            "expected_to_value": "13:15",
            "start_allow_value": "13:25",
            "recovery_allow_value": "13:40",
        },
    }
    cfg = defaults[session]
    monkeypatch.setenv("GITHUB_EVENT_NAME", event_name)
    monkeypatch.setenv("MODE", "trade")
    monkeypatch.setenv("PB1_FORCE_TRADE_SESSION", session)
    monkeypatch.setenv(cfg["enable"], "1")
    monkeypatch.setenv(cfg["expected_from"], cfg["expected_from_value"])
    monkeypatch.setenv(cfg["expected_to"], cfg["expected_to_value"])
    monkeypatch.setenv(cfg["start_allow"], cfg["start_allow_value"])
    monkeypatch.setenv(cfg["recovery_allow"], cfg["recovery_allow_value"])
    monkeypatch.setenv(cfg["force_late"], force_late)
    monkeypatch.setenv(cfg["force_hard"], force_hard)
    if session == "am":
        monkeypatch.setenv("PB1_FORCE_TRADE_PM_LATE_START", "0")
        monkeypatch.setenv("PB1_FORCE_TRADE_PM_AFTER_1340", "0")
    else:
        monkeypatch.setenv("PB1_FORCE_TRADE_AM_LATE_START", "0")
        monkeypatch.setenv("PB1_FORCE_TRADE_AM_AFTER_1030", "0")


def _patch_orders_repo(monkeypatch, *, session_orders=None, open_orders=None, session_marker: bool = False) -> None:
    session_orders = list(session_orders or [])
    open_orders = list(open_orders or [])

    class DummyOrdersRepo:
        def __init__(self, _engine):
            pass

        def list_today_session_buy_orders(self, *_args, **_kwargs):
            return list(session_orders)

        def get_open_orders(self, *_args, **_kwargs):
            return list(open_orders)

        def has_today_session_marker(self, *_args, **_kwargs):
            return session_marker

    monkeypatch.setattr(pb1_runner, "OrdersRepo", DummyOrdersRepo)


def test_am_schedule_normal() -> None:
    now = datetime(2024, 1, 2, 9, 12, tzinfo=ZoneInfo("Asia/Seoul"))
    policy = pb1_runner._detect_trade_session_policy(now=now, event_name="schedule", run_mode="trade", trading_day=True, session="am")
    assert policy["should_run"] is True
    assert policy["recovery"] is False
    assert policy["classification"] == "NORMAL_AM_SCHEDULE"


def test_am_schedule_late_recovery(monkeypatch) -> None:
    _set_session_env(monkeypatch, session="am", event_name="schedule")
    now = datetime(2024, 1, 2, 10, 7, tzinfo=ZoneInfo("Asia/Seoul"))
    policy = pb1_runner._detect_trade_session_policy(now=now, event_name="schedule", run_mode="trade", trading_day=True, session="am")
    assert policy["should_run"] is True
    assert policy["recovery"] is True
    assert policy["classification"] == "LATE_AM_RECOVERY"


def test_am_manual_hard_cutoff_no_force(monkeypatch) -> None:
    _set_session_env(monkeypatch, session="am", event_name="workflow_dispatch")
    now = datetime(2024, 1, 2, 11, 19, tzinfo=ZoneInfo("Asia/Seoul"))
    policy = pb1_runner._detect_trade_session_policy(now=now, event_name="workflow_dispatch", run_mode="trade", trading_day=True, session="am")
    assert policy["should_run"] is False
    assert policy["skip_reason"] == "late_manual_start_after_1030_requires_force"
    assert policy["classification"] == "SKIP_AM_MANUAL_HARD_CUTOFF"


def test_am_manual_hard_cutoff_with_force(monkeypatch) -> None:
    _set_session_env(monkeypatch, session="am", event_name="workflow_dispatch", force_hard="1")
    now = datetime(2024, 1, 2, 11, 19, tzinfo=ZoneInfo("Asia/Seoul"))
    policy = pb1_runner._detect_trade_session_policy(now=now, event_name="workflow_dispatch", run_mode="trade", trading_day=True, session="am")
    assert policy["should_run"] is True
    assert policy["force_override_used"] is True
    assert policy["classification"] == "FORCED_LATE_AM_MANUAL"


def test_pm_schedule_normal() -> None:
    now = datetime(2024, 1, 2, 13, 10, tzinfo=ZoneInfo("Asia/Seoul"))
    policy = pb1_runner._detect_trade_session_policy(now=now, event_name="schedule", run_mode="trade", trading_day=True, session="pm")
    assert policy["should_run"] is True
    assert policy["recovery"] is False
    assert policy["classification"] == "NORMAL_PM_SCHEDULE"


def test_pm_schedule_late_recovery(monkeypatch) -> None:
    _set_session_env(monkeypatch, session="pm", event_name="schedule")
    now = datetime(2024, 1, 2, 13, 31, tzinfo=ZoneInfo("Asia/Seoul"))
    policy = pb1_runner._detect_trade_session_policy(now=now, event_name="schedule", run_mode="trade", trading_day=True, session="pm")
    assert policy["should_run"] is True
    assert policy["recovery"] is True
    assert policy["classification"] == "LATE_PM_RECOVERY"


def test_pm_manual_hard_cutoff_no_force(monkeypatch) -> None:
    _set_session_env(monkeypatch, session="pm", event_name="workflow_dispatch")
    now = datetime(2024, 1, 2, 13, 52, tzinfo=ZoneInfo("Asia/Seoul"))
    policy = pb1_runner._detect_trade_session_policy(now=now, event_name="workflow_dispatch", run_mode="trade", trading_day=True, session="pm")
    assert policy["should_run"] is False
    assert policy["skip_reason"] == "late_manual_start_after_1340_requires_force"
    assert policy["classification"] == "SKIP_PM_MANUAL_HARD_CUTOFF"


def test_pm_manual_hard_cutoff_with_force(monkeypatch) -> None:
    _set_session_env(monkeypatch, session="pm", event_name="workflow_dispatch", force_hard="1")
    now = datetime(2024, 1, 2, 13, 52, tzinfo=ZoneInfo("Asia/Seoul"))
    policy = pb1_runner._detect_trade_session_policy(now=now, event_name="workflow_dispatch", run_mode="trade", trading_day=True, session="pm")
    assert policy["should_run"] is True
    assert policy["force_override_used"] is True
    assert policy["classification"] == "FORCED_LATE_PM_MANUAL"


def test_am_duplicate_skip(monkeypatch) -> None:
    _set_session_env(monkeypatch, session="am", event_name="workflow_dispatch", force_hard="1")
    _patch_orders_repo(monkeypatch, session_orders=[{"code": "005930", "status": "SUBMITTED"}], session_marker=True)
    now = datetime(2024, 1, 2, 11, 19, tzinfo=ZoneInfo("Asia/Seoul"))
    result = pb1_runner._evaluate_session_recovery_guard(engine=SimpleNamespace(), env="practice", session_kind="am", now=now)
    assert result["skip"] is True
    assert pb1_runner._normalize_phase_guard_exit_reason("am", result["exit_reason"]) == "phase_guard_skip_duplicate_am_run"


def test_pm_duplicate_skip(monkeypatch) -> None:
    _set_session_env(monkeypatch, session="pm", event_name="workflow_dispatch", force_hard="1")
    _patch_orders_repo(monkeypatch, session_orders=[{"code": "000660", "status": "SUBMITTED"}], session_marker=True)
    now = datetime(2024, 1, 2, 13, 52, tzinfo=ZoneInfo("Asia/Seoul"))
    result = pb1_runner._evaluate_session_recovery_guard(engine=SimpleNamespace(), env="practice", session_kind="pm", now=now)
    assert result["skip"] is True
    assert pb1_runner._normalize_phase_guard_exit_reason("pm", result["exit_reason"]) == "phase_guard_skip_duplicate_pm_run"


def test_force_override_does_not_auto_enable_live_orders() -> None:
    engine = PB1Engine.__new__(PB1Engine)
    engine.trading_day = True
    engine.order_allowed = True
    engine.market_window_name = "after"
    engine.force_entry_window_override = True
    engine.session_recovery_continue = True
    engine.force_block_live = True
    engine.intended_live = False
    engine.strategy_mode = "AUTO"
    reasons = PB1Engine._order_precheck_gate_reasons(engine, side="BUY", stage="entry")
    assert "window_blocked" not in reasons
    assert "live_gate_blocked" in reasons


def test_downstream_no_second_block(monkeypatch) -> None:
    now = datetime(2024, 1, 2, 11, 19, tzinfo=ZoneInfo("Asia/Seoul"))
    monkeypatch.setenv("PB1_FORCE_ENTRY_WINDOW_OVERRIDE", "1")
    monkeypatch.setenv("PB1_FORCED_TRADE_SESSION", "am")
    phase, reason, window_name = resolve_pb1_phase(now, True, None, force_entry_window_override=True, forced_trade_session="am")
    decision = window_router.decide_window(now=now, override="after")
    assert phase == "entry"
    assert reason == "force_entry_window_override"
    assert window_name == "day"
    assert decision is not None
    assert decision.name == "day"


def test_pm_manage_only_preserved() -> None:
    policy = pb1_runner._resolve_session_trade_policy(session="pm", phase_name="manage", entry_enabled=False)
    assert policy["no_new_entry"] is True
    assert policy["reason"] == "pm_strategy_manage_only"

def test_close_recovery_window() -> None:
    now = datetime(2024, 1, 2, 15, 34, tzinfo=ZoneInfo("Asia/Seoul"))
    policy = pb1_runner._detect_close_start_policy(now=now, event_name="schedule")
    assert policy["should_run"] is True
    assert policy["recovery"] is True
    assert policy["classification"] == "RECOVERY_CLOSE_START"

def test_close_stale_start() -> None:
    now = datetime(2024, 1, 2, 16, 6, tzinfo=ZoneInfo("Asia/Seoul"))
    policy = pb1_runner._detect_close_start_policy(now=now, event_name="schedule")
    assert policy["should_run"] is False
    assert policy["skip_reason"] == "skip_close_stale_start"