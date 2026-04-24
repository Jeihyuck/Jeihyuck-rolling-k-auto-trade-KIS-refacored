"""
테스트 2/3/4: AM/PM/CLOSE phase guard 정책 검증
- schedule 실행 시 허용 시간 창 검증
- 수동 실행 토큰 검증
- diag_replay 시 FORCE_BLOCK_LIVE 강제 적용 검증
"""
from __future__ import annotations

import pytest


def simulate_am_phase_guard(now_hhmm: int, event_name: str, confirm_manual_am: str = "") -> dict:
    """trade-am.yml의 phase guard 로직을 파이썬으로 재현한다."""
    should_run = 1
    skip_reason = ""

    if event_name == "schedule":
        if now_hhmm < 900:
            should_run = 0
            skip_reason = "early_start_am"
        elif now_hhmm > 910:
            should_run = 0
            skip_reason = "skip_stale_start_am"

    if event_name == "workflow_dispatch":
        if confirm_manual_am != "RUN_AM_MANUAL":
            should_run = 0
            skip_reason = "manual_am_not_confirmed"
        elif now_hhmm > 1030:
            should_run = 0
            skip_reason = "manual_am_after_1030_blocked"

    return {"should_run": should_run, "skip_reason": skip_reason}


def simulate_pm_phase_guard(now_hhmm: int, event_name: str, confirm_manual_pm: str = "") -> dict:
    """trade-pm.yml의 phase guard 로직을 파이썬으로 재현한다."""
    should_run = 1
    skip_reason = ""

    if event_name == "schedule":
        if now_hhmm < 1300:
            should_run = 0
            skip_reason = "early_start_pm"
        elif now_hhmm > 1310:
            should_run = 0
            skip_reason = "skip_stale_start_pm"

    if event_name == "workflow_dispatch":
        if confirm_manual_pm != "RUN_PM_MANUAL":
            should_run = 0
            skip_reason = "manual_pm_not_confirmed"
        elif now_hhmm > 1340:
            should_run = 0
            skip_reason = "manual_pm_after_1340_blocked"

    return {"should_run": should_run, "skip_reason": skip_reason}


def simulate_close_phase_guard(
    now_hhmm: int,
    event_name: str,
    manual_mode: str = "live_close",
    confirm_manual_close: str = "",
) -> dict:
    """trade-close.yml의 phase guard 로직을 파이썬으로 재현한다."""
    should_run = 1
    skip_reason = ""
    execution_route = "live_close"

    # diag_replay / compute_only env 강제값
    force_env: dict[str, str] = {}

    if event_name == "schedule":
        if now_hhmm < 1515:
            should_run = 0
            skip_reason = "close_early_start"
        elif now_hhmm > 1520:
            should_run = 0
            skip_reason = "skip_close_stale_start"

    if event_name == "workflow_dispatch":
        if manual_mode == "live_close":
            if confirm_manual_close != "RUN_CLOSE_MANUAL":
                should_run = 0
                skip_reason = "manual_close_not_confirmed"
            elif now_hhmm > 1530:
                should_run = 0
                skip_reason = "manual_close_after_1530_blocked"
        else:
            execution_route = "manual_replay"
            force_env = {
                "FORCE_BLOCK_LIVE": "1",
                "DISABLE_LIVE_TRADING": "1",
                "LIVE_TRADING_ENABLED": "0",
                "DRY_RUN": "1",
                "STRATEGY_MODE": "DIAG",
                "FORCE_STRATEGY_MODE": "DIAG",
                "PB1_ENTRY_ENABLED": "0",
            }

    return {
        "should_run": should_run,
        "skip_reason": skip_reason,
        "execution_route": execution_route,
        "force_env": force_env,
    }


class TestAmPhaseGuard:
    def test_schedule_early_start(self):
        r = simulate_am_phase_guard(859, "schedule")
        assert r["should_run"] == 0
        assert r["skip_reason"] == "early_start_am"

    def test_schedule_exact_start(self):
        r = simulate_am_phase_guard(900, "schedule")
        assert r["should_run"] == 1
        assert r["skip_reason"] == ""

    def test_schedule_within_window(self):
        r = simulate_am_phase_guard(910, "schedule")
        assert r["should_run"] == 1
        assert r["skip_reason"] == ""

    def test_schedule_stale_start(self):
        r = simulate_am_phase_guard(911, "schedule")
        assert r["should_run"] == 0
        assert r["skip_reason"] == "skip_stale_start_am"

    def test_dispatch_no_token(self):
        r = simulate_am_phase_guard(900, "workflow_dispatch", confirm_manual_am="")
        assert r["should_run"] == 0
        assert r["skip_reason"] == "manual_am_not_confirmed"

    def test_dispatch_wrong_token(self):
        r = simulate_am_phase_guard(900, "workflow_dispatch", confirm_manual_am="WRONG")
        assert r["should_run"] == 0
        assert r["skip_reason"] == "manual_am_not_confirmed"

    def test_dispatch_after_1030(self):
        r = simulate_am_phase_guard(1031, "workflow_dispatch", confirm_manual_am="RUN_AM_MANUAL")
        assert r["should_run"] == 0
        assert r["skip_reason"] == "manual_am_after_1030_blocked"

    def test_dispatch_valid(self):
        r = simulate_am_phase_guard(930, "workflow_dispatch", confirm_manual_am="RUN_AM_MANUAL")
        assert r["should_run"] == 1
        assert r["skip_reason"] == ""

    def test_dispatch_at_1030_boundary(self):
        # 정확히 1030은 허용 (> 1030이 차단)
        r = simulate_am_phase_guard(1030, "workflow_dispatch", confirm_manual_am="RUN_AM_MANUAL")
        assert r["should_run"] == 1


class TestPmPhaseGuard:
    def test_schedule_early_start(self):
        r = simulate_pm_phase_guard(1259, "schedule")
        assert r["should_run"] == 0
        assert r["skip_reason"] == "early_start_pm"

    def test_schedule_exact_start(self):
        r = simulate_pm_phase_guard(1300, "schedule")
        assert r["should_run"] == 1
        assert r["skip_reason"] == ""

    def test_schedule_within_window(self):
        r = simulate_pm_phase_guard(1310, "schedule")
        assert r["should_run"] == 1
        assert r["skip_reason"] == ""

    def test_schedule_stale_start(self):
        r = simulate_pm_phase_guard(1311, "schedule")
        assert r["should_run"] == 0
        assert r["skip_reason"] == "skip_stale_start_pm"

    def test_dispatch_no_token(self):
        r = simulate_pm_phase_guard(1300, "workflow_dispatch", confirm_manual_pm="")
        assert r["should_run"] == 0
        assert r["skip_reason"] == "manual_pm_not_confirmed"

    def test_dispatch_wrong_token(self):
        r = simulate_pm_phase_guard(1300, "workflow_dispatch", confirm_manual_pm="WRONG")
        assert r["should_run"] == 0
        assert r["skip_reason"] == "manual_pm_not_confirmed"

    def test_dispatch_after_1340(self):
        r = simulate_pm_phase_guard(1341, "workflow_dispatch", confirm_manual_pm="RUN_PM_MANUAL")
        assert r["should_run"] == 0
        assert r["skip_reason"] == "manual_pm_after_1340_blocked"

    def test_dispatch_valid(self):
        r = simulate_pm_phase_guard(1305, "workflow_dispatch", confirm_manual_pm="RUN_PM_MANUAL")
        assert r["should_run"] == 1
        assert r["skip_reason"] == ""

    def test_dispatch_at_1340_boundary(self):
        r = simulate_pm_phase_guard(1340, "workflow_dispatch", confirm_manual_pm="RUN_PM_MANUAL")
        assert r["should_run"] == 1


class TestClosePhaseGuard:
    def test_schedule_early_start(self):
        r = simulate_close_phase_guard(1514, "schedule")
        assert r["should_run"] == 0
        assert r["skip_reason"] == "close_early_start"

    def test_schedule_exact_start(self):
        r = simulate_close_phase_guard(1515, "schedule")
        assert r["should_run"] == 1
        assert r["skip_reason"] == ""

    def test_schedule_within_window(self):
        r = simulate_close_phase_guard(1520, "schedule")
        assert r["should_run"] == 1
        assert r["skip_reason"] == ""

    def test_schedule_stale_start(self):
        r = simulate_close_phase_guard(1521, "schedule")
        assert r["should_run"] == 0
        assert r["skip_reason"] == "skip_close_stale_start"

    def test_dispatch_live_no_token(self):
        r = simulate_close_phase_guard(1515, "workflow_dispatch", manual_mode="live_close", confirm_manual_close="")
        assert r["should_run"] == 0
        assert r["skip_reason"] == "manual_close_not_confirmed"

    def test_dispatch_live_after_1530(self):
        r = simulate_close_phase_guard(1531, "workflow_dispatch", manual_mode="live_close", confirm_manual_close="RUN_CLOSE_MANUAL")
        assert r["should_run"] == 0
        assert r["skip_reason"] == "manual_close_after_1530_blocked"

    def test_dispatch_live_valid(self):
        r = simulate_close_phase_guard(1515, "workflow_dispatch", manual_mode="live_close", confirm_manual_close="RUN_CLOSE_MANUAL")
        assert r["should_run"] == 1

    def test_dispatch_diag_replay_blocks_live(self):
        r = simulate_close_phase_guard(1600, "workflow_dispatch", manual_mode="diag_replay")
        assert r["should_run"] == 1
        assert r["execution_route"] == "manual_replay"
        fe = r["force_env"]
        assert fe["FORCE_BLOCK_LIVE"] == "1"
        assert fe["DISABLE_LIVE_TRADING"] == "1"
        assert fe["LIVE_TRADING_ENABLED"] == "0"
        assert fe["DRY_RUN"] == "1"
        assert fe["STRATEGY_MODE"] == "DIAG"
        assert fe["FORCE_STRATEGY_MODE"] == "DIAG"
        assert fe["PB1_ENTRY_ENABLED"] == "0"

    def test_dispatch_compute_only_blocks_live(self):
        r = simulate_close_phase_guard(1600, "workflow_dispatch", manual_mode="compute_only")
        assert r["execution_route"] == "manual_replay"
        assert r["force_env"]["FORCE_BLOCK_LIVE"] == "1"
