"""
tests/test_phase_guard_after_close_override.py

목적:
workflow_dispatch + DIAG + FORCE_RUN=1 + PB1_DIAG_FULL_EXEC=1 +
FORCE_MARKET_WINDOW=day + FORCE_PB1_PHASE=entry + DRY_RUN=1 + FORCE_BLOCK_LIVE=1
조합에서 PM valid_window(15:10) 이후라도 should_run=True, force_override_used=True 확인.

금지 조건:
- skip_reason=outside_trade_pm_valid_window
- force_override_used=False when override active
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trader.pb1_runner import (
    _after_close_entry_dryrun_enabled,
    _detect_trade_session_policy,
)

KST_DATE = (2026, 4, 29)


def _make_kst(hhmm: str) -> datetime:
    h, m = int(hhmm[:2]), int(hhmm[2:])
    return datetime(*KST_DATE, h, m, 0)


def _set_diag_env(*, use_allow_flag: bool = False):
    os.environ.update({
        "MODE": "trade",
        "STRATEGY_MODE": "DIAG",
        "FORCE_RUN": "1",
        "PB1_DIAG_FULL_EXEC": "1",
        "FORCE_MARKET_WINDOW": "day",
        "FORCE_PB1_PHASE": "entry",
        "DRY_RUN": "1",
        "FORCE_BLOCK_LIVE": "1",
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "CLOSE_AUCTION_START": "15:15",
    })
    if use_allow_flag:
        os.environ["PB1_ALLOW_AFTER_CLOSE_ENTRY_DRYRUN"] = "1"
    else:
        os.environ.pop("PB1_ALLOW_AFTER_CLOSE_ENTRY_DRYRUN", None)


def _clear_env():
    for k in [
        "MODE", "STRATEGY_MODE", "FORCE_RUN", "PB1_DIAG_FULL_EXEC",
        "FORCE_MARKET_WINDOW", "FORCE_PB1_PHASE", "DRY_RUN", "FORCE_BLOCK_LIVE",
        "GITHUB_EVENT_NAME", "PB1_ALLOW_AFTER_CLOSE_ENTRY_DRYRUN",
        "LIVE_TRADING_ENABLED",
    ]:
        os.environ.pop(k, None)


# ---------------------------------------------------------------------------
# _after_close_entry_dryrun_enabled
# ---------------------------------------------------------------------------

class TestAfterCloseDryrunEnabled:
    def setup_method(self): _set_diag_env()
    def teardown_method(self): _clear_env()

    def test_force_run_and_diag_full_exec_activates(self):
        """FORCE_RUN=1 + PB1_DIAG_FULL_EXEC=1 → allow_flag 없어도 True"""
        assert _after_close_entry_dryrun_enabled(_make_kst("1935")) is True

    def test_allow_flag_alone_activates(self):
        """PB1_ALLOW_AFTER_CLOSE_ENTRY_DRYRUN=1 단독으로도 True"""
        _set_diag_env(use_allow_flag=True)
        os.environ["FORCE_RUN"] = "0"
        os.environ["PB1_DIAG_FULL_EXEC"] = "0"
        assert _after_close_entry_dryrun_enabled(_make_kst("1935")) is True

    def test_no_force_run_and_no_flag_disabled(self):
        os.environ["FORCE_RUN"] = "0"
        os.environ.pop("PB1_ALLOW_AFTER_CLOSE_ENTRY_DRYRUN", None)
        assert _after_close_entry_dryrun_enabled(_make_kst("1935")) is False

    def test_diag_full_exec_zero_disabled(self):
        os.environ["PB1_DIAG_FULL_EXEC"] = "0"
        os.environ.pop("PB1_ALLOW_AFTER_CLOSE_ENTRY_DRYRUN", None)
        assert _after_close_entry_dryrun_enabled(_make_kst("1935")) is False

    def test_before_close_start_disabled(self):
        """13:00은 close_start(15:15) 미만 → False"""
        assert _after_close_entry_dryrun_enabled(_make_kst("1300")) is False

    def test_force_block_live_zero_disabled(self):
        os.environ["FORCE_BLOCK_LIVE"] = "0"
        assert _after_close_entry_dryrun_enabled(_make_kst("1935")) is False

    def test_dry_run_zero_disabled(self):
        os.environ["DRY_RUN"] = "0"
        assert _after_close_entry_dryrun_enabled(_make_kst("1935")) is False

    def test_live_mode_disabled(self):
        os.environ["STRATEGY_MODE"] = "LIVE"
        assert _after_close_entry_dryrun_enabled(_make_kst("1935")) is False


# ---------------------------------------------------------------------------
# _detect_trade_session_policy PM 19:35
# ---------------------------------------------------------------------------

class TestPMPolicyAfterHours:
    def setup_method(self): _set_diag_env()
    def teardown_method(self): _clear_env()

    def _run(self, now_hhmm: str = "1935") -> dict:
        return _detect_trade_session_policy(
            now=_make_kst(now_hhmm),
            event_name="workflow_dispatch",
            run_mode="trade",
            trading_day=True,
            session="pm",
        )

    def test_should_run_true(self):
        p = self._run()
        assert p["should_run"] is True, f"should_run=False, skip_reason={p.get('skip_reason')}"

    def test_force_override_used_true(self):
        p = self._run()
        assert p["force_override_used"] is True

    def test_skip_reason_is_not_outside_window(self):
        p = self._run()
        assert p.get("skip_reason") != "outside_trade_pm_valid_window", \
            "금지된 skip_reason 검출"

    def test_reason_field_after_close_entry_dryrun(self):
        p = self._run()
        assert p.get("reason") == "after_close_entry_dryrun", \
            f"reason 필드 오류: {p.get('reason')}"

    def test_classification_contains_after_close(self):
        p = self._run()
        assert "AFTER_CLOSE_ENTRY_DRYRUN" in p["classification"], \
            f"classification 오류: {p['classification']}"

    def test_live_gate_env_enforced(self):
        """override 발동 시 LIVE_TRADING_ENABLED=0 강제"""
        os.environ.pop("LIVE_TRADING_ENABLED", None)
        self._run()
        assert os.environ.get("LIVE_TRADING_ENABLED") == "0"
        assert os.environ.get("FORCE_BLOCK_LIVE") == "1"

    def test_live_mode_after_hours_still_skips(self):
        """DIAG 조건 없이 19:35 → 기존대로 skip"""
        os.environ["STRATEGY_MODE"] = "LIVE"
        os.environ["DRY_RUN"] = "0"
        os.environ["FORCE_BLOCK_LIVE"] = "0"
        os.environ.pop("PB1_ALLOW_AFTER_CLOSE_ENTRY_DRYRUN", None)
        p = self._run()
        assert p["should_run"] is False
        assert "outside_trade_pm_valid_window" in (p.get("skip_reason") or "")

    def test_window_inside_unaffected_by_override(self):
        """13:30(window 내부)은 AFTER_CLOSE classification 아님"""
        p = self._run("1330")
        assert p["should_run"] is True
        assert "AFTER_CLOSE_ENTRY_DRYRUN" not in p["classification"]
