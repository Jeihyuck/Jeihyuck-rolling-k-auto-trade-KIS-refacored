"""
tests/test_workflow_prewarm_cron_contract.py

Prewarm cron 계약 검증:
- AM: 08:15 KST = 23:15 UTC 전날  →  "15 23 * * *"
- PM: deprecated (schedule 제거, manual fallback only)
- CLOSE: backup (schedule 유지), 15:00 KST = 06:00 UTC
- AFTERNOON: 12:15 KST = 03:15 UTC  →  "15 3 * * *"
- 각 workflow에 PB1_TARGET_START_TIME / Wait until target start step 존재
- timeout-minutes: AM=295, AFTERNOON=250, CLOSE=50
"""
from __future__ import annotations

import re
from pathlib import Path

WORKFLOWS_DIR = Path(__file__).parent.parent / ".github" / "workflows"


def _read(name: str) -> str:
    return (WORKFLOWS_DIR / name).read_text()


# ── trade-afternoon.yml existence & structure ─────────────────────────────────

def test_afternoon_workflow_exists():
    assert (WORKFLOWS_DIR / "trade-afternoon.yml").exists(), (
        "trade-afternoon.yml must exist"
    )


def test_afternoon_cron_is_early_prewarm():
    content = _read("trade-afternoon.yml")
    assert re.search(r'cron:\s*"15 3 \* \* \*"', content), (
        "trade-afternoon.yml cron should be '15 3 * * *' (12:15 KST prewarm)"
    )


def test_afternoon_timeout_250():
    content = _read("trade-afternoon.yml")
    assert re.search(r"timeout-minutes:\s*250", content), (
        "trade-afternoon.yml timeout-minutes should be 250"
    )


def test_afternoon_has_pm_entry_phase():
    content = _read("trade-afternoon.yml")
    assert 'PB1_SESSION_KIND: "pm"' in content, (
        "trade-afternoon.yml must have PB1_SESSION_KIND: \"pm\""
    )
    assert 'FORCE_PB1_PHASE: "entry"' in content, (
        "trade-afternoon.yml PM step must have FORCE_PB1_PHASE: \"entry\""
    )
    assert 'PB1_ENTRY_ENABLED: "1"' in content, (
        "trade-afternoon.yml PM step must have PB1_ENTRY_ENABLED: \"1\""
    )


def test_afternoon_has_close_exit_phase():
    content = _read("trade-afternoon.yml")
    assert 'PB1_SESSION_KIND: "close"' in content, (
        "trade-afternoon.yml must have PB1_SESSION_KIND: \"close\""
    )
    assert 'FORCE_PB1_PHASE: "exit"' in content, (
        "trade-afternoon.yml Close step must have FORCE_PB1_PHASE: \"exit\""
    )
    assert 'PB1_ENTRY_ENABLED: "0"' in content, (
        "trade-afternoon.yml Close step must have PB1_ENTRY_ENABLED: \"0\""
    )


def test_afternoon_close_entry_enabled_is_zero():
    """Close 스텝에서 entry_enabled=1이 로그에 출력되면 안 된다."""
    content = _read("trade-afternoon.yml")
    assert 'entry_enabled=0' in content, (
        "trade-afternoon.yml close phase log must emit entry_enabled=0"
    )
    # PM은 entry_enabled=1이고 close는 entry_enabled=0이어야 하므로 0이 존재해야 함
    assert 'entry_enabled=1' in content, (
        "trade-afternoon.yml pm phase log must emit entry_enabled=1"
    )


def test_afternoon_pm_continues_on_error():
    content = _read("trade-afternoon.yml")
    assert "continue-on-error: true" in content, (
        "trade-afternoon.yml pm/close steps must have continue-on-error: true"
    )


# ── AM ────────────────────────────────────────────────────────────────────────

def test_am_cron_is_early_prewarm():
    content = _read("trade-am.yml")
    assert re.search(r'cron:\s*"15 23 \* \* \*"', content), (
        "trade-am.yml cron should be '15 23 * * *' (08:15 KST prewarm)"
    )


def test_am_target_start_time():
    content = _read("trade-am.yml")
    assert 'PB1_TARGET_START_TIME: "09:00"' in content, (
        "trade-am.yml must set PB1_TARGET_START_TIME: \"09:00\""
    )


def test_am_start_allow_until_extended():
    content = _read("trade-am.yml")
    assert 'PB1_START_ALLOW_UNTIL: "09:25"' in content, (
        "trade-am.yml PB1_START_ALLOW_UNTIL should be \"09:25\""
    )


def test_am_delay_log_in_phase_guard():
    content = _read("trade-am.yml")
    assert "delay_seconds" in content, (
        "trade-am.yml phase guard must compute delay_seconds"
    )
    assert "schedule_expected=0815" in content, (
        "trade-am.yml must log schedule_expected=0815"
    )


def test_am_late_start_log():
    content = _read("trade-am.yml")
    assert "[TRADE_AM][LATE_START]" in content, (
        "trade-am.yml must emit [TRADE_AM][LATE_START] log"
    )


# ── PM (deprecated) ───────────────────────────────────────────────────────────

def test_pm_schedule_removed_or_deprecated():
    content = _read("trade-pm.yml")
    assert "Deprecated as scheduled workflow" in content, (
        "trade-pm.yml must contain deprecation comment"
    )
    assert 'cron:' not in content, (
        "trade-pm.yml must not have cron schedule (deprecated)"
    )


# ── Close (backup) ────────────────────────────────────────────────────────────

def test_close_target_start_time():
    content = _read("trade-close.yml")
    assert 'PB1_TARGET_START_TIME: "15:15"' in content, (
        "trade-close.yml must set PB1_TARGET_START_TIME: \"15:15\""
    )


def test_close_entry_disabled():
    content = _read("trade-close.yml")
    assert 'PB1_ENTRY_ENABLED: "0"' in content, (
        "trade-close.yml must set PB1_ENTRY_ENABLED: \"0\""
    )


def test_close_exit_phase_enforced():
    content = _read("trade-close.yml")
    assert 'FORCE_PB1_PHASE: "exit"' in content, (
        "trade-close.yml must set FORCE_PB1_PHASE: \"exit\""
    )


def test_close_has_already_done_check():
    content = _read("trade-close.yml")
    assert "already_done" in content, (
        "trade-close.yml must check if afternoon close already completed"
    )


def test_close_buy_order_forbidden_check():
    content = _read("trade-close.yml")
    assert "BUY order detected" in content or "ORDER.*BUY" in content, (
        "trade-close.yml must verify no BUY orders in close session"
    )


# ── PB1_PREWARM_ENABLED ───────────────────────────────────────────────────────

def test_am_has_prewarm_enabled():
    content = _read("trade-am.yml")
    assert 'PB1_PREWARM_ENABLED: "1"' in content, (
        "trade-am.yml must set PB1_PREWARM_ENABLED: \"1\""
    )


def test_afternoon_has_prewarm_enabled():
    content = _read("trade-afternoon.yml")
    assert 'PB1_PREWARM_ENABLED: "1"' in content, (
        "trade-afternoon.yml must set PB1_PREWARM_ENABLED: \"1\""
    )


# ── Wait until target start step ────────────────────────────────────────────

def test_am_has_wait_until_target_start_step():
    content = _read("trade-am.yml")
    assert "Wait until target start" in content, (
        "trade-am.yml must contain 'Wait until target start' step"
    )


def test_afternoon_has_wait_until_pm_step():
    content = _read("trade-afternoon.yml")
    assert "Wait until PM target start" in content, (
        "trade-afternoon.yml must contain 'Wait until PM target start' step"
    )


def test_afternoon_has_wait_until_close_step():
    content = _read("trade-afternoon.yml")
    assert "Wait until Close target start" in content, (
        "trade-afternoon.yml must contain 'Wait until Close target start' step"
    )


# ── timeout-minutes ───────────────────────────────────────────────────────────

def test_am_timeout_295():
    content = _read("trade-am.yml")
    assert re.search(r"timeout-minutes:\s*295", content), (
        "trade-am.yml timeout-minutes should be 295"
    )


def test_afternoon_timeout_250_value():
    content = _read("trade-afternoon.yml")
    assert re.search(r"timeout-minutes:\s*250", content), (
        "trade-afternoon.yml timeout-minutes should be 250"
    )


def test_close_timeout_50():
    content = _read("trade-close.yml")
    assert re.search(r"timeout-minutes:\s*50", content), (
        "trade-close.yml timeout-minutes should be 50"
    )


# ── session loop config ───────────────────────────────────────────────────────

def test_am_run_loop_minutes():
    content = _read("trade-am.yml")
    assert 'PB1_RUN_LOOP_MINUTES: "235"' in content


def test_afternoon_pm_run_loop_minutes():
    content = _read("trade-afternoon.yml")
    assert 'PB1_RUN_LOOP_MINUTES: "130"' in content


def test_afternoon_close_run_loop_minutes():
    content = _read("trade-afternoon.yml")
    assert 'PB1_RUN_LOOP_MINUTES: "15"' in content


def test_close_run_loop_minutes():
    content = _read("trade-close.yml")
    assert 'PB1_RUN_LOOP_MINUTES: "15"' in content

