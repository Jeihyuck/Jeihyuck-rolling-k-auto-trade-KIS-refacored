"""
tests/test_workflow_phase_guard_minute_compare.py

Phase guard 분 단위 정수 비교 로직을 Python으로 시뮬레이션하여 검증.
bash의 HHMM 문자열 비교 대신 분 단위 정수 비교가 올바르게 동작하는지 확인.

검증 케이스:
- AM now=0845 schedule → should_run=1, wait_until_target=1
- AM now=0855 schedule → should_run=1, wait_until_target=1
- AM now=0900 schedule → should_run=1, wait_until_target=0
- AM now=0911 schedule → should_run=0, skip_stale_start_am
- PM now=1245 schedule → should_run=1, wait_until_target=1
- PM now=1311 schedule → skip_stale_start_pm
- CLOSE now=1500 schedule → should_run=1, wait_until_target=1
- CLOSE now=1521 schedule → skip_close_stale_start
"""
from __future__ import annotations

import pytest


def _phase_guard(session: str, now_hhmm: str, event_name: str = "schedule") -> dict:
    """bash phase_guard 로직을 Python으로 시뮬레이션.

    bash 코드:
        now_min=$((10#$now_h * 60 + 10#$now_m))
        if now_min > allow_until_min  → should_run=0 skip_stale
        elif now_min < target_start_min → wait_until_target=1
    """
    now_h = int(now_hhmm[:2])
    now_m = int(now_hhmm[2:])
    now_min = now_h * 60 + now_m

    if session == "am":
        target_start_min = 9 * 60         # 540
        allow_until_min = 9 * 60 + 10     # 550
        skip_reason_stale = "skip_stale_start_am"
    elif session == "pm":
        target_start_min = 13 * 60        # 780
        allow_until_min = 13 * 60 + 10    # 790
        skip_reason_stale = "skip_stale_start_pm"
    elif session == "close":
        target_start_min = 15 * 60 + 15   # 915
        allow_until_min = 15 * 60 + 20    # 920
        skip_reason_stale = "skip_close_stale_start"
    else:
        raise ValueError(f"Unknown session: {session}")

    should_run = 1
    skip_reason = ""
    wait_until_target = 0
    wait_seconds = 0

    if event_name == "schedule":
        if now_min > allow_until_min:
            should_run = 0
            skip_reason = skip_reason_stale
        elif now_min < target_start_min:
            wait_until_target = 1
            wait_seconds = (target_start_min - now_min) * 60

    return {
        "should_run": should_run,
        "skip_reason": skip_reason,
        "wait_until_target": wait_until_target,
        "wait_seconds": wait_seconds,
        "now_min": now_min,
        "target_start_min": target_start_min,
        "allow_until_min": allow_until_min,
    }


# ── AM ────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("now_hhmm,expected", [
    ("0845", {"should_run": 1, "wait_until_target": 1, "wait_seconds": 900}),
    ("0855", {"should_run": 1, "wait_until_target": 1, "wait_seconds": 300}),
    ("0859", {"should_run": 1, "wait_until_target": 1, "wait_seconds": 60}),
    ("0900", {"should_run": 1, "wait_until_target": 0, "wait_seconds": 0}),
    ("0905", {"should_run": 1, "wait_until_target": 0, "wait_seconds": 0}),
    ("0910", {"should_run": 1, "wait_until_target": 0, "wait_seconds": 0}),
    ("0911", {"should_run": 0, "skip_reason": "skip_stale_start_am"}),
    ("0956", {"should_run": 0, "skip_reason": "skip_stale_start_am"}),
    ("1255", {"should_run": 0, "skip_reason": "skip_stale_start_am"}),
])
def test_am_phase_guard(now_hhmm, expected):
    result = _phase_guard("am", now_hhmm, "schedule")
    for k, v in expected.items():
        assert result[k] == v, (
            f"AM {now_hhmm}: {k} expected={v} got={result[k]}"
        )


def test_am_wait_seconds_at_0845():
    """08:45 → 09:00: 15분 = 900초."""
    result = _phase_guard("am", "0845", "schedule")
    assert result["wait_seconds"] == 900


def test_am_wait_seconds_at_0855():
    """08:55 → 09:00: 5분 = 300초."""
    result = _phase_guard("am", "0855", "schedule")
    assert result["wait_seconds"] == 300


def test_am_stale_boundary():
    """09:10 is last valid, 09:11 is stale."""
    ok = _phase_guard("am", "0910", "schedule")
    assert ok["should_run"] == 1

    stale = _phase_guard("am", "0911", "schedule")
    assert stale["should_run"] == 0
    assert stale["skip_reason"] == "skip_stale_start_am"


# ── PM ────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("now_hhmm,expected", [
    ("1245", {"should_run": 1, "wait_until_target": 1, "wait_seconds": 900}),
    ("1300", {"should_run": 1, "wait_until_target": 0, "wait_seconds": 0}),
    ("1305", {"should_run": 1, "wait_until_target": 0}),
    ("1310", {"should_run": 1, "wait_until_target": 0}),
    ("1311", {"should_run": 0, "skip_reason": "skip_stale_start_pm"}),
    ("1400", {"should_run": 0, "skip_reason": "skip_stale_start_pm"}),
])
def test_pm_phase_guard(now_hhmm, expected):
    result = _phase_guard("pm", now_hhmm, "schedule")
    for k, v in expected.items():
        assert result[k] == v, (
            f"PM {now_hhmm}: {k} expected={v} got={result[k]}"
        )


def test_pm_stale_boundary():
    """13:10 is last valid, 13:11 is stale."""
    ok = _phase_guard("pm", "1310", "schedule")
    assert ok["should_run"] == 1

    stale = _phase_guard("pm", "1311", "schedule")
    assert stale["should_run"] == 0
    assert stale["skip_reason"] == "skip_stale_start_pm"


# ── CLOSE ─────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("now_hhmm,expected", [
    ("1500", {"should_run": 1, "wait_until_target": 1, "wait_seconds": 900}),
    ("1510", {"should_run": 1, "wait_until_target": 1, "wait_seconds": 300}),
    ("1515", {"should_run": 1, "wait_until_target": 0, "wait_seconds": 0}),
    ("1518", {"should_run": 1, "wait_until_target": 0}),
    ("1520", {"should_run": 1, "wait_until_target": 0}),
    ("1521", {"should_run": 0, "skip_reason": "skip_close_stale_start"}),
    ("1530", {"should_run": 0, "skip_reason": "skip_close_stale_start"}),
])
def test_close_phase_guard(now_hhmm, expected):
    result = _phase_guard("close", now_hhmm, "schedule")
    for k, v in expected.items():
        assert result[k] == v, (
            f"CLOSE {now_hhmm}: {k} expected={v} got={result[k]}"
        )


def test_close_stale_boundary():
    """15:20 is last valid, 15:21 is stale."""
    ok = _phase_guard("close", "1520", "schedule")
    assert ok["should_run"] == 1

    stale = _phase_guard("close", "1521", "schedule")
    assert stale["should_run"] == 0
    assert stale["skip_reason"] == "skip_close_stale_start"


# ── workflow_dispatch는 schedule 로직 미적용 ─────────────────────────────────

def test_dispatch_bypasses_schedule_gate():
    """workflow_dispatch는 schedule 시간 gate를 타지 않는다."""
    # 늦은 시간도 schedule gate에서 막히지 않아야 함
    for session, hhmm in [("am", "1200"), ("pm", "1600"), ("close", "1600")]:
        result = _phase_guard(session, hhmm, "workflow_dispatch")
        assert result["should_run"] == 1, (
            f"{session} dispatch at {hhmm} should not be blocked by schedule gate"
        )
        assert result["wait_until_target"] == 0


# ── 분 단위 정수 비교 정확성 ─────────────────────────────────────────────────

def test_minute_integer_compare_no_leading_zero_issue():
    """0 prefix가 있는 시간도 정수 비교로 올바르게 처리."""
    # "0845" 파싱 → 8*60+45 = 525
    result = _phase_guard("am", "0845", "schedule")
    assert result["now_min"] == 525
    assert result["target_start_min"] == 540
    # wait = (540 - 525) * 60 = 900
    assert result["wait_seconds"] == 900


def test_stale_detection_using_only_int_compare():
    """HHMM 문자열 비교 없이 정수 비교만으로 stale을 정확히 탐지."""
    # now=0911 → now_min=551 > allow_until=550 → stale
    result = _phase_guard("am", "0911", "schedule")
    assert result["now_min"] == 551
    assert result["allow_until_min"] == 550
    assert result["should_run"] == 0
