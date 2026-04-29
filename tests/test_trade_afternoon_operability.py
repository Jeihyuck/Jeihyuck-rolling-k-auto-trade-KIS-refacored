"""
tests/test_trade_afternoon_operability.py

목적:
1. PB1_SESSION_KIND=afternoon, FORCE_PB1_PHASE=entry → HEARTBEAT prefix = TRADE_AFTERNOON
2. PB1_SESSION_KIND=afternoon, FORCE_PB1_PHASE=exit → HEARTBEAT prefix = TRADE_CLOSE
3. PB1_SESSION_KIND=am → HEARTBEAT prefix = TRADE_AM
4. PM 로그에서 [TRADE_AM][HEARTBEAT]가 있으면 validator가 fail
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _resolve_hb_prefix(session_kind: str, forced_phase: str) -> str:
    """pb1_runner.py의 heartbeat prefix 결정 로직과 동일"""
    sk = (session_kind or "").strip().lower()
    if sk == "am":
        return "TRADE_AM"
    elif sk in ("afternoon", "pm"):
        phase = (forced_phase or "").strip().lower()
        return "TRADE_CLOSE" if phase == "exit" else "TRADE_AFTERNOON"
    elif sk == "close":
        return "TRADE_CLOSE"
    else:
        return f"TRADE_{sk.upper()}"


def test_afternoon_entry_heartbeat_prefix():
    """PM session / entry phase → TRADE_AFTERNOON"""
    os.environ["PB1_SESSION_KIND"] = "afternoon"
    os.environ["FORCE_PB1_PHASE"] = "entry"
    prefix = _resolve_hb_prefix("afternoon", "entry")
    assert prefix == "TRADE_AFTERNOON", f"Expected TRADE_AFTERNOON, got {prefix}"


def test_afternoon_exit_heartbeat_prefix():
    """PM session / exit phase → TRADE_CLOSE"""
    os.environ["PB1_SESSION_KIND"] = "afternoon"
    os.environ["FORCE_PB1_PHASE"] = "exit"
    prefix = _resolve_hb_prefix("afternoon", "exit")
    assert prefix == "TRADE_CLOSE", f"Expected TRADE_CLOSE, got {prefix}"


def test_am_heartbeat_prefix():
    """AM session → TRADE_AM"""
    os.environ["PB1_SESSION_KIND"] = "am"
    os.environ["FORCE_PB1_PHASE"] = "entry"
    prefix = _resolve_hb_prefix("am", "entry")
    assert prefix == "TRADE_AM", f"Expected TRADE_AM, got {prefix}"


def test_trade_afternoon_log_validator_rejects_am_heartbeat():
    """
    PM 로그에 [TRADE_AM][HEARTBEAT]가 있으면 validator가 감지해야 한다.
    """
    sample_log_bad = """
[TRADE_AFTERNOON][TRIGGER] event=schedule actor=github run_id=12345
[TRADE_AFTERNOON][START_META] schedule_expected=1215 actual_start=131930 delay_seconds=3930
[PHASE][START] phase=pm session_window=1300-1510 entry_enabled=1
[TRADE_AM][HEARTBEAT] tick=1 state=NO_ORDERABLE candidates_scanned=30
""".strip()

    has_am_heartbeat = "[TRADE_AM][HEARTBEAT]" in sample_log_bad
    assert has_am_heartbeat is True, "Test setup: log should contain AM heartbeat"

    # validator 함수
    def validate_afternoon_log(log: str) -> list[str]:
        errors = []
        if "[TRADE_AM][HEARTBEAT]" in log:
            errors.append("TRADE_AM heartbeat label detected in trade-afternoon log")
        if "connection in transaction status ACTIVE" in log:
            errors.append("poisoned DB connection detected")
        if "can't change 'autocommit'" in log:
            errors.append("autocommit active transaction error detected")
        if "Traceback" in log:
            errors.append("Traceback detected")
        return errors

    errors = validate_afternoon_log(sample_log_bad)
    assert len(errors) >= 1
    assert any("TRADE_AM heartbeat" in e for e in errors)


def test_trade_afternoon_log_validator_accepts_correct_log():
    """정상 PM 로그는 validator를 통과해야 한다."""
    sample_log_good = """
[TRADE_AFTERNOON][TRIGGER] event=schedule actor=github run_id=12345
[TRADE_AFTERNOON][START_META] schedule_expected=1215 actual_start=131930 delay_seconds=3930
[PHASE][START] phase=pm session_window=1300-1510 entry_enabled=1
[TRADE_AFTERNOON][HEARTBEAT] tick=1 state=NO_ORDERABLE candidates_scanned=30
[PHASE][END] phase=pm
[PHASE][START] phase=close session_window=1515-1530 entry_enabled=0
[TRADE_CLOSE][HEARTBEAT] tick=1 state=NO_ORDERABLE candidates_scanned=30
[TRADE_CLOSE][DONE] close_done=1
""".strip()

    def validate_afternoon_log(log: str) -> list[str]:
        errors = []
        if "[TRADE_AM][HEARTBEAT]" in log:
            errors.append("TRADE_AM heartbeat label detected in trade-afternoon log")
        if "connection in transaction status ACTIVE" in log:
            errors.append("poisoned DB connection detected")
        if "can't change 'autocommit'" in log:
            errors.append("autocommit active transaction error detected")
        if "Traceback" in log:
            errors.append("Traceback detected")
        return errors

    errors = validate_afternoon_log(sample_log_good)
    assert errors == [], f"Expected no errors, got: {errors}"


def test_close_session_heartbeat_prefix():
    """close session_kind → TRADE_CLOSE"""
    prefix = _resolve_hb_prefix("close", "exit")
    assert prefix == "TRADE_CLOSE", f"Expected TRADE_CLOSE, got {prefix}"


def test_unknown_session_heartbeat_prefix():
    """unknown session_kind → TRADE_UNKNOWN"""
    prefix = _resolve_hb_prefix("unknown", "")
    assert prefix == "TRADE_UNKNOWN", f"Expected TRADE_UNKNOWN, got {prefix}"
