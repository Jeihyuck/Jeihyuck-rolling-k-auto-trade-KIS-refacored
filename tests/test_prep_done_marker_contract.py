"""
tests/test_prep_done_marker_contract.py

prep_runner.py가 다음 마커를 내보내는지 소스 수준에서 검증:
- [PREP][DB_VERIFY][OK]
- [PREP][CANONICAL][QUALITY] trade_can_proceed=1
- [PREP][DONE] env=... as_of=... final30=... trade_can_proceed=
- [PREP][EXIT] status=OK
- LEDGER_EVENT event_type=PREP_DONE
"""
from __future__ import annotations

import re
from pathlib import Path

PREP_RUNNER = Path(__file__).parent.parent / "trader" / "prep_runner.py"


def _src() -> str:
    return PREP_RUNNER.read_text()


def test_prep_db_verify_ok_log_emitted():
    assert re.search(r"\[PREP\]\[DB_VERIFY\]\[OK\]", _src()), (
        "prep_runner must emit [PREP][DB_VERIFY][OK] after DB roundtrip"
    )


def test_prep_canonical_quality_log_emitted():
    assert re.search(r"\[PREP\]\[CANONICAL\]\[QUALITY\].*trade_can_proceed=1", _src()), (
        "prep_runner must emit [PREP][CANONICAL][QUALITY] trade_can_proceed=1 when gate passes"
    )


def test_prep_done_log_has_env_and_status_ok():
    assert re.search(r"\[PREP\]\[DONE\].*env=.*status=OK", _src()), (
        "prep_runner must emit [PREP][DONE] with env= and status=OK at end of main()"
    )


def test_prep_exit_ok_log_emitted():
    assert re.search(r"\[PREP\]\[EXIT\] status=OK", _src()), (
        "prep_runner must emit [PREP][EXIT] status=OK just before return 0"
    )


def test_prep_exit_ok_before_return_0():
    """[PREP][EXIT] status=OK 는 return 0 직전에 위치해야 함."""
    src = _src()
    exit_ok_pos = src.rfind("[PREP][EXIT] status=OK")
    return_0_pos = src.rfind("return 0")
    assert exit_ok_pos != -1, "[PREP][EXIT] status=OK not found"
    assert return_0_pos != -1, "return 0 not found"
    assert exit_ok_pos < return_0_pos, (
        "[PREP][EXIT] status=OK must appear before final return 0"
    )


def test_ledger_event_prep_done_upserted():
    src = _src()
    assert re.search(r"event_type=['\"]?PREP_DONE['\"]?", src), (
        "prep_runner must upsert PREP_DONE ledger event"
    )
    assert re.search(r"upsert_prep_event", src), (
        "prep_runner must call upsert_prep_event for PREP_DONE"
    )


def test_verify_prep_log_checks_new_patterns():
    """verify_prep_log.py에 새 패턴이 등록되었는지 확인."""
    verify_src = (
        Path(__file__).parent.parent / "scripts" / "verify_prep_log.py"
    ).read_text()

    assert "prep_db_verify_ok" in verify_src, (
        "verify_prep_log.py must define 'prep_db_verify_ok' pattern"
    )
    assert "prep_canonical_quality" in verify_src, (
        "verify_prep_log.py must define 'prep_canonical_quality' pattern"
    )
    assert "prep_exit_ok" in verify_src, (
        "verify_prep_log.py must define 'prep_exit_ok' pattern"
    )


def test_verify_prep_log_fails_on_missing_exit_ok(tmp_path):
    """[PREP][EXIT] status=OK가 없는 로그는 실패로 판별되어야 함."""
    import subprocess
    import sys

    # 최소한의 성공 로그 (EXIT OK 없음)
    incomplete_log = tmp_path / "incomplete.log"
    incomplete_log.write_text(
        "[PREP][START] env=practice as_of=2025-01-06\n"
        "[PREP][DONE] as_of=2025-01-06 status=OK\n"
        # [PREP][EXIT] status=OK 고의로 누락
    )

    verify_script = (
        Path(__file__).parent.parent / "scripts" / "verify_prep_log.py"
    )
    result = subprocess.run(
        [sys.executable, str(verify_script), str(incomplete_log)],
        capture_output=True,
        text=True,
    )
    # verify_prep_log.py 가 실패(exit 1) 또는 FAIL 출력해야 함
    output_combined = result.stdout + result.stderr
    assert result.returncode != 0 or "FAIL" in output_combined, (
        "verify_prep_log.py should FAIL when [PREP][EXIT] status=OK is missing"
    )
