"""test_us_workflow_schedule_contract.py

GitHub Actions 워크플로우 파일의 스케줄/검증 계약 테스트.
- 다중화 cron 수 확인
- force_now description에 DRY_RUN/OFFLINE 안내 포함 확인
- FINISH / RUN_SUMMARY / EXIT 마커 존재 확인
- Verify log contract 스크립트에 already_ran/FINISH 처리 포함 확인
- already_prepared 종료 경로 exit 0 처리 확인
- skip_stale_start_us_prep 경로 exit 1 처리 확인
- final_duplicate_guard step 존재 확인 (am, afternoon)
- check_us_afternoon_already_ran 함수 존재 확인
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

WORKFLOWS_DIR = Path(__file__).parent.parent.parent / ".github" / "workflows"
REPOS_PY = Path(__file__).parent.parent.parent / "trader" / "us" / "db" / "repos.py"

PREP_FILE = WORKFLOWS_DIR / "us-trade-prep.yml"
AM_FILE = WORKFLOWS_DIR / "us-trade-am.yml"
AFTERNOON_FILE = WORKFLOWS_DIR / "us-trade-afternoon.yml"


@pytest.fixture(scope="module")
def prep_text():
    return PREP_FILE.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def am_text():
    return AM_FILE.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def afternoon_text():
    return AFTERNOON_FILE.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def repos_text():
    return REPOS_PY.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# 1. 다중화 cron 수 확인
# ---------------------------------------------------------------------------

def test_us_trade_workflows_have_no_crons(prep_text, am_text, afternoon_text):
    """주문 가능 US workflow는 GitHub Actions schedule을 가지면 안 된다."""
    for name, text in {
        "us-trade-prep.yml": prep_text,
        "us-trade-am.yml": am_text,
        "us-trade-afternoon.yml": afternoon_text,
    }.items():
        assert "schedule:" not in text, f"{name} must not have schedule trigger"
        assert "workflow_dispatch:" in text, f"{name} must keep workflow_dispatch"


def test_us_trade_workflows_default_to_safe_mode(prep_text, am_text, afternoon_text):
    """GitHub 수동 실행 기본값은 주문 불가 안전모드여야 한다."""
    for name, text in {
        "us-trade-prep.yml": prep_text,
        "us-trade-am.yml": am_text,
        "us-trade-afternoon.yml": afternoon_text,
    }.items():
        for expected in (
            'DRY_RUN: "1"',
            'DISABLE_LIVE_TRADING: "1"',
            'LIVE_TRADING_ENABLED: "0"',
            'STRATEGY_MODE: "INTENT_ONLY"',
            'FORCE_STRATEGY_MODE: "INTENT_ONLY"',
        ):
            assert expected in text, f"{name} missing safe env {expected}"


# ---------------------------------------------------------------------------
# 2. force_now description에 DRY_RUN/OFFLINE 안내 포함 확인
# ---------------------------------------------------------------------------

def test_am_force_now_description_mentions_dry_run(am_text):
    """us-trade-am.yml force_now description에 DRY_RUN/OFFLINE 경고가 있어야 한다."""
    assert "DRY_RUN" in am_text or "OFFLINE" in am_text, (
        "us-trade-am.yml force_now description에 DRY_RUN/OFFLINE 안내가 없습니다."
    )


def test_afternoon_force_now_description_mentions_dry_run(afternoon_text):
    """us-trade-afternoon.yml force_now description에 DRY_RUN/OFFLINE 경고가 있어야 한다."""
    assert "DRY_RUN" in afternoon_text or "OFFLINE" in afternoon_text, (
        "us-trade-afternoon.yml force_now description에 DRY_RUN/OFFLINE 안내가 없습니다."
    )


# ---------------------------------------------------------------------------
# 3. FINISH / RUN_SUMMARY / EXIT 마커 존재 확인
# ---------------------------------------------------------------------------

def test_prep_has_finish_marker(prep_text):
    """us-trade-prep.yml에 [US_PREP][FINISH] 마커가 있어야 한다."""
    assert "[US_PREP][FINISH]" in prep_text, (
        "us-trade-prep.yml에 [US_PREP][FINISH] 마커가 없습니다."
    )


def test_am_has_finish_marker(am_text):
    """us-trade-am.yml에 [US_TRADE_AM][FINISH] 마커가 있어야 한다."""
    assert "[US_TRADE_AM][FINISH]" in am_text, (
        "us-trade-am.yml에 [US_TRADE_AM][FINISH] 마커가 없습니다."
    )


def test_afternoon_has_finish_marker(afternoon_text):
    """us-trade-afternoon.yml에 [US_TRADE_AFTERNOON][FINISH] 마커가 있어야 한다."""
    assert "[US_TRADE_AFTERNOON][FINISH]" in afternoon_text, (
        "us-trade-afternoon.yml에 [US_TRADE_AFTERNOON][FINISH] 마커가 없습니다."
    )


# ---------------------------------------------------------------------------
# 4. Verify log contract에 already_ran 처리 포함 확인
# ---------------------------------------------------------------------------

def test_am_verify_handles_already_ran(am_text):
    """us-trade-am.yml Verify 스크립트가 already_ran을 성공으로 처리해야 한다."""
    assert "already_ran" in am_text, (
        "us-trade-am.yml Verify log contract에 already_ran 처리가 없습니다."
    )


def test_afternoon_verify_handles_already_ran(afternoon_text):
    """us-trade-afternoon.yml Verify 스크립트가 already_ran을 성공으로 처리해야 한다."""
    assert "already_ran" in afternoon_text, (
        "us-trade-afternoon.yml Verify log contract에 already_ran 처리가 없습니다."
    )


# ---------------------------------------------------------------------------
# 5. already_prepared 경로 exit 0 처리 확인 (prep)
# ---------------------------------------------------------------------------

def test_prep_already_prepared_exits_zero(prep_text):
    """us-trade-prep.yml Verify log contract에 already_prepared → exit 0 처리가 있어야 한다."""
    # already_prepared 뒤에 exit 0이 있어야 함
    match = re.search(r"already_prepared.*?exit\s+0", prep_text, re.DOTALL)
    assert match is not None, (
        "us-trade-prep.yml Verify log contract에 already_prepared → exit 0 처리가 없습니다."
    )


# ---------------------------------------------------------------------------
# 6. skip_stale_start_us_prep 경로 exit 1 처리 확인 (prep)
# ---------------------------------------------------------------------------

def test_prep_stale_start_exits_one(prep_text):
    """us-trade-prep.yml Verify log contract에 skip_stale_start_us_prep → exit 1이 있어야 한다."""
    match = re.search(r"skip_stale_start_us_prep.*?exit\s+1", prep_text, re.DOTALL)
    assert match is not None, (
        "us-trade-prep.yml Verify log contract에 skip_stale_start_us_prep → exit 1 처리가 없습니다."
    )


# ---------------------------------------------------------------------------
# 7. final_duplicate_guard step 존재 확인 (am, afternoon)
# ---------------------------------------------------------------------------

def test_am_has_final_duplicate_guard_step(am_text):
    """us-trade-am.yml에 Final AM duplicate guard 스텝이 있어야 한다."""
    assert "Final AM duplicate guard" in am_text or "final_duplicate_guard" in am_text, (
        "us-trade-am.yml에 final_duplicate_guard step이 없습니다."
    )


def test_afternoon_has_final_duplicate_guard_step(afternoon_text):
    """us-trade-afternoon.yml에 Final Afternoon duplicate guard 스텝이 있어야 한다."""
    assert "Final Afternoon duplicate guard" in afternoon_text or "final_duplicate_guard" in afternoon_text, (
        "us-trade-afternoon.yml에 final_duplicate_guard step이 없습니다."
    )


# ---------------------------------------------------------------------------
# 8. check_us_afternoon_already_ran 함수 존재 확인
# ---------------------------------------------------------------------------

def test_repos_py_has_check_us_afternoon_already_ran(repos_text):
    """trader/us/db/repos.py에 check_us_afternoon_already_ran 함수가 있어야 한다."""
    assert "def check_us_afternoon_already_ran" in repos_text, (
        "trader/us/db/repos.py에 check_us_afternoon_already_ran 함수가 없습니다."
    )
