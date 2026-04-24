"""
tests/test_workflow_cron_daily_contract.py

모든 workflow cron이 매일 실행(* * *)이고 prep timeout이 90분인지 계약적 검증.
"""
from __future__ import annotations

import re
from pathlib import Path

WORKFLOWS_DIR = Path(__file__).parent.parent / ".github" / "workflows"


def _read(name: str) -> str:
    return (WORKFLOWS_DIR / name).read_text()


def test_trade_prep_cron_is_daily():
    content = _read("trade-prep.yml")
    assert re.search(r'cron:\s*"0 22 \* \* \*"', content), (
        "trade-prep.yml cron should be '0 22 * * *' (daily, 07:00 KST)"
    )


def test_trade_prep_timeout_90():
    content = _read("trade-prep.yml")
    assert re.search(r"timeout-minutes:\s*90", content), (
        "trade-prep.yml timeout-minutes should be 90"
    )


def test_trade_am_cron_is_daily():
    content = _read("trade-am.yml")
    assert re.search(r'cron:\s*"0 0 \* \* \*"', content), (
        "trade-am.yml cron should be '0 0 * * *' (daily, 09:00 KST)"
    )
    # 요일 제한(1-5) 없어야 함
    assert "1-5" not in content or "Every day" in content or "0 0 * * *" in content


def test_trade_pm_cron_is_daily():
    content = _read("trade-pm.yml")
    assert re.search(r'cron:\s*"0 4 \* \* \*"', content), (
        "trade-pm.yml cron should be '0 4 * * *' (daily, 13:00 KST)"
    )


def test_trade_close_cron_is_daily():
    content = _read("trade-close.yml")
    assert re.search(r'cron:\s*"15 6 \* \* \*"', content), (
        "trade-close.yml cron should be '15 6 * * *' (daily, 15:15 KST)"
    )


def test_no_workflow_uses_weekday_only_cron():
    """모든 workflow에 1-5 요일 제한 cron이 없어야 한다."""
    for wf in ["trade-prep.yml", "trade-am.yml", "trade-pm.yml", "trade-close.yml"]:
        content = _read(wf)
        # cron 줄에 1-5 가 있으면 안 됨
        for line in content.splitlines():
            if "cron:" in line:
                assert "1-5" not in line, (
                    f"{wf} still has weekday-only cron: {line.strip()}"
                )
