from __future__ import annotations

from pathlib import Path


WORKFLOW_DIR = Path(__file__).resolve().parents[2] / ".github" / "workflows"


def _read(name: str) -> str:
    return (WORKFLOW_DIR / name).read_text(encoding="utf-8")


def test_am_workflow_prints_pnl_report_to_console_and_summary() -> None:
    content = _read("us-trade-am.yml")
    assert "- name: Print US PnL report to console and summary" in content
    assert "cat reports/us_pnl/latest_us_pnl_report.md" in content
    assert "GITHUB_STEP_SUMMARY" in content
    assert "[US_PNL_REPORT_MD][BEGIN]" in content
    assert "[US_PNL_REPORT_MD][END]" in content


def test_afternoon_workflow_prints_pnl_report_to_console_and_summary() -> None:
    content = _read("us-trade-afternoon.yml")
    assert "- name: Print US PnL report to console and summary" in content
    assert "cat reports/us_pnl/latest_us_pnl_report.md" in content
    assert "GITHUB_STEP_SUMMARY" in content
    assert "[US_PNL_REPORT_MD][BEGIN]" in content
    assert "[US_PNL_REPORT_MD][END]" in content
