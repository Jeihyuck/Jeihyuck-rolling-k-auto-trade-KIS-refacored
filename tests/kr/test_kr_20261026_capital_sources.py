"""Ensure KR 100m operational entrypoints stay aligned after 2026-10-25 reset."""
from pathlib import Path

import pytest

WORKFLOWS = [
    ".github/workflows/trade-prep.yml",
    ".github/workflows/trade-am.yml",
    ".github/workflows/trade-afternoon.yml",
    ".github/workflows/trade-close.yml",
    ".github/workflows/unified-pipeline.yml",
]
WRAPPERS = [
    "scripts/wsl/run-kr-prep.sh",
    "scripts/wsl/run-kr-am.sh",
    "scripts/wsl/run-kr-afternoon.sh",
    "scripts/wsl/run-kr-close.sh",
]


@pytest.mark.parametrize("path",WORKFLOWS)
def test_kr_workflow_capital_100m(path):
    text = Path(path).read_text(encoding="utf-8")
    assert 'PAPER_MAX_CAPITAL_KRW: "100000000"' in text
    assert 'EXPECTED_PRACTICE_CAPITAL_KRW: "100000000"' in text
    assert 'PAPER_MAX_CAPITAL_KRW: "50000000"' not in text
    assert 'EXPECTED_PRACTICE_CAPITAL_KRW: "50000000"' not in text


@pytest.mark.parametrize("path",WRAPPERS)
def test_kr_wsl_capital_reasserted_after_dotenv(path):
    text = Path(path).read_text(encoding="utf-8")
    assert text.index('if [[ -f .env ]]') < text.index("export PAPER_MAX_CAPITAL_KRW=100000000")
    assert "export EXPECTED_PRACTICE_CAPITAL_KRW=100000000" in text
    assert 'if [[ "$STRATEGY_ENV" == "practice" && "$KIS_ENV" == "practice" ]]' in text


def test_kr_new_epoch_gate_in_deploy_preflight_not_in_us_code():
    text=Path("scripts/wsl/deploy-preflight.sh").read_text(encoding="utf-8")
    assert '"${market^^}" == "KR" && "$trade_date" == "2026-10-26"' in text
    assert 'scripts/kr/check_20261026_new_practice_account.py' in text
    assert 'action=BLOCK_TRADING' in text


def test_us_capital_not_modified_by_kr_pr():
    us=Path("scripts/wsl/run-us-am.sh").read_text(encoding="utf-8")
    assert "US_PAPER_MAX_CAPITAL_KRW" in us
