# -*- coding: utf-8 -*-
"""workflow DRY_RUN expression 검증 테스트.

workflow_dispatch에서 inputs.dry_run=false이면 DRY_RUN=0이 되어야 한다.
expression: (inputs.dry_run && '1' || '0') 패턴 확인.
"""
import pathlib
import re

WORKFLOW_DIR = pathlib.Path(".github/workflows")
US_WORKFLOW_GLOB = "us-trade-*.yml"
# workflow_dispatch 없는 report/prep도 포함할 수 있으므로 trade- 한정


def _read_workflow(path):
    return path.read_text(encoding="utf-8")


def _get_us_trade_workflows():
    return sorted(WORKFLOW_DIR.glob(US_WORKFLOW_GLOB))


def test_us_trade_workflows_exist():
    wfs = _get_us_trade_workflows()
    assert len(wfs) >= 1, f"us-trade-*.yml 파일이 없다: {WORKFLOW_DIR}"


def test_dry_run_expression_handles_false(monkeypatch):
    """DRY_RUN expression이 inputs.dry_run=false를 올바르게 0으로 해석한다.
    
    잘못된 표현: inputs.dry_run == true && '1' || '0'
    올바른 표현:  (inputs.dry_run && '1' || '0')
    """
    for path in _get_us_trade_workflows():
        content = _read_workflow(path)
        # DRY_RUN 줄이 있는 파일만 검사
        if "DRY_RUN" not in content:
            continue
        # 잘못된 패턴: inputs.dry_run == 'true' 또는 == true (따옴표 없이)
        bad_pattern = r"inputs\.dry_run\s*==\s*['\"]?true['\"]?"
        bad_matches = re.findall(bad_pattern, content, re.IGNORECASE)
        assert not bad_matches, (
            f"{path}: 잘못된 DRY_RUN expression (false 처리 안됨): {bad_matches}\n"
            "올바른 표현: (inputs.dry_run && '1' || '0')"
        )


def test_dry_run_expression_ternary_pattern(monkeypatch):
    """inputs.dry_run && '1' || '0' 패턴이 포함되어 있어야 한다."""
    for path in _get_us_trade_workflows():
        content = _read_workflow(path)
        if "DRY_RUN" not in content or "inputs.dry_run" not in content:
            continue
        # 올바른 패턴: inputs.dry_run && '1' || '0'
        ok_pattern = r"inputs\.dry_run\s*&&\s*'1'\s*\|\|\s*'0'"
        ok_match = re.search(ok_pattern, content)
        assert ok_match, (
            f"{path}: DRY_RUN expression에 올바른 ternary 패턴이 없다\n"
            "expected: inputs.dry_run && '1' || '0'"
        )
