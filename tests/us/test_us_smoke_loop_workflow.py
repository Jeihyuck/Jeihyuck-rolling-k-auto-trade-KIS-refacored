# -*- coding: utf-8 -*-
"""smoke_loop workflow 입력값 계약 테스트.

us-trade-am.yml 과 us-trade-afternoon.yml에 smoke_loop 입력이 정의되어야 한다.
"""
import os
import pytest
import yaml


_WORKFLOW_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", ".github", "workflows"
)


def _load_workflow(filename: str) -> dict:
    path = os.path.join(_WORKFLOW_DIR, filename)
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.mark.parametrize("workflow_file", [
    "us-trade-am.yml",
    "us-trade-afternoon.yml",
])
def test_smoke_loop_input_defined(workflow_file):
    """smoke_loop 입력이 workflow_dispatch inputs에 정의되어야 한다."""
    wf = _load_workflow(workflow_file)
    inputs = (
        wf.get("on", {})
          .get("workflow_dispatch", {})
          .get("inputs", {})
    )
    assert "smoke_loop" in inputs, (
        f"{workflow_file} must define smoke_loop in workflow_dispatch inputs"
    )


@pytest.mark.parametrize("workflow_file", [
    "us-trade-am.yml",
    "us-trade-afternoon.yml",
])
def test_smoke_loop_input_is_boolean_type(workflow_file):
    """smoke_loop 입력의 type이 boolean이어야 한다."""
    wf = _load_workflow(workflow_file)
    inputs = (
        wf.get("on", {})
          .get("workflow_dispatch", {})
          .get("inputs", {})
    )
    smoke = inputs.get("smoke_loop", {})
    assert smoke.get("type") == "boolean", (
        f"{workflow_file}: smoke_loop input must have type: boolean"
    )


@pytest.mark.parametrize("workflow_file", [
    "us-trade-am.yml",
    "us-trade-afternoon.yml",
])
def test_smoke_loop_default_is_false(workflow_file):
    """smoke_loop 입력의 기본값이 false여야 한다."""
    wf = _load_workflow(workflow_file)
    inputs = (
        wf.get("on", {})
          .get("workflow_dispatch", {})
          .get("inputs", {})
    )
    smoke = inputs.get("smoke_loop", {})
    assert smoke.get("default") is False, (
        f"{workflow_file}: smoke_loop default must be false"
    )


def test_us_agent_yml_has_kis_diag_option():
    """us-agent.yml의 mode 선택지에 kis-diag가 포함되어야 한다."""
    wf = _load_workflow("us-agent.yml")
    inputs = (
        wf.get("on", {})
          .get("workflow_dispatch", {})
          .get("inputs", {})
    )
    options = inputs.get("mode", {}).get("options", [])
    assert "kis-diag" in options, (
        "us-agent.yml mode options must include 'kis-diag'"
    )
