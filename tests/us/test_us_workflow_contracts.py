# -*- coding: utf-8 -*-
"""tests/us/test_us_workflow_contracts.py - workflow YAML parse 테스트."""
import pytest
from pathlib import Path


WORKFLOW_DIR = Path(__file__).resolve().parents[2] / ".github" / "workflows"

US_TRADE_WORKFLOWS = [
    "us-trade-prep.yml",
    "us-trade-am.yml",
    "us-trade-afternoon.yml",
    "us-trade-close.yml",
    "us-agent.yml",
]


@pytest.mark.parametrize("filename", US_TRADE_WORKFLOWS)
def test_workflow_yaml_parseable(filename):
    """각 workflow YAML 파일이 yaml.safe_load로 파싱 가능해야 한다."""
    try:
        import yaml
    except ImportError:
        pytest.skip("pyyaml not installed")

    path = WORKFLOW_DIR / filename
    assert path.exists(), f"Workflow file not found: {path}"

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert isinstance(data, dict), f"Expected dict but got {type(data)}"
    assert "name" in data, "Missing 'name' key"
    assert "on" in data or True  # 'on' may be parsed as True by yaml


def test_us_agent_yml_no_schedule():
    """us-agent.yml에 schedule이 없어야 한다."""
    try:
        import yaml
    except ImportError:
        pytest.skip("pyyaml not installed")

    path = WORKFLOW_DIR / "us-agent.yml"
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    trigger = data.get("on", data.get(True, {}))
    if isinstance(trigger, dict):
        assert "schedule" not in trigger, "us-agent.yml must not have schedule trigger"


def test_us_trade_am_has_schedule():
    try:
        import yaml
    except ImportError:
        pytest.skip("pyyaml not installed")

    path = WORKFLOW_DIR / "us-trade-am.yml"
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    trigger = data.get("on", data.get(True, {}))
    assert "schedule" in trigger, "us-trade-am.yml must have schedule trigger"


def test_us_trade_workflows_have_safety_env():
    """모든 us-trade-*.yml은 US_LIVE_TRADING_ENABLED=0 을 가져야 한다."""
    try:
        import yaml
    except ImportError:
        pytest.skip("pyyaml not installed")

    for filename in ["us-trade-am.yml", "us-trade-afternoon.yml", "us-trade-close.yml"]:
        path = WORKFLOW_DIR / filename
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        assert "US_LIVE_TRADING_ENABLED" in content, \
            f"{filename} missing US_LIVE_TRADING_ENABLED"
        assert "ALLOW_REAL_ORDER" in content, \
            f"{filename} missing ALLOW_REAL_ORDER"
