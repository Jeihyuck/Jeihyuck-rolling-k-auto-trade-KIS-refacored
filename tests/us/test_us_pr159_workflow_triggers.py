from pathlib import Path

import yaml


BASE_BRANCH = "stabilize/04-lifecycle-contract-recovery-20261004"


def _pull_request_branches(path: str) -> list[str]:
    workflow = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    triggers = workflow.get("on", workflow.get(True))
    return triggers["pull_request"]["branches"]


def test_pr159_required_workflows_target_exact_stabilization_base():
    expected = {
        ".github/workflows/us-harness.yml": [
            "dual-agent",
            "stabilize/01-execution-state-contract-20261004",
            "stabilize/02-us-broker-recovery-20261004",
            "stabilize/03-kr-broker-recovery-20261004",
            BASE_BRANCH,
        ],
        ".github/workflows/pr-kr-trading-contracts.yml": [
            "dual-agent",
            "main",
            "master",
            "stabilize/02-us-broker-recovery-20261004",
            "stabilize/03-kr-broker-recovery-20261004",
            BASE_BRANCH,
        ],
        ".github/workflows/trading-epoch-ci.yml": [
            "dual-agent",
            "stabilize/01-execution-state-contract-20261004",
            "stabilize/02-us-broker-recovery-20261004",
            "stabilize/03-kr-broker-recovery-20261004",
            BASE_BRANCH,
        ],
    }

    for path, expected_branches in expected.items():
        assert _pull_request_branches(path) == expected_branches
