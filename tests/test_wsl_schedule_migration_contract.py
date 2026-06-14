from pathlib import Path

WORKFLOWS = Path(".github/workflows")
ORDER_WORKFLOWS = [
    "trade-prep.yml",
    "trade-am.yml",
    "trade-afternoon.yml",
    "trade-close.yml",
    "trade-pm.yml",
    "us-trade-prep.yml",
    "us-trade-am.yml",
    "us-trade-afternoon.yml",
    "us-trade-close.yml",
    "us-agent.yml",
    "us-market-dispatcher.yml",
    "unified-pipeline.yml",
]
SAFE_ENV = [
    'DRY_RUN: "1"',
    'DISABLE_LIVE_TRADING: "1"',
    'LIVE_TRADING_ENABLED: "0"',
    'STRATEGY_MODE: "INTENT_ONLY"',
    'FORCE_STRATEGY_MODE: "INTENT_ONLY"',
]


def test_order_capable_workflows_are_manual_safe_mode_only():
    for filename in ORDER_WORKFLOWS:
        path = WORKFLOWS / filename
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        assert "schedule:" not in text, f"{filename} must not have schedule trigger"
        assert "workflow_dispatch:" in text, f"{filename} must keep workflow_dispatch"
        for expected in SAFE_ENV:
            assert expected in text, f"{filename} missing {expected}"


def test_wsl_runner_scripts_exist_and_log_to_runtime():
    expected = {
        "scripts/wsl/run-kr-trader.sh": "runtime/wsl-kr-trader.log",
        "scripts/wsl/run-us-trader.sh": "runtime/wsl-us-trader.log",
        "scripts/wsl/run-kr-dryrun.sh": "run-kr-trader.sh",
        "scripts/wsl/run-us-dryrun.sh": "run-us-trader.sh",
    }
    for script, marker in expected.items():
        path = Path(script)
        assert path.exists(), f"missing {script}"
        text = path.read_text(encoding="utf-8")
        assert marker in text, f"{script} missing {marker}"


def test_env_file_is_not_committed_and_example_exists():
    assert not Path(".env").exists(), ".env must not be committed"
    assert Path(".env.example").exists(), ".env.example must exist"
