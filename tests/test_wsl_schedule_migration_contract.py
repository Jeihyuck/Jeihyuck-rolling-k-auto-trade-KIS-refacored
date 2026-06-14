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
        "scripts/wsl/run-us-trader.sh": "run-us-am.sh",
        "scripts/wsl/run-us-prep.sh": "trader.us.runner.dispatcher",
        "scripts/wsl/run-us-am.sh": "trader.us.runner.trade_session_runner",
        "scripts/wsl/run-us-afternoon.sh": "--session afternoon",
        "scripts/wsl/run-us-close.sh": "--mode trade-close",
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


def test_us_wsl_scripts_preserve_workflow_entrypoints_and_env_defaults():
    prep = Path("scripts/wsl/run-us-prep.sh").read_text(encoding="utf-8")
    am = Path("scripts/wsl/run-us-am.sh").read_text(encoding="utf-8")
    afternoon = Path("scripts/wsl/run-us-afternoon.sh").read_text(encoding="utf-8")
    close = Path("scripts/wsl/run-us-close.sh").read_text(encoding="utf-8")

    assert "trader.us.runner.dispatcher" in prep
    assert "--mode prep" in prep
    assert "trader.us.runner.trade_session_runner" in am
    assert "--session am" in am
    assert "trader.us.runner.trade_session_runner" in afternoon
    assert "--session afternoon" in afternoon
    assert "trader.us.runner.dispatcher" in close
    assert "--mode trade-close" in close

    for key in (
        "TRADING_REGION",
        "US_AGENT_ENABLED",
        "US_PAPER_TRADING_ENABLED",
        "US_LIVE_TRADING_ENABLED",
        "DISABLE_REAL_TRADING",
        "ALLOW_REAL_ORDER",
        "US_STRATEGY_ENGINE",
        "US_DEFAULT_ENTRY_BOOK",
        "US_MAX_ORDER_USD",
        "US_TICK_TIMEOUT_SEC",
    ):
        assert key in am, f"run-us-am.sh missing {key}"
        assert key in afternoon, f"run-us-afternoon.sh missing {key}"


def test_us_dryrun_forwards_session_argument_to_dispatcher():
    text = Path("scripts/wsl/run-us-dryrun.sh").read_text(encoding="utf-8")
    assert 'run-us-trader.sh "$@"' in text
