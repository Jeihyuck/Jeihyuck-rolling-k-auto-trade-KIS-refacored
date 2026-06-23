from pathlib import Path


def _workflow_text() -> str:
    return Path(".github/workflows/trade-close.yml").read_text(encoding="utf-8")


def _env() -> dict[str, str]:
    env: dict[str, str] = {}
    in_env = False
    for line in _workflow_text().splitlines():
        if line.strip() == "env:":
            in_env = True
            continue
        if in_env and line and not line.startswith(" "):
            break
        if in_env and line.startswith("  ") and ":" in line:
            key, value = line.strip().split(":", 1)
            env[key] = value.strip().strip('"')
    return env


def test_trade_close_workflow_is_close_exit_only_contract():
    env = _env()
    assert env["PB1_SESSION_KIND"] == "close"
    assert env["PB1_FORCE_TRADE_SESSION"] == "close"
    assert env["FORCE_MARKET_WINDOW"] == "close"
    assert env["FORCE_PB1_PHASE"] == "exit"
    assert env["PB1_ENTRY_ENABLED"] == "0"
    assert env["ENTRY_ENABLED"] == "0"
    assert env["PB1_CLOSE_LIQUIDATION_ENABLED"] == "1"
    assert env["PB1_CLOSE_ALLOW_KIS_HOLDINGS_WITHOUT_FINAL30"] == "1"
    assert env["KR_BLOCK_EMPTY_FINAL30_ENGINE_BOOT"] == "1"
    assert env["KR_TRADE_FINAL30_DB_FALLBACK"] == "1"
    assert env["KR_FINAL30_SOURCE_OF_TRUTH"] == "db_roundtrip"
    assert env["DB_LOCK_TIMEOUT_MS"] == "5000"
    assert env["DB_STATEMENT_TIMEOUT_MS"] == "15000"
    assert env["DB_IDLE_IN_TX_SESSION_TIMEOUT_MS"] == "15000"
    assert env["DB_IDLE_IN_TX_TIMEOUT_MS"] == "15000"
    assert env["PB1_FAIL_OPEN_ON_ORDER_LOOKUP_TIMEOUT"] == "1"
    assert env["DRY_RUN"] == "0"
    assert env["DISABLE_LIVE_TRADING"] == "0"
    assert env["LIVE_TRADING_ENABLED"] == "1"
    assert env["KIS_HTTP_ENABLED"] == "1"
    assert env.get("STRATEGY_MODE") != "INTENT_ONLY"
    assert env.get("FORCE_STRATEGY_MODE") != "INTENT_ONLY"
    assert env.get("PB1_SESSION_KIND") != "afternoon"
    assert env.get("PB1_FORCE_TRADE_SESSION") != "afternoon"
    assert env.get("FORCE_MARKET_WINDOW") != "afternoon"


def test_trade_close_workflow_comments_describe_practice_actual_backup():
    text = _workflow_text()
    assert "practice-account actual SELL liquidation backup" in text
    forbidden = ["afternoon normalize", "INTENT_ONLY backup", "dry-run close", "DISABLED: trade-close"]
    for needle in forbidden:
        assert needle not in text
