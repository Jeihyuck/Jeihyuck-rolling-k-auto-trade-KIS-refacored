from pathlib import Path


def _workflow_text() -> str:
    return Path(".github/workflows/trade-close.yml").read_text(encoding="utf-8")


def test_trade_close_workflow_is_close_exit_only_contract():
    text = _workflow_text()
    required = {
        'PB1_SESSION_KIND: "close"',
        'PB1_FORCE_TRADE_SESSION: "close"',
        'FORCE_MARKET_WINDOW: "close"',
        'FORCE_PB1_PHASE: "exit"',
        'PB1_ENTRY_ENABLED: "0"',
        'PB1_CLOSE_LIQUIDATION_ENABLED: "1"',
        'PB1_CLOSE_ALLOW_KIS_HOLDINGS_WITHOUT_FINAL30: "1"',
    }
    for needle in required:
        assert needle in text
    forbidden = {
        'PB1_SESSION_KIND: "afternoon"',
        'PB1_FORCE_TRADE_SESSION: "afternoon"',
        'FORCE_MARKET_WINDOW: "afternoon"',
    }
    for needle in forbidden:
        assert needle not in text
