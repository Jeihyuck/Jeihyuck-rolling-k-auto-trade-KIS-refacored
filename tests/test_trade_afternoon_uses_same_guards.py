from pathlib import Path


REQUIRED_FLAGS = [
    "PB1_BLOCK_ENTRY_AFTER_EXIT",
    "PB1_RECONCILE_ONLY_AFTER_ORDER_SUBMIT",
    "PB1_SOLD_TODAY_COOLDOWN",
    "PB1_KIS_HOLDINGS_AUTHORITATIVE",
    "PB1_REQUIRE_FILL_BEFORE_POSITION_UPDATE",
    "PB1_TIME_STOP_BASIS",
    "PB1_BLOCK_INSUFFICIENT_OHLCV",
    "PB1_REQUIRE_EXPLICIT_TRIGGER_BYPASS",
    "PB1_KIS_RATE_LIMIT_SAFE",
]


def test_trade_afternoon_uses_same_guards():
    repo_root = Path(__file__).resolve().parents[1]
    am_text = (repo_root / ".github/workflows/trade-am.yml").read_text(encoding="utf-8")
    afternoon_text = (repo_root / ".github/workflows/trade-afternoon.yml").read_text(encoding="utf-8")

    for flag in REQUIRED_FLAGS:
        assert flag in am_text
        assert flag in afternoon_text