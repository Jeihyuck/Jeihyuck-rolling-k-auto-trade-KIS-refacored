# -*- coding: utf-8 -*-
"""US prep-AM-afternoon pipeline contract tests."""

from pathlib import Path


def test_us_trade_runners_do_not_import_kr_modules():
    """US trade runners must not import Korean-market modules."""
    files = [
        Path("trader/us/runner/trade_tick_runner.py"),
        Path("trader/us/runner/trade_session_runner.py"),
    ]
    forbidden = [
        "from trader.kr",
        "import trader.kr",
        "from trader.legacy_kosdaq_runner",
        "import trader.legacy_kosdaq_runner",
    ]

    for path in files:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            assert token not in text, f"{path} contains forbidden import: {token}"


def test_us_trade_tick_blocks_raw_universe_fallback_by_contract():
    """US trade tick must block raw universe fallback and enforce locked watchlist."""
    path = Path("trader/us/runner/trade_tick_runner.py")
    if not path.exists():
        return
    
    text = path.read_text(encoding="utf-8")
    
    # Must mention locked watchlist and fallback disabled
    assert "locked_watchlist" in text.lower(), "trade_tick_runner should reference locked_watchlist"
    assert ("fallback" in text.lower() and "disabled" in text.lower()) or "raw_universe_fallback" in text, \
        "trade_tick_runner should block raw universe fallback"


def test_us_entry_uses_watchlist_dedupe():
    """US trade tick must dedupe watchlist by symbol before entry evaluation."""
    path = Path("trader/us/runner/trade_tick_runner.py")
    if not path.exists():
        return
    
    text = path.read_text(encoding="utf-8")
    
    # Must have dedupe function
    assert "_dedupe_watchlist_best_by_symbol" in text, "trade_tick_runner should have dedupe function"
    assert "WATCHLIST_DEDUPE" in text, "trade_tick_runner should log watchlist dedupe"
