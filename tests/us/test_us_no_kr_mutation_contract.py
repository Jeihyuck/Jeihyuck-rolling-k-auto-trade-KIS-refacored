# -*- coding: utf-8 -*-
"""US-only patch contract.

This test documents that this patch must not require Korean-market code changes.
"""

from pathlib import Path


def test_us_patch_does_not_depend_on_kr_modules_in_us_runner():
    """US runner files must not import Korean-market modules."""
    us_files = [
        Path("trader/us/runner/prep_runner.py"),
        Path("trader/us/runner/dispatcher.py"),
    ]
    forbidden_imports = [
        "from trader.kr",
        "import trader.kr",
        "from trader.legacy_kosdaq_runner",
        "import trader.legacy_kosdaq_runner",
        "from trader.pb1_runner",
        "import trader.pb1_runner",
    ]

    for path in us_files:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        for forbidden in forbidden_imports:
            assert forbidden not in text, f"{path} must not contain '{forbidden}'"


def test_us_patch_does_not_import_kr_db_repos():
    """US files must not import trader.db.repos (KR DB module)."""
    us_files = [
        Path("trader/us/runner/prep_runner.py"),
        Path("trader/us/runner/dispatcher.py"),
        Path("trader/us/runner/trade_tick_runner.py"),
    ]
    forbidden_imports = [
        "from trader.db.repos import",
        "import trader.db.repos",
    ]

    for path in us_files:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        for forbidden in forbidden_imports:
            assert forbidden not in text, f"{path} must not contain '{forbidden}'"


def test_us_db_repos_only_uses_us_prefix():
    """trader/us/db/repos.py must only access us_* tables."""
    path = Path("trader/us/db/repos.py")
    if not path.exists():
        return

    text = path.read_text(encoding="utf-8")
    
    # Should only reference us_* tables
    forbidden_kr_tables = [
        "FROM orders",
        "INTO orders",
        "FROM positions",
        "INTO positions",
        "FROM fills",
        "INTO fills",
        "FROM watchlist",
        "INTO watchlist",
        "FROM runs",
        "INTO runs",
        "FROM pb1_watchlist",
        "INTO pb1_watchlist",
    ]
    
    for forbidden in forbidden_kr_tables:
        assert forbidden not in text, f"trader/us/db/repos.py must not use '{forbidden}'"
