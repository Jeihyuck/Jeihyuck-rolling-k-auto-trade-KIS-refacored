from pathlib import Path


def test_preflight_uses_final30_scored_not_locked_watchlist():
    script = Path('scripts/wsl/check-us-prep-before-am.sh').read_text()
    assert 'final30_scored.json' in script
    assert 'locked_watchlist.json' not in script


def test_us_force_now_passes_cli_and_exports_force_now_input():
    script = Path('scripts/wsl/run-us-prep.sh').read_text()
    assert 'export FORCE_NOW_INPUT="${US_FORCE_NOW}"' in script
    assert 'cmd+=(--force-now "${US_FORCE_NOW}")' in script
