from pathlib import Path


def test_preflight_reads_final30_scored_not_locked_watchlist_json():
    src = Path("scripts/wsl/check-us-prep-before-am.sh").read_text(encoding="utf-8")
    assert "final30_scored.json" in src
    assert "load_latest_us_prep_status" in src
    assert "load_locked_us_watchlist" in src
    assert "locked_watchlist.json" not in src
    assert "contract_and_final30" in src
    assert "db_locked_watchlist" in src
