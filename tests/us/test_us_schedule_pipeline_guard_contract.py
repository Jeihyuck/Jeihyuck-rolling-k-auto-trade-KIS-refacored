# -*- coding: utf-8 -*-
"""US schedule prep/AM pipeline guard contract tests."""

from pathlib import Path


def test_us_prep_schedule_starts_earlier_and_stale_window_wider():
    text = Path(".github/workflows/us-trade-prep.yml").read_text(encoding="utf-8")

    assert 'cron: "20 12 * * 1-5"' in text
    assert 'cron: "20 13 * * 1-5"' in text
    assert 'cron: "05 14 * * 1-5"' in text
    assert "target_min=$((8 * 60 + 15))" in text
    assert "stale_min=$((9 * 60 + 45))" in text
    assert "phase_window=0815-0945" in text


def test_us_prep_stale_skip_is_not_silent_success():
    text = Path(".github/workflows/us-trade-prep.yml").read_text(encoding="utf-8")

    assert "skip_stale_start_us_prep" in text
    assert "US prep skipped due to stale start" in text


def test_us_am_has_same_day_prep_guard():
    text = Path(".github/workflows/us-trade-am.yml").read_text(encoding="utf-8")
    script_text = Path("scripts/guard_us_prep_contract.py").read_text(encoding="utf-8")

    assert "Guard same-day US prep contract" in text
    assert "[US_PREP_GUARD][CHECK]" in script_text
    assert "[US_PREP_GUARD][FAIL]" in script_text
    assert "load_latest_us_prep_status" in script_text
    assert "load_locked_us_watchlist" in script_text
    assert "python scripts/guard_us_prep_contract.py" in text


def test_us_afternoon_has_same_day_prep_guard():
    text = Path(".github/workflows/us-trade-afternoon.yml").read_text(encoding="utf-8")
    script_text = Path("scripts/guard_us_prep_contract.py").read_text(encoding="utf-8")

    assert "Guard same-day US prep contract" in text
    assert "[US_PREP_GUARD][CHECK]" in script_text
    assert "[US_PREP_GUARD][FAIL]" in script_text
    assert "load_latest_us_prep_status" in script_text
    assert "load_locked_us_watchlist" in script_text
    assert "python scripts/guard_us_prep_contract.py" in text
