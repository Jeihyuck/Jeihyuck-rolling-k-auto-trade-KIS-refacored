"""
Test env_bool parsing to ensure DRY_RUN=0 is parsed as False.
"""
import os
import pytest
from trader.utils.env import env_bool, parse_env_flag


def test_env_bool_zero_is_false(monkeypatch):
    """DRY_RUN=0 must parse to False"""
    monkeypatch.setenv("DRY_RUN", "0")
    assert env_bool("DRY_RUN", default=True) is False


def test_env_bool_one_is_true(monkeypatch):
    """DRY_RUN=1 must parse to True"""
    monkeypatch.setenv("DRY_RUN", "1")
    assert env_bool("DRY_RUN", default=False) is True


def test_env_bool_false_values(monkeypatch):
    """All FALSE_VALUES must parse to False"""
    for val in ["0", "false", "f", "no", "n", "off", "FALSE", "False", "OFF"]:
        monkeypatch.setenv("TEST_FLAG", val)
        result = env_bool("TEST_FLAG", default=True)
        assert result is False, f"Failed for value: {val}"


def test_env_bool_true_values(monkeypatch):
    """All TRUE_VALUES must parse to True"""
    for val in ["1", "true", "t", "yes", "y", "on", "TRUE", "True", "ON"]:
        monkeypatch.setenv("TEST_FLAG", val)
        result = env_bool("TEST_FLAG", default=False)
        assert result is True, f"Failed for value: {val}"


def test_env_bool_missing_uses_default(monkeypatch):
    """Missing env should return default"""
    monkeypatch.delenv("MISSING_FLAG", raising=False)
    assert env_bool("MISSING_FLAG", default=True) is True
    assert env_bool("MISSING_FLAG", default=False) is False


def test_env_bool_empty_uses_default(monkeypatch):
    """Empty env should return default"""
    monkeypatch.setenv("EMPTY_FLAG", "")
    assert env_bool("EMPTY_FLAG", default=True) is True
    monkeypatch.setenv("EMPTY_FLAG", "  ")
    assert env_bool("EMPTY_FLAG", default=False) is False


def test_parse_env_flag_validation(monkeypatch):
    """parse_env_flag should mark invalid values"""
    monkeypatch.setenv("INVALID_FLAG", "maybe")
    flag = parse_env_flag("INVALID_FLAG", default=False)
    assert flag.valid is False
    assert flag.value is False  # Should use default


def test_dry_run_live_scenario(monkeypatch):
    """
    Simulate LIVE scenario: DRY_RUN=0 must parse to False
    This is the critical test case from the bug report.
    """
    monkeypatch.setenv("DRY_RUN", "0")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "1")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "0")
    
    dry_run = env_bool("DRY_RUN", default=True)
    
    # CRITICAL: This must be False for live trading to work
    assert dry_run is False, (
        f"BUG DETECTED: DRY_RUN=0 parsed as {dry_run} (should be False). "
        f"env={os.getenv('DRY_RUN')} type={type(dry_run).__name__}"
    )
