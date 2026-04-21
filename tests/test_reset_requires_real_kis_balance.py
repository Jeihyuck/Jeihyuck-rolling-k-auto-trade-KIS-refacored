from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.reset_practice_account_state import execute_practice_account_state_reset
from tests.test_reset_practice_account import _make_engine, _seed_tables
from trader.kis_wrapper import KisAPI


def test_reset_requires_real_kis_balance_when_http_disabled(monkeypatch):
    engine, schema = _make_engine()
    _seed_tables(engine, schema)
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("STRATEGY_MODE", "LIVE")
    monkeypatch.setenv("RESET_PRACTICE_ACCOUNT", "1")
    monkeypatch.setenv("EXPECTED_PRACTICE_CAPITAL_KRW", "100000000")
    monkeypatch.setenv("EXPECTED_INITIAL_HOLDINGS", "0")
    monkeypatch.setenv("KIS_HTTP_ENABLED", "0")
    monkeypatch.setenv("KIS_APP_KEY", "test-key")
    monkeypatch.setenv("KIS_APP_SECRET", "test-secret")
    monkeypatch.setenv("KIS_REST_URL", "https://example.test")
    monkeypatch.setenv("CANO", "50160136")
    monkeypatch.setenv("ACNT_PRDT_CD", "01")
    monkeypatch.setattr(KisAPI, "get_valid_token", lambda self: "dummy_token")

    api = KisAPI(kis_env="practice")

    with pytest.raises(RuntimeError, match="real KIS balance required for account reset"):
        execute_practice_account_state_reset(engine=engine, kis=api)