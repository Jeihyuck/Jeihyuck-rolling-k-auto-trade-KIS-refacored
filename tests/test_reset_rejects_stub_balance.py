from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.reset_practice_account_state import execute_practice_account_state_reset
from tests.test_reset_practice_account import _make_engine, _seed_tables
from trader.db.repos import PracticeAccountResetRepo


class _StubBalanceKis:
    CANO = "50160136"
    ACNT_PRDT_CD = "01"

    def get_balance_cached(self, force=True):
        return {
            "output1": [],
            "output2": {"dnca_tot_amt": "10000000", "ord_psbl_cash": "10000000"},
            "_stub": True,
            "_source": "http_disabled_stub",
        }


def test_reset_rejects_stub_balance_before_db_clear(monkeypatch):
    engine, schema = _make_engine()
    _seed_tables(engine, schema)
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("RESET_PRACTICE_ACCOUNT", "1")
    monkeypatch.setenv("EXPECTED_PRACTICE_CAPITAL_KRW", "100000000")
    monkeypatch.setenv("EXPECTED_INITIAL_HOLDINGS", "0")

    clear_called = False

    def _fail_if_cleared(self, env: str, account_key: str | None = None):
        nonlocal clear_called
        clear_called = True
        raise AssertionError("clear_account_state should not be called for stub balances")

    monkeypatch.setattr(PracticeAccountResetRepo, "clear_account_state", _fail_if_cleared)

    with pytest.raises(RuntimeError, match="HTTP disabled or stub balance returned"):
        execute_practice_account_state_reset(engine=engine, kis=_StubBalanceKis())

    assert clear_called is False