from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.reset_practice_account_state import execute_practice_account_state_reset
from tests.test_account_reset import _DummyKis
from tests.test_reset_practice_account import _make_engine, _seed_tables
from trader.account_state import resolve_account_sanity_capital_tolerance_krw


def test_reset_tolerance_pct_is_applied(monkeypatch, tmp_path):
    engine, schema = _make_engine()
    _seed_tables(engine, schema)
    monkeypatch.setenv("TRADER_CACHE_ROOT", str(tmp_path))
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("RESET_PRACTICE_ACCOUNT", "1")
    monkeypatch.setenv("EXPECTED_PRACTICE_CAPITAL_KRW", "100000000")
    monkeypatch.setenv("EXPECTED_INITIAL_HOLDINGS", "0")
    monkeypatch.setenv("ACCOUNT_SANITY_TOLERANCE_PCT", "0.05")

    assert resolve_account_sanity_capital_tolerance_krw(100000000) == 5000000

    result = execute_practice_account_state_reset(engine=engine, kis=_DummyKis(cash="95000000"))

    assert result["performed"] is True
    assert result["cash_krw"] == 95000000