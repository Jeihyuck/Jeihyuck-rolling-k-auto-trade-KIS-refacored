from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.reset_practice_account_state import execute_practice_account_state_reset
from tests.test_reset_practice_account import _make_engine, _seed_tables


class _DummyKis:
    CANO = "50160136"
    ACNT_PRDT_CD = "01"

    def __init__(self, *, holdings_qty: str = "0", cash: str = "100000000"):
        self.holdings_qty = holdings_qty
        self.cash = cash

    def get_balance_cached(self, force=True):
        rows = []
        if int(self.holdings_qty):
            rows = [{"pdno": "005930", "hldg_qty": self.holdings_qty}]
        return {
            "output1": rows,
            "output2": {"dnca_tot_amt": self.cash, "ord_psbl_cash": self.cash},
        }


def test_reset_state_script_archives_and_clears(monkeypatch, tmp_path):
    engine, schema = _make_engine()
    _seed_tables(engine, schema)
    monkeypatch.setenv("TRADER_CACHE_ROOT", str(tmp_path))
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("RESET_PRACTICE_ACCOUNT", "1")
    monkeypatch.setenv("EXPECTED_PRACTICE_CAPITAL_KRW", "100000000")
    monkeypatch.setenv("EXPECTED_INITIAL_HOLDINGS", "0")

    result = execute_practice_account_state_reset(engine=engine, kis=_DummyKis())

    assert result["performed"] is True
    archive_path = Path(result["archive_path"])
    assert archive_path.exists()
    payload = json.loads(archive_path.read_text(encoding="utf-8"))
    assert payload["masked_account"] == "practice:***0136:01"
    assert payload["before_counts"]["positions"] == 1


def test_reset_state_script_rejects_nonempty_kis_holdings(monkeypatch):
    engine, schema = _make_engine()
    _seed_tables(engine, schema)
    monkeypatch.setenv("STRATEGY_ENV", "practice")
    monkeypatch.setenv("KIS_ENV", "practice")
    monkeypatch.setenv("RESET_PRACTICE_ACCOUNT", "1")
    monkeypatch.setenv("EXPECTED_PRACTICE_CAPITAL_KRW", "100000000")
    monkeypatch.setenv("EXPECTED_INITIAL_HOLDINGS", "0")

    with pytest.raises(RuntimeError, match="holdings mismatch"):
        execute_practice_account_state_reset(engine=engine, kis=_DummyKis(holdings_qty="8"))