import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import trader.pb1_runner as pb1_runner


class _DummyKis:
    CANO = "50160136"
    ACNT_PRDT_CD = "01"


def test_practice_account_sanity_passes_for_expected_empty_account(monkeypatch):
    monkeypatch.setenv("ACCOUNT_SANITY_CHECK", "1")
    monkeypatch.setenv("EXPECTED_PRACTICE_CAPITAL_KRW", "100000000")
    monkeypatch.setenv("EXPECTED_INITIAL_HOLDINGS", "0")

    result = pb1_runner._practice_account_sanity_check(
        env="practice",
        kis=_DummyKis(),
        balance_state=pb1_runner.BALANCE_STATE_OK,
        balance_snapshot={
            "output1": [],
            "output2": {"dnca_tot_amt": "100000000", "ord_psbl_cash": "100000000"},
        },
    )

    assert result["enabled"] is True
    assert result["ok"] is True
    assert result["masked_account"] == "practice:***0136:01"


def test_practice_account_sanity_fails_when_old_holdings_exist(monkeypatch):
    monkeypatch.setenv("ACCOUNT_SANITY_CHECK", "1")
    monkeypatch.setenv("EXPECTED_PRACTICE_CAPITAL_KRW", "100000000")
    monkeypatch.setenv("EXPECTED_INITIAL_HOLDINGS", "0")

    result = pb1_runner._practice_account_sanity_check(
        env="practice",
        kis=_DummyKis(),
        balance_state=pb1_runner.BALANCE_STATE_OK,
        balance_snapshot={
            "output1": [{"pdno": "005930", "hldg_qty": "8"}],
            "output2": {"dnca_tot_amt": "97000000", "ord_psbl_cash": "97000000"},
        },
    )

    assert result["ok"] is False
    assert "holdings_mismatch" in result["reason"]
    assert "capital_mismatch" in result["reason"]