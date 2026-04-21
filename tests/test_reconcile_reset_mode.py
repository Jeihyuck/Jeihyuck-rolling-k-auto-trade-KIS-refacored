from datetime import datetime
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import trader.reconcile_kis as reconcile_kis


class _DummyKis:
    CANO = "50160136"
    ACNT_PRDT_CD = "01"

    def get_balance_cached(self, force=True):
        return {
            "output1": [{"pdno": "005930", "hldg_qty": "8"}],
            "output2": {"dnca_tot_amt": "100000000", "ord_psbl_cash": "100000000"},
        }


class _DummyPositionsRepo:
    called = False

    def __init__(self, _engine):
        pass

    def restore_missing_from_holdings(self, **_kwargs):
        type(self).called = True
        return 8


class _DummyReconcileLogRepo:
    def __init__(self, _engine):
        self.payload = None

    def append_log(self, **_kwargs):
        self.payload = _kwargs


def test_reconcile_reset_mode_skips_restore(monkeypatch):
    monkeypatch.setenv("RESET_PRACTICE_ACCOUNT", "1")
    monkeypatch.setenv("RESET_ALLOW_KIS_HOLDINGS", "0")
    monkeypatch.setattr(reconcile_kis, "PositionsRepo", _DummyPositionsRepo)
    monkeypatch.setattr(reconcile_kis, "ReconcileLogRepo", _DummyReconcileLogRepo)
    monkeypatch.setattr(reconcile_kis, "reconcile_today", lambda **_kwargs: {"ok": True, "orders": 0, "fills": 0})
    monkeypatch.setattr(
        reconcile_kis,
        "evaluate_stale_db_guard",
        lambda **_kwargs: (False, "reset_mode", {"empty_streak": 0}),
    )

    result = reconcile_kis.reconcile_kis(
        engine=object(),
        kis=_DummyKis(),
        env="practice",
        run_id="run-1",
        strategy="pb1_pullback_close",
        tick_ts=datetime(2026, 4, 21, 9, 0, 0),
    )

    assert _DummyPositionsRepo.called is False
    assert result["restored_positions"] == 0
    assert result["reset_mode"] is True
    assert result["masked_account"] == "practice:***0136:01"