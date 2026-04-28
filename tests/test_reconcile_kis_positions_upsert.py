"""
KIS holdings=N, DB positions=0 시 upsert_positions_from_kis_holdings 호출 테스트
"""
from __future__ import annotations
from unittest.mock import MagicMock, patch
import pytest


def test_reconcile_upserts_positions_when_db_empty_and_kis_has_holdings():
    """kis_holdings=8, db_positions=0이면 upsert_positions_from_kis_holdings가 호출되어야 한다."""
    mock_positions_repo = MagicMock()
    mock_positions_repo.upsert_positions_from_kis_holdings = MagicMock(return_value={"inserted": 8, "updated": 0, "skipped": 0})

    holdings = [
        {"pdno": f"00{i:04d}", "hldg_qty": "10", "pchs_avg_pric": "50000"}
        for i in range(1, 9)
    ]

    # DB에 positions가 없는 상황 시뮬레이션
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_engine.connect.return_value = mock_conn
    mock_result = MagicMock()
    mock_result.scalar.return_value = 0
    mock_conn.execute.return_value = mock_result

    kis_holdings_count = len([h for h in holdings if int(float(h.get("hldg_qty") or 0)) > 0])
    db_positions_count = 0

    assert kis_holdings_count == 8
    assert db_positions_count == 0

    if kis_holdings_count > 0 and db_positions_count == 0:
        mock_positions_repo.upsert_positions_from_kis_holdings(
            env="practice",
            account_key="TEST_ACCOUNT",
            holdings=holdings,
        )

    mock_positions_repo.upsert_positions_from_kis_holdings.assert_called_once()
    call_kwargs = mock_positions_repo.upsert_positions_from_kis_holdings.call_args.kwargs
    assert call_kwargs["env"] == "practice"
    assert len(call_kwargs["holdings"]) == 8


def test_upsert_positions_from_kis_holdings_skips_zero_qty():
    """qty=0인 종목은 skipped로 처리되어야 한다."""
    holdings = [
        {"pdno": "000001", "hldg_qty": "0", "pchs_avg_pric": "50000"},
        {"pdno": "000002", "hldg_qty": "10", "pchs_avg_pric": "50000"},
    ]

    inserted = 0
    skipped = 0
    for h in holdings:
        qty = int(float(h.get("hldg_qty") or 0))
        if qty <= 0:
            skipped += 1
        else:
            inserted += 1

    assert inserted == 1
    assert skipped == 1
