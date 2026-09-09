"""
한국장 KIS authoritative empty guard 테스트
"""
import os
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

from trader.pb1_engine import PB1Engine, _is_kr_stock_code, _is_kis_balance_authoritative_empty


def test_is_kr_stock_code():
    """한국장 6자리 숫자 종목 코드 판별"""
    assert _is_kr_stock_code("028050") is True
    assert _is_kr_stock_code("066970") is True
    assert _is_kr_stock_code("000001") is True
    
    # 미국장 ticker는 False
    assert _is_kr_stock_code("AAPL") is False
    assert _is_kr_stock_code("TSLA") is False
    assert _is_kr_stock_code("NVDA") is False
    
    # 잘못된 형식
    assert _is_kr_stock_code("12345") is False
    assert _is_kr_stock_code("1234567") is False
    assert _is_kr_stock_code("") is False
    assert _is_kr_stock_code(None) is False


def test_is_kis_balance_authoritative_empty_output1_empty():
    """KIS balance output1=[]이면 authoritative empty"""
    balance_snapshot = {
        "rt_cd": "0",
        "output1": [],
        "output2": {
            "scts_evlu_amt": "0",
            "pchs_amt_smtl_amt": "0",
            "evlu_amt_smtl_amt": "0",
        },
    }
    assert _is_kis_balance_authoritative_empty(balance_snapshot) is True


def test_is_kis_balance_authoritative_empty_output1_has_rows():
    """KIS balance output1에 row가 있으면 not empty"""
    balance_snapshot = {
        "rt_cd": "0",
        "output1": [{"pdno": "028050"}],
        "output2": {
            "scts_evlu_amt": "0",
            "pchs_amt_smtl_amt": "0",
            "evlu_amt_smtl_amt": "0",
        },
    }
    assert _is_kis_balance_authoritative_empty(balance_snapshot) is False


def test_is_kis_balance_authoritative_empty_error_response():
    """KIS balance rt_cd != 0이면 not authoritative"""
    balance_snapshot = {
        "rt_cd": "1",
        "output1": [],
        "output2": {},
    }
    assert _is_kis_balance_authoritative_empty(balance_snapshot) is False


def test_is_kis_balance_authoritative_empty_none():
    """balance_snapshot이 None이면 not authoritative"""
    assert _is_kis_balance_authoritative_empty(None) is False


@pytest.fixture
def mock_engine():
    """PB1Engine 테스트용 mock"""
    from trader.db.repos import UniverseRepo, OrdersRepo, FillsRepo, PositionsRepo, LedgerEventsRepo
    
    with patch("trader.pb1_engine.now_kst") as mock_now:
        mock_now.return_value = datetime(2026, 5, 13, 10, 0, 0)
        
        engine = PB1Engine(
            universe_repo=MagicMock(spec=UniverseRepo),
            orders_repo=MagicMock(spec=OrdersRepo),
            fills_repo=MagicMock(spec=FillsRepo),
            positions_repo=MagicMock(spec=PositionsRepo),
            ledger_repo=MagicMock(spec=LedgerEventsRepo),
            kis=None,
            dry_run=True,
            env="practice",
            run_id="test-run-id",
            phase="exit",
            window_label="am",
        )
        
        return engine


def test_kr_kis_empty_balance_blocks_ledger_reconstruct(mock_engine):
    """KIS 보유 0이면 ledger_reconstruct 금지"""
    # KIS balance empty
    balance_snapshot = {
        "rt_cd": "0",
        "output1": [],
        "output2": {
            "scts_evlu_amt": "0",
            "pchs_amt_smtl_amt": "0",
            "evlu_amt_smtl_amt": "0",
        },
    }
    
    mock_engine._balance_snapshot = balance_snapshot
    
    # ledger positions에는 과거 BUY가 있음
    ledger_positions = [
        {"code": "028050", "qty": 58, "avg_buy_price": 52200},
    ]
    
    # env 설정
    os.environ["KR_AUTHORITATIVE_KIS_BALANCE_FOR_EXIT"] = "1"
    os.environ["KR_BLOCK_LEDGER_RECONSTRUCT_WHEN_KIS_EMPTY"] = "1"
    
    try:
        holdings, meta = mock_engine.load_effective_holdings_for_exit(
            balance_rows=[],
            ledger_positions=ledger_positions,
            balance_snapshot=balance_snapshot,
        )
        
        assert holdings == []
        assert meta["source"] == "kis_empty_authoritative"
        assert meta["fallback_blocked"] is True
    finally:
        os.environ.pop("KR_AUTHORITATIVE_KIS_BALANCE_FOR_EXIT", None)
        os.environ.pop("KR_BLOCK_LEDGER_RECONSTRUCT_WHEN_KIS_EMPTY", None)


def test_kr_exit_allows_ledger_when_env_set(mock_engine):
    """KR_ALLOW_LEDGER_RECONSTRUCT_WITHOUT_KIS=1이면 허용 (단, KIS empty guard는 여전히 우선)"""
    balance_snapshot = {
        "rt_cd": "0",
        "output1": [],
        "output2": {
            "scts_evlu_amt": "0",
            "pchs_amt_smtl_amt": "0",
            "evlu_amt_smtl_amt": "0",
        },
    }
    
    mock_engine._balance_snapshot = balance_snapshot
    
    ledger_positions = [
        {"code": "028050", "qty": 58, "avg_buy_price": 52200},
    ]
    
    # env 설정: KIS empty guard는 여전히 우선
    os.environ["KR_AUTHORITATIVE_KIS_BALANCE_FOR_EXIT"] = "1"
    os.environ["KR_BLOCK_LEDGER_RECONSTRUCT_WHEN_KIS_EMPTY"] = "1"
    os.environ["KR_ALLOW_LEDGER_RECONSTRUCT_WITHOUT_KIS"] = "1"
    
    try:
        # KIS empty guard가 우선하므로 kis_empty_authoritative가 반환됨
        holdings, meta = mock_engine.load_effective_holdings_for_exit(
            balance_rows=[],
            ledger_positions=ledger_positions,
            balance_snapshot=balance_snapshot,
        )
        
        # KIS empty guard가 먼저 작동함
        assert meta["source"] == "kis_empty_authoritative"
        assert meta["fallback_blocked"] is True
    finally:
        os.environ.pop("KR_AUTHORITATIVE_KIS_BALANCE_FOR_EXIT", None)
        os.environ.pop("KR_BLOCK_LEDGER_RECONSTRUCT_WHEN_KIS_EMPTY", None)
        os.environ.pop("KR_ALLOW_LEDGER_RECONSTRUCT_WITHOUT_KIS", None)


def test_us_ticker_not_affected_by_kr_stale_position_guard():
    """미국장 ticker는 한국장 guard 영향 없음"""
    assert _is_kr_stock_code("AAPL") is False
    assert _is_kr_stock_code("TSLA") is False
    assert _is_kr_stock_code("NVDA") is False
    assert _is_kr_stock_code("028050") is True


def test_buyable_gate_does_not_disguise_db_position_as_kis_holding(mock_engine):
    """2026-09-09: authoritative KIS absence must remain qty=0."""
    mock_engine._balance_snapshot = {
        "rt_cd": "0",
        "output1": [{"pdno": "005930", "hldg_qty": "22", "ord_psbl_qty": "22"}],
        "output2": {"scts_evlu_amt": "1000000"},
    }
    mock_engine.fills_repo.list_fills_in_window.return_value = []
    mock_engine.orders_repo.list_orders_in_window.return_value = []
    mock_engine.ledger_repo.list_events_in_window.return_value = []

    ctx = mock_engine._build_buyable_gate_context(
        codes=["066570"],
        positions=[{"code": "066570", "qty": 1}],
    )

    assert ctx["066570"]["db_position_qty"] == 1
    assert ctx["066570"]["kis_holding_qty"] == 0
